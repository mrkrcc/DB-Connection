from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import mysql.connector as connection
from bson.json_util import dumps
from pymongo import MongoClient
import pypyodbc as odbc
import pandas as pd
import oracledb
import logging
import json

logging.basicConfig(filename="connection.log", filemode='a', format='%(name)s - %(levelname)s - %(message)s', level = logging.WARNING)
def saveLog(logType, message):
	if logType == 30:
		logging.warning(json.dumps(message))
	elif logType == 40:
		logging.error(json.dumps(message))
	elif logType == 50:
		logging.critical(json.dumps(message))	
	else:
		print(bcolors.OKGREEN, "LOG", logType, message, bcolors.ENDC)

class LOGLEVEL:
	CRITICAL = 50
	ERROR = 40
	WARNING = 30
	INFO = 20
	DEBUG = 10

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
class Databases():

  @classmethod
  def _connectMongo( cls, host, port, username, password, db):
    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
        myClient = MongoClient(mongo_uri)
    else:
        myClient = MongoClient(host, port)
    return myClient[db]


  @classmethod
  def readMongo( cls, db, collection, criteria={}, host='localhost', port=27017, username=None, password=None, no_id=True ):
    """ Read from Mongo and Store into DataFrame """

    # Connect to MongoDB
    mydb = Databases._connectMongo(host=host, port=port, username=username, password=password, db=db)

    # Make a query to the specific DB and Collection
    result = json.loads(dumps(mydb[collection].find(criteria)))

    # Expand the cursor and construct the DataFrame
    df =  pd.DataFrame(result)

    # Delete the _id
    if no_id:
        del df['_id']
    return df

  @classmethod
  def _connectMySQL( cls, db, password, user, host):
    mydb = connection.connect(host = host, database = db, user = user, passwd = password, use_pure=True)
    return mydb

  @classmethod 
  def readMySQL( cls, query, db, password, user = "root", host = "localhost"):
    
    #: Connect to MySQL
    mydb = Databases._connectMySQL( db, password, user, host )

    #: Make a query, get DataFrame
    df = pd.read_sql(query , mydb)
    mydb.close()
    return df

  @classmethod
  def _connectMSSQL( cls, driver, server, db, user, password ):
    connectionString = f"DRIVER={{{driver}}};SERVER={server};DATABASE={db};uid={user};pwd={password}"
    conn = odbc.connect(connectionString)
    return conn

  @classmethod
  def readMSSQL( cls, driver, server, db, user, password, query ):
    
    #: Connect MsSQL
    conn = Databases._connectMSSQL( driver, server, db, user, password )
    return pd.read_sql(query, conn)

  @classmethod
  def _connectCassandra( cls, keyspace, db, username, password, host = "0.0.0.0", port = 9042 ):
    ap = PlainTextAuthProvider( username = username, password = password )
    cls = Cluster([host], port = port, auth_provider = ap )
    session = cls.connect(keyspace, wait_for_all_pools = True)
    session.execute(f"USE {db}")
    return session

  @classmethod
  def readCassandra( cls, keyspace, db, query, username = "cassandra", password = "cassandra", host = "0.0.0.0", port = 9042):
    #: Connect Cassandra DB, get selected db
    session = Databases._connectCassandra( keyspace, db, username, password, host, port)
    #: Execute query
    data = session.execute( query )
    return pd.DataFrame( data )

  @classmethod
  def _connectOracle( cls, host, port, serviceName, username, password):
    connectionString = f"(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port={port})(host={host}))(connect_data=(service_name={serviceName}))(security=(ssl_server_dn_match=yes)))"
    connection = oracledb.connect( user = username, password = password, dsn = connectionString)
    return connection.cursor()

  @classmethod
  def readOracle( cls, host, port, serviceName, username, password, query):
    #: Connect Oracle DB, get cursor to execute query
    cursor = Databases._connectOracle( host, port, serviceName, username, password)
    return pd.DataFrame( cursor.execute( query ) )
