import os
import sys
import configparser
import time
import pathlib
#import mysql.connector
import mariadb

dirList = []
config = configparser.ConfigParser()
config.read('indexer.ini')
config = config['DATABASE']
db_table = sys.argv[1]

cnx = mariadb.connect(user=config['user'], password=config['password'],
                              host=config['server'], port=int(config['port']),
                              database=config['database'])
cursor = cnx.cursor()
try:
 cursor.execute("Drop table " + db_table)
except:
 print("Cannot drop the table " + db_table) 

cursor.execute("create table " + db_table + " (id int primary key auto_increment, parent_id int, name varchar(255), entrytype tinyint, mtime DATETIME, atime DATETIME, size BIGINT);")
 

for dirpath, dnames, fnames in os.walk("./"):
    for f in fnames:
        if (dirpath not in dirList):
         dirList.append(dirpath)
        absFilePath = os.path.abspath(os.path.join(dirpath, f))
        fileInfo = pathlib.Path(absFilePath).stat()
        #filelist.append({"filePath" : absFilePath, "size" : fileInfo.st_size, "mtime" : fileInfo.st_mtime, "atime" : fileInfo.st_atime })
        cursor.execute("insert into " + db_table + " (parent_id,name,entrytype,mtime,atime,size) VALUES (0,'" + f + "',1,FROM_UNIXTIME(" + str(fileInfo.st_mtime) + "),FROM_UNIXTIME(" + str(fileInfo.st_atime) + "), "  + str(fileInfo.st_size) + ")" )
        #print (absFilePath)
        print (dirpath, f)
        #print(os.path.join(dirpath, f))

print (dirList)
cursor.close()
cnx.commit()
cnx.close()
