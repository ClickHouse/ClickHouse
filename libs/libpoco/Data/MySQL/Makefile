#
# Makefile
#
# $Id: //poco/1.4/Data/MySQL/Makefile#1 $
#
# Makefile for Poco MySQL
#

include $(POCO_BASE)/build/rules/global

SYSLIBS += -L/usr/local/lib$(LIB64SUFFIX)/mysql -L/usr/lib$(LIB64SUFFIX)/mysql -L/usr/mysql/lib$(LIB64SUFFIX) -L/usr/mysql/lib$(LIB64SUFFIX)/mysql -L/usr/local/mysql/lib$(LIB64SUFFIX) -lmysqlclient
INCLUDE += -I/usr/local/include/mysql/ -I/usr/include/mysql/ -I/usr/mysql/include/mysql -I/usr/local/mysql/include
SYSFLAGS += -DTHREADSAFE -DNO_TCL

objects = Binder Extractor SessionImpl Connector \
	MySQLStatementImpl ResultMetadata MySQLException \
	SessionHandle StatementExecutor Utility

target         = PocoDataMySQL
target_version = $(LIBVERSION)
target_libs    = PocoData PocoFoundation

include $(POCO_BASE)/build/rules/lib
