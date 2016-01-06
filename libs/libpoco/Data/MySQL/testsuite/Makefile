#
# Makefile
#
# $Id: //poco/1.3/Data/MySQL/testsuite/Makefile#1 $
#
# Makefile for Poco MySQL testsuite
#

include $(POCO_BASE)/build/rules/global

INCLUDE += -I./../include -I/usr/local/include/mysql -I/usr/include/mysql/ -I/usr/mysql/include/mysql -I/usr/local/mysql/include

# Note: linking order is important, do not change it.
SYSLIBS += -L/usr/local/lib$(LIB64SUFFIX)/mysql -L/usr/lib$(LIB64SUFFIX)/mysql -L/usr/mysql/lib$(LIB64SUFFIX) -L/usr/mysql/lib$(LIB64SUFFIX)/mysql -L/usr/local/mysql/lib$(LIB64SUFFIX) -lmysqlclient -lz -lpthread -ldl

objects = MySQLTestSuite Driver MySQLTest SQLExecutor

target         = testrunner
target_version = 1
target_libs    = PocoDataMySQL PocoData PocoFoundation CppUnit

include $(POCO_BASE)/build/rules/exec
