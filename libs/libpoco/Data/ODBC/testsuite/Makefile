#
# Makefile
#
# $Id: //poco/1.4/Data/ODBC/testsuite/Makefile#4 $
#
# Makefile for Poco SQLite testsuite
#
# For Unicode support, add following to COMMONFLAGS:
#
# -DUNICODE
#
# Unicode is supported only for UnixODBC
#

include $(POCO_BASE)/build/rules/global

include $(POCO_BASE)/Data/ODBC/ODBC.make

##################################################################################################
# Note:                                                                                          #
#  Do not change linking order or move this line up, these libs have to be linked in this order. #
##################################################################################################
ifneq ($(OSNAME),Darwin)
SYSLIBS += -lltdl
endif
ifneq ($(OSNAME),FreeBSD)
SYSLIBS += -ldl
endif

objects = ODBCTestSuite Driver \
	ODBCDB2Test ODBCMySQLTest ODBCOracleTest ODBCPostgreSQLTest \
	ODBCSQLiteTest ODBCSQLServerTest ODBCTest SQLExecutor

ifeq ($(POCO_CONFIG),MinGW)
	objects += ODBCAccessTest
endif

target         = testrunner
target_version = 1
target_libs    = PocoDataODBC PocoData PocoFoundation CppUnit

include $(POCO_BASE)/build/rules/exec
