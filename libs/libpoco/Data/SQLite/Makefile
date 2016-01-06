#
# Makefile
#
# $Id: //poco/1.4/Data/SQLite/Makefile#2 $
#
# Makefile for Poco SQLite
#

include $(POCO_BASE)/build/rules/global

COMMONFLAGS += -I$(POCO_BASE)/Data/SQLite/src

SYSFLAGS += -DSQLITE_THREADSAFE=1 -DSQLITE_DISABLE_LFS \
        -DSQLITE_OMIT_UTF16 -DSQLITE_OMIT_PROGRESS_CALLBACK -DSQLITE_OMIT_COMPLETE \
        -DSQLITE_OMIT_TCL_VARIABLE -DSQLITE_OMIT_DEPRECATED

objects = Binder Extractor Notifier SessionImpl Connector \
        SQLiteException SQLiteStatementImpl Utility

sqlite_objects = sqlite3

ifdef POCO_UNBUNDLED
        SYSLIBS += -lsqlite3
else
        objects += $(sqlite_objects)
endif

target         = PocoDataSQLite
target_version = $(LIBVERSION)
target_libs    = PocoData PocoFoundation

include $(POCO_BASE)/build/rules/lib
