#
# ODBC.make
#
# Makefile fragment for finding ODBC library
#

ifndef POCO_ODBC_INCLUDE
POCO_ODBC_INCLUDE = /usr/include
endif

ifndef POCO_ODBC_LIB
ifeq (0, $(shell test -d /usr/lib/$(OSARCH)-linux-gnu; echo $$?))
POCO_ODBC_LIB = /usr/lib/$(OSARCH)-linux-gnu
else ifeq (0, $(shell test -d /usr/lib64; echo $$?))
POCO_ODBC_LIB = /usr/lib64
else
POCO_ODBC_LIB = /usr/lib
endif
endif

ifeq ($(LINKMODE),STATIC)
LIBLINKEXT = .a
else
ifeq ($(OSNAME), CYGWIN)
LIBLINKEXT = $(IMPLIBLINKEXT)
else
LIBLINKEXT = $(SHAREDLIBLINKEXT)
endif
endif

INCLUDE += -I$(POCO_ODBC_INCLUDE)
SYSLIBS += -L$(POCO_ODBC_LIB)

##
## MinGW
##
ifeq ($(POCO_CONFIG),MinGW)
# -DODBCVER=0x0300: SQLHandle declaration issue
# -DNOMINMAX      : MIN/MAX macros defined in windows conflict with libstdc++
CXXFLAGS += -DODBCVER=0x0300 -DNOMINMAX

##
## unixODBC
##
else ifeq (0, $(shell test -e $(POCO_ODBC_LIB)/libodbc$(LIBLINKEXT); echo $$?))
SYSLIBS += -lodbc
ifeq (0, $(shell test -e $(POCO_ODBC_LIB)/libodbcinst$(LIBLINKEXT); echo $$?))
SYSLIBS += -lodbcinst
endif
COMMONFLAGS += -DPOCO_UNIXODBC

##
## iODBC
##
else ifeq (0, $(shell test -e $(POCO_ODBC_LIB)/libiodbc$(LIBLINKEXT); echo $$?))
SYSLIBS += -liodbc -liodbcinst
COMMONFLAGS += -DPOCO_IODBC -I/usr/include/iodbc

# TODO: OSX >= 10.8 deprecated non-Unicode ODBC API functions, silence warnings until iODBC Unicode support
COMMONFLAGS += -Wno-deprecated-declarations

else
$(error No ODBC library found. Please install unixODBC or iODBC or specify POCO_ODBC_LIB and try again)
endif

