#
# Makefile
#
# $Id: //poco/Main/Net/samples/HTTPTimeServer/Makefile#1 $
#
# Makefile for Poco HTTPSTimeServer
#

include $(POCO_BASE)/build/rules/global

# Note: linking order is important, do not change it.
ifeq ($(POCO_CONFIG),FreeBSD)
SYSLIBS += -lssl -lcrypto -lz
else
SYSLIBS += -lssl -lcrypto -lz -ldl
endif

objects = HTTPSTimeServer

target         = HTTPSTimeServer
target_version = 1
target_libs    = PocoNetSSL PocoCrypto PocoNet PocoUtil PocoJSON PocoXML PocoFoundation

include $(POCO_BASE)/build/rules/exec

ifdef POCO_UNBUNDLED
        SYSLIBS += -lpcre -lexpat
endif
