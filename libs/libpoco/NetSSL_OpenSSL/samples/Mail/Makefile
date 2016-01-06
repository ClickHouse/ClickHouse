#
# Makefile
#
# $Id: //poco/1.4/Net/samples/Mail/Makefile#1 $
#
# Makefile for Poco Mail
#

include $(POCO_BASE)/build/rules/global

# Note: linking order is important, do not change it.
ifeq ($(POCO_CONFIG),FreeBSD)
SYSLIBS += -lssl -lcrypto -lz
else
SYSLIBS += -lssl -lcrypto -lz -ldl
endif

objects = Mail

target         = Mail
target_version = 1
target_libs    = PocoNetSSL PocoNet PocoCrypto PocoUtil PocoJSON PocoXML PocoFoundation

include $(POCO_BASE)/build/rules/exec
