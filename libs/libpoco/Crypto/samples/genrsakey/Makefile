#
# Makefile
#
# $Id: //poco/Main/template/sample.make#4 $
#
# Makefile for Poco genrsakey
#

include $(POCO_BASE)/build/rules/global

# Note: linking order is important, do not change it.
ifeq ($(POCO_CONFIG),FreeBSD)
SYSLIBS += -lssl -lcrypto -lz
else
SYSLIBS += -lssl -lcrypto -lz -ldl
endif
objects = genrsakey

target         = genrsakey
target_version = 1
target_libs    = PocoCrypto PocoUtil PocoJSON PocoXML PocoFoundation

include $(POCO_BASE)/build/rules/exec

ifdef POCO_UNBUNDLED
        SYSLIBS += -lpcre -lexpat
endif
