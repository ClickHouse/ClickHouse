#
# Makefile
#
# $Id: //poco/1.4/Util/samples/pkill/Makefile#1 $
#
# Makefile for Poco pkill utility
#

include $(POCO_BASE)/build/rules/global

objects = pkill

target         = pkill
target_version = 1
target_libs    = PocoUtil PocoJSON PocoXML PocoFoundation

include $(POCO_BASE)/build/rules/exec

ifdef POCO_UNBUNDLED
        SYSLIBS += -lz -lpcre -lexpat
endif
