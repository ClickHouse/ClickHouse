#
# Makefile
#
# $Id: //poco/1.4/Net/samples/Ping/Makefile#1 $
#
# Makefile for Poco ping
#

include $(POCO_BASE)/build/rules/global

objects = Ping

target         = Ping
target_version = 1
target_libs    = PocoUtil PocoJSON PocoNet PocoXML PocoFoundation

include $(POCO_BASE)/build/rules/exec

ifdef POCO_UNBUNDLED
        SYSLIBS += -lz -lpcre -lexpat
endif
