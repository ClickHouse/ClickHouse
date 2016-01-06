#
# Makefile
#
# $Id: //poco/1.4/Net/samples/HTTPFormServer/Makefile#1 $
#
# Makefile for Poco HTTPFormServer
#

include $(POCO_BASE)/build/rules/global

objects = HTTPFormServer

target         = HTTPFormServer
target_version = 1
target_libs    = PocoUtil PocoJSON PocoNet PocoXML PocoFoundation

include $(POCO_BASE)/build/rules/exec

ifdef POCO_UNBUNDLED
        SYSLIBS += -lz -lpcre -lexpat
endif
