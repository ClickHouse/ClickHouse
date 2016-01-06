#
# Makefile
#
# $Id: //poco/1.4/Zip/samples/unzip/Makefile#1 $
#
# Makefile for Poco SampleServer
#

include $(POCO_BASE)/build/rules/global

objects = unzip

target         = unzip
target_version = 1
target_libs    = PocoZip PocoUtil PocoJSON PocoXML PocoFoundation

include $(POCO_BASE)/build/rules/exec

ifdef POCO_UNBUNDLED
        SYSLIBS += -lz -lpcre -lexpat
endif
