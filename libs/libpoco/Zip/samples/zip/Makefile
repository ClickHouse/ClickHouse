#
# Makefile
#
# $Id: //poco/1.4/Zip/samples/zip/Makefile#1 $
#
# Makefile for Poco SampleApp
#

include $(POCO_BASE)/build/rules/global

objects = zip

target         = zip
target_version = 1
target_libs    = PocoZip PocoUtil PocoJSON PocoXML PocoFoundation

include $(POCO_BASE)/build/rules/exec

ifdef POCO_UNBUNDLED
        SYSLIBS += -lz -lpcre -lexpat
endif
