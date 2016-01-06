#
# Makefile
#
# $Id: //poco/1.4/Util/samples/SampleApp/Makefile#1 $
#
# Makefile for Poco SampleApp
#

include $(POCO_BASE)/build/rules/global

objects = SampleApp

target         = SampleApp
target_version = 1
target_libs    = PocoUtil PocoJSON PocoXML PocoFoundation

include $(POCO_BASE)/build/rules/exec

ifdef POCO_UNBUNDLED
        SYSLIBS += -lz -lpcre -lexpat
endif
