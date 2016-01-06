#
# Makefile
#
# $Id: //poco/1.4/XML/samples/SAXParser/Makefile#1 $
#
# Makefile for Poco SAXParser
#

include $(POCO_BASE)/build/rules/global

objects = SAXParser

target         = SAXParser
target_version = 1
target_libs    = PocoXML PocoFoundation

include $(POCO_BASE)/build/rules/exec

ifdef POCO_UNBUNDLED
        SYSLIBS += -lexpat
endif
