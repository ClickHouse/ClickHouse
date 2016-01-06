#
# Makefile
#
# $Id: //poco/1.4/XML/samples/DOMParser/Makefile#1 $
#
# Makefile for Poco DOMParser
#

include $(POCO_BASE)/build/rules/global

objects = DOMParser

target         = DOMParser
target_version = 1
target_libs    = PocoXML PocoFoundation

include $(POCO_BASE)/build/rules/exec

ifdef POCO_UNBUNDLED
        SYSLIBS += -lexpat
endif

