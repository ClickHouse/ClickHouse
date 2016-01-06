#
# Makefile
#
# $Id: //poco/1.4/Foundation/samples/grep/Makefile#1 $
#
# Makefile for Poco grep
#

include $(POCO_BASE)/build/rules/global

objects = grep

target         = grep
target_version = 1
target_libs    = PocoFoundation

include $(POCO_BASE)/build/rules/exec

ifdef POCO_UNBUNDLED
        SYSLIBS += -lpcre
endif

