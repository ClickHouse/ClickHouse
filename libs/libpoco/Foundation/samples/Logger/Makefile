#
# Makefile
#
# $Id: //poco/1.4/Foundation/samples/Logger/Makefile#1 $
#
# Makefile for Poco Logger
#

include $(POCO_BASE)/build/rules/global

objects = Logger

target         = Logger
target_version = 1
target_libs    = PocoFoundation

include $(POCO_BASE)/build/rules/exec

ifdef POCO_UNBUNDLED
        SYSLIBS += -lz
endif

