#
# Makefile
#
# $Id: //poco/1.4/Foundation/samples/inflate/Makefile#1 $
#
# Makefile for Poco inflate
#

include $(POCO_BASE)/build/rules/global

objects = inflate

target         = inflate
target_version = 1
target_libs    = PocoFoundation

include $(POCO_BASE)/build/rules/exec

ifdef POCO_UNBUNDLED
        SYSLIBS += -lz
endif

