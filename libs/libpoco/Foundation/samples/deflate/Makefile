#
# Makefile
#
# $Id: //poco/1.4/Foundation/samples/deflate/Makefile#1 $
#
# Makefile for Poco deflate
#

include $(POCO_BASE)/build/rules/global

objects = deflate

target         = deflate
target_version = 1
target_libs    = PocoFoundation

include $(POCO_BASE)/build/rules/exec

ifdef POCO_UNBUNDLED
        SYSLIBS += -lz
endif

