#
# Makefile
#
# $Id: //poco/Main/Net/samples/SMTPLogger/Makefile#2 $
#
# Makefile for Poco SMTPLogger
#

include $(POCO_BASE)/build/rules/global

objects = SMTPLogger

target         = SMTPLogger
target_version = 1
target_libs    = PocoNet PocoFoundation

include $(POCO_BASE)/build/rules/exec

ifdef POCO_UNBUNDLED
        SYSLIBS += -lz
endif
