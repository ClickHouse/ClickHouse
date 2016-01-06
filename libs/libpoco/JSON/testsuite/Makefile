#
# Makefile
#
# $Id$
#
# Makefile for Poco JSON testsuite
#

include $(POCO_BASE)/build/rules/global

objects = Driver JSONTest JSONTestSuite

target         = testrunner
target_version = 1
target_libs    = PocoJSON PocoFoundation CppUnit

include $(POCO_BASE)/build/rules/exec

ifdef POCO_UNBUNDLED
        SYSLIBS += -lpcre -lexpat
endif
