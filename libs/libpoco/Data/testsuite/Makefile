#
# Makefile
#
# $Id: //poco/Main/Data/testsuite/Makefile#7 $
#
# Makefile for Poco Data testsuite
#

include $(POCO_BASE)/build/rules/global

ifeq ($(findstring SunOS,$(POCO_HOST_OSNAME)),SunOS)
  POCO_SUN_FORTE = $(findstring SunOS-SunForte, $(POCO_CONFIG))
  POCO_SUN_STUDIO = $(findstring SunOS-SunStudio, $(POCO_CONFIG))
  ifneq (,$or ($(POCO_SUN_FORTE), $(POCO_SUN_STUDIO)))
    CXXFLAGS += -erroff=hidevf
  endif
endif

objects = DataTestSuite Driver \
	DataTest SessionPoolTest \
	Binder Extractor Preparator SessionImpl Connector TestStatementImpl

target         = testrunner
target_version = 1
target_libs    = PocoData PocoFoundation CppUnit

include $(POCO_BASE)/build/rules/exec
