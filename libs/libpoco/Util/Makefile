#
# Makefile
#
# $Id: //poco/1.4/Util/Makefile#2 $
#
# Makefile for Poco Util
#

include $(POCO_BASE)/build/rules/global

objects = AbstractConfiguration Application ConfigurationMapper \
	ConfigurationView HelpFormatter IniFileConfiguration LayeredConfiguration \
	LoggingConfigurator LoggingSubsystem MapConfiguration \
	Option OptionException OptionProcessor OptionSet \
	PropertyFileConfiguration Subsystem SystemConfiguration \
	FilesystemConfiguration ServerApplication \
	Validator IntValidator RegExpValidator OptionCallback \
	Timer TimerTask

ifeq ($(findstring MinGW, $(POCO_CONFIG)), MinGW)
	objects += WinService WinRegistryKey WinRegistryConfiguration
endif

target         = PocoUtil
target_version = $(LIBVERSION)
target_libs    = PocoFoundation

ifndef POCO_UTIL_NO_XMLCONFIGURATION
objects     += XMLConfiguration
target_libs += PocoXML
endif

ifndef POCO_UTIL_NO_JSONCONFIGURATION
objects     += JSONConfiguration
target_libs += PocoJSON
endif

include $(POCO_BASE)/build/rules/lib
