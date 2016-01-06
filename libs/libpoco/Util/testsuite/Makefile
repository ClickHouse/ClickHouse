#
# Makefile
#
# $Id: //poco/1.4/Util/testsuite/Makefile#1 $
#
# Makefile for Poco Util testsuite
#

include $(POCO_BASE)/build/rules/global

objects = AbstractConfigurationTest ConfigurationTestSuite \
	ConfigurationMapperTest ConfigurationViewTest Driver  \
	HelpFormatterTest IniFileConfigurationTest LayeredConfigurationTest \
	LoggingConfiguratorTest MapConfigurationTest \
	OptionProcessorTest OptionSetTest OptionTest \
	OptionsTestSuite PropertyFileConfigurationTest \
	SystemConfigurationTest UtilTestSuite XMLConfigurationTest \
	FilesystemConfigurationTest ValidatorTest \
	TimerTestSuite TimerTest \
	JSONConfigurationTest

target         = testrunner
target_version = 1
target_libs    = PocoUtil PocoXML PocoJSON PocoFoundation CppUnit

include $(POCO_BASE)/build/rules/exec

ifdef POCO_UNBUNDLED
        SYSLIBS += -lz -lpcre -lexpat
endif
