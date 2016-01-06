#
# Makefile
#
# $Id: //poco/1.4/XML/testsuite/Makefile#1 $
#
# Makefile for Poco XML testsuite
#

include $(POCO_BASE)/build/rules/global

objects = AttributesImplTest ChildNodesTest DOMTestSuite DocumentTest \
	DocumentTypeTest Driver ElementTest EventTest NamePoolTest NameTest \
	NamespaceSupportTest NodeIteratorTest NodeTest ParserWriterTest \
	SAXParserTest SAXTestSuite TextTest TreeWalkerTest \
	XMLTestSuite XMLWriterTest NodeAppenderTest

target         = testrunner
target_version = 1
target_libs    = PocoXML PocoFoundation CppUnit

include $(POCO_BASE)/build/rules/exec

ifdef POCO_UNBUNDLED
        SYSLIBS += -lexpat
endif
