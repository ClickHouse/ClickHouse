#
# Makefile
#
# $Id: //poco/1.4/PageCompiler/Makefile#1 $
#
# Makefile for Poco C++ Server Page Compiler
#

include $(POCO_BASE)/build/rules/global

objects = Page PageReader \
	CodeWriter ApacheCodeWriter OSPCodeWriter \
	PageCompiler

ifdef POCO_UNBUNDLED
	SYSLIBS += -lz -lpcre -lexpat
endif

target         = cpspc
target_version = 1
target_libs    = PocoNet PocoUtil PocoXML PocoJSON PocoFoundation

include $(POCO_BASE)/build/rules/exec
