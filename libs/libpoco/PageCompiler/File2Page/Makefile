#
# Makefile
#
# $Id: //poco/1.4/PageCompiler/File2Page/Makefile#1 $
#
# Makefile for Poco C++ Server Page Compiler File2Page Utility
#

include $(POCO_BASE)/build/rules/global

objects = File2Page

ifdef POCO_UNBUNDLED
        SYSLIBS += -lz -lpcre -lexpat
endif

target         = f2cpsp
target_version = 1
target_libs    = PocoUtil PocoXML PocoJSON PocoFoundation

include $(POCO_BASE)/build/rules/exec
