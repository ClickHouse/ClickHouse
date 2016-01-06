#
# Makefile
#
# $Id: //poco/1.4/PageCompiler/samples/HTTPTimeServer/Makefile#1 $
#
# Makefile for Poco HTTPTimeServer
#

include $(POCO_BASE)/build/rules/global

# Where to find the PageCompiler executable
PAGECOMPILER = $(POCO_BASE)/PageCompiler/bin/$(POCO_HOST_OSNAME)/$(POCO_HOST_OSARCH)/cpspc

objects = HTTPTimeServerApp TimeHandler

target         = HTTPTimeServer
target_version = 1
target_libs    = PocoUtil PocoJSON PocoNet PocoXML PocoFoundation

include $(POCO_BASE)/build/rules/exec

ifdef POCO_UNBUNDLED
        SYSLIBS += -lz -lpcre -lexpat
endif

# Rule for runnning PageCompiler
src/%.cpp: src/%.cpsp
	@echo "** Compiling Page" $<
	$(PAGECOMPILER) $<
