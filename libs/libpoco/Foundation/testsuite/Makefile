#
# Makefile
#
# $Id: //poco/1.4/Foundation/testsuite/Makefile#1 $
#
# Makefile for Poco Foundation testsuite
#

.PHONY: projects
clean all: projects
projects:
	$(MAKE) -f Makefile-Driver $(MAKECMDGOALS)
ifneq ($(LINKMODE),STATIC)
	$(MAKE) -f Makefile-TestLibrary $(MAKECMDGOALS)
endif
	$(MAKE) -f Makefile-TestApp $(MAKECMDGOALS)
