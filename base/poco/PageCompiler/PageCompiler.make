#
# PageCompiler.make
#
# Make include file for PageCompiler executable
#
# This file defines the PAGE_COMPILER make variable
# which can be used to invoke the PageCompiler from
# a Makefile.
#

ifneq (,$(findstring debug,$(DEFAULT_TARGET) $(MAKECMDGOALS)))
ifneq (,$(findstring static,$(DEFAULT_TARGET) $(MAKECMDGOALS)))
PAGE_COMPILER = $(POCO_BASE)/PageCompiler/$(POCO_HOST_BINDIR)/static/cpspcd
else
PAGE_COMPILER = $(POCO_BASE)/PageCompiler/$(POCO_HOST_BINDIR)/cpspcd
endif
else
ifneq (,$(findstring static,$(DEFAULT_TARGET) $(MAKECMDGOALS)))
PAGE_COMPILER = $(POCO_BASE)/PageCompiler/$(POCO_HOST_BINDIR)/static/cpspc
else
PAGE_COMPILER = $(POCO_BASE)/PageCompiler/$(POCO_HOST_BINDIR)/cpspc
endif
endif
