#
# Makefile
#
# $Id$
#
# Makefile for Poco JSON
#

include $(POCO_BASE)/build/rules/global

INCLUDE += -I $(POCO_BASE)/JSON/include/Poco/JSON

objects = Array Object Parser Handler Stringifier \
	ParseHandler PrintHandler Query JSONException \
	Template TemplateCache

target         = PocoJSON
target_version = $(LIBVERSION)
target_libs    = PocoFoundation

include $(POCO_BASE)/build/rules/lib
