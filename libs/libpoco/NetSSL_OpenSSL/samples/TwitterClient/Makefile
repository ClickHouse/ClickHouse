#
# Makefile
#
# $Id: //poco/1.4/Net/samples/TwitterClient/Makefile#1 $
#
# Makefile for Poco Twitter Client
#

include $(POCO_BASE)/build/rules/global

# Note: linking order is important, do not change it.
ifeq ($(POCO_CONFIG),FreeBSD)
SYSLIBS += -lssl -lcrypto -lz
else
SYSLIBS += -lssl -lcrypto -lz -ldl
endif

objects = Twitter TweetApp

target         = tweet
target_version = 1
target_libs    = PocoNetSSL PocoCrypto PocoNet PocoUtil PocoJSON PocoXML PocoFoundation

include $(POCO_BASE)/build/rules/exec

ifdef POCO_UNBUNDLED
        SYSLIBS += -lz -lpcre -lexpat
endif
