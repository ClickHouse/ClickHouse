#
# Makefile
#
# $Id: //poco/1.4/Crypto/testsuite/Makefile#1 $
#
# Makefile for Poco Crypto testsuite
#

include $(POCO_BASE)/build/rules/global

# Note: linking order is important, do not change it.
ifeq ($(POCO_CONFIG),FreeBSD)
SYSLIBS += -lssl -lcrypto -lz
else
SYSLIBS += -lssl -lcrypto -lz -ldl
endif

objects = CryptoTestSuite Driver \
	CryptoTest DigestEngineTest RSATest

target         = testrunner
target_version = 1
target_libs    = PocoCrypto PocoFoundation CppUnit

include $(POCO_BASE)/build/rules/exec
