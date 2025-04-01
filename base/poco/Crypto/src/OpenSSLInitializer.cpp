//
// OpenSSLInitializer.cpp
//
// Library: Crypto
// Package: CryptoCore
// Module:  OpenSSLInitializer
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include <openssl/ssl.h>
#include <openssl/rand.h>
#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/conf.h>

#include "Poco/Crypto/OpenSSLInitializer.h"
#include "Poco/RandomStream.h"
#include "Poco/Thread.h"


using Poco::RandomInputStream;
using Poco::Thread;




namespace Poco {
namespace Crypto {


Poco::FastMutex* OpenSSLInitializer::_mutexes(0);
Poco::AtomicCounter OpenSSLInitializer::_rc;


OpenSSLInitializer::OpenSSLInitializer()
{
	initialize();
}


OpenSSLInitializer::~OpenSSLInitializer()
{
	try
	{
		uninitialize();
	}
	catch (...)
	{
		poco_unexpected();
	}
}


void OpenSSLInitializer::initialize()
{
	if (++_rc == 1)
	{
        OPENSSL_init_crypto(OPENSSL_INIT_LOAD_CONFIG, nullptr);
	}
}


void OpenSSLInitializer::uninitialize()
{
	if (--_rc == 0)
	{
		EVP_cleanup();
		ERR_free_strings();
		CRYPTO_set_locking_callback(0);
		CRYPTO_set_id_callback(0);
		delete [] _mutexes;

		CONF_modules_free();
	}
}


void OpenSSLInitializer::lock(int mode, int n, const char* file, int line)
{
	if (mode & CRYPTO_LOCK)
		_mutexes[n].lock();
	else
		_mutexes[n].unlock();
}


unsigned long OpenSSLInitializer::id()
{
	// Note: we use an old-style C cast here because
	// neither static_cast<> nor reinterpret_cast<>
	// work uniformly across all platforms.
	return (unsigned long) Poco::Thread::currentTid();
}


struct CRYPTO_dynlock_value* OpenSSLInitializer::dynlockCreate(const char* file, int line)
{
	return new CRYPTO_dynlock_value;
}


void OpenSSLInitializer::dynlock(int mode, struct CRYPTO_dynlock_value* lock, const char* file, int line)
{
	poco_check_ptr (lock);

	if (mode & CRYPTO_LOCK)
		lock->_mutex.lock();
	else
		lock->_mutex.unlock();
}


void OpenSSLInitializer::dynlockDestroy(struct CRYPTO_dynlock_value* lock, const char* file, int line)
{
	delete lock;
}


void initializeCrypto()
{
	OpenSSLInitializer::initialize();
}


void uninitializeCrypto()
{
	OpenSSLInitializer::uninitialize();
}


} } // namespace Poco::Crypto
