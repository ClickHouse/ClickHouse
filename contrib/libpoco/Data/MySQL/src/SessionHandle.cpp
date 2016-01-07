//
// SesssionHandle.cpp
//
// $Id: //poco/1.4/Data/MySQL/src/SessionHandle.cpp#1 $
//
// Library: Data
// Package: MySQL
// Module:  SessionHandle
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/MySQL/SessionHandle.h"
#include "Poco/Data/DataException.h"
#include "Poco/SingletonHolder.h"
#ifdef POCO_OS_FAMILY_UNIX
#include <pthread.h>
#endif


#define POCO_MYSQL_VERSION_NUMBER ((NDB_VERSION_MAJOR<<16) | (NDB_VERSION_MINOR<<8) | (NDB_VERSION_BUILD&0xFF))


namespace Poco {
namespace Data {
namespace MySQL {


#ifdef POCO_OS_FAMILY_UNIX
class ThreadCleanupHelper
{
public:
	ThreadCleanupHelper()
	{
		if (pthread_key_create(&_key, &ThreadCleanupHelper::cleanup) != 0)
			throw Poco::SystemException("cannot create TLS key for mysql cleanup");
	}
	
	void init()
	{
		if (pthread_setspecific(_key, reinterpret_cast<void*>(1)))
			throw Poco::SystemException("cannot set TLS key for mysql cleanup");
	}
	
	static ThreadCleanupHelper& instance()
	{
		return *_sh.get();
	}
	
	static void cleanup(void* data)
	{
		mysql_thread_end();
	}
	
private:
	pthread_key_t _key;
	static Poco::SingletonHolder<ThreadCleanupHelper> _sh;
};


Poco::SingletonHolder<ThreadCleanupHelper> ThreadCleanupHelper::_sh;
#endif


SessionHandle::SessionHandle(MYSQL* mysql): _pHandle(0)
{
	init(mysql);
#ifdef POCO_OS_FAMILY_UNIX
	ThreadCleanupHelper::instance().init();
#endif
}


void SessionHandle::init(MYSQL* mysql)
{
	if (!_pHandle)
	{
		_pHandle = mysql_init(mysql);
		if (!_pHandle)
			throw ConnectionException("mysql_init error");
	}
}


SessionHandle::~SessionHandle()
{
	close();
}


void SessionHandle::options(mysql_option opt)
{
	if (mysql_options(_pHandle, opt, 0) != 0)
		throw ConnectionException("mysql_options error", _pHandle);
}


void SessionHandle::options(mysql_option opt, bool b)
{
	my_bool tmp = b;
	if (mysql_options(_pHandle, opt, &tmp) != 0)
		throw ConnectionException("mysql_options error", _pHandle);
}


void SessionHandle::options(mysql_option opt, const char* c)
{
	if (mysql_options(_pHandle, opt, c) != 0)
		throw ConnectionException("mysql_options error", _pHandle);
}


void SessionHandle::options(mysql_option opt, unsigned int i)
{
#if (POCO_MYSQL_VERSION_NUMBER < 0x050108)
	const char* tmp = (const char *)&i;
#else
	const void* tmp = (const void *)&i;
#endif
	if (mysql_options(_pHandle, opt, tmp) != 0)
		throw ConnectionException("mysql_options error", _pHandle);
}


void SessionHandle::connect(const char* host, const char* user, const char* password, const char* db, unsigned int port)
{
#ifdef HAVE_MYSQL_REAL_CONNECT
	if (!mysql_real_connect(_pHandle, host, user, password, db, port, 0, 0))
		throw ConnectionFailedException(mysql_error(_pHandle));
#else
	if (!mysql_connect(_pHandle, host, user, password))
		throw ConnectionFailedException(mysql_error(_pHandle))
#endif
}


void SessionHandle::close()
{
	if (_pHandle)
	{
		mysql_close(_pHandle);
		_pHandle = 0;
	}
}


void SessionHandle::startTransaction()
{
	if (mysql_autocommit(_pHandle, false) != 0)
		throw TransactionException("Start transaction failed.", _pHandle);
}


void SessionHandle::commit()
{
	if (mysql_commit(_pHandle) != 0)
		throw TransactionException("Commit failed.", _pHandle);
}


void SessionHandle::rollback()
{
	if (mysql_rollback(_pHandle) != 0)
		throw TransactionException("Rollback failed.", _pHandle);
}


}}} // Poco::Data::MySQL
