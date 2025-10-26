//
// NamedEvent_UNIX.cpp
//
// Library: Foundation
// Package: Processes
// Module:  NamedEvent
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/NamedEvent_UNIX.h"
#include "Poco/Format.h"
#include "Poco/Exception.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#if defined(sun) || defined(__APPLE__) || defined(__osf__) || defined(__QNX__) || defined(_AIX)
#include <semaphore.h>
#else
#include <unistd.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#endif


namespace Poco {


#if (POCO_OS == POCO_OS_LINUX) || (POCO_OS == POCO_OS_ANDROID) || (POCO_OS == POCO_OS_CYGWIN) || (POCO_OS == POCO_OS_FREE_BSD) || (POCO_OS == POCO_OS_SOLARIS)
	union semun
	{
		int                 val;
		struct semid_ds*    buf;
		unsigned short int* array;
		struct seminfo*     __buf;
	};
#elif (POCO_OS == POCO_OS_HPUX)
	union semun
	{
		int              val;
		struct semid_ds* buf;
		ushort*          array;
	};
#endif


NamedEventImpl::NamedEventImpl(const std::string& name):
	_name(name)
{
	std::string fileName = getFileName();
#if defined(sun) || defined(__APPLE__) || defined(__osf__) || defined(__QNX__) || defined(_AIX)
	_sem = sem_open(fileName.c_str(), O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO, 0);
	if ((long) _sem == (long) SEM_FAILED)
		throw SystemException(Poco::format("cannot create named mutex %s (sem_open() failed, errno=%d)", fileName, errno), _name);
#else
	int fd = open(fileName.c_str(), O_RDONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
	if (fd != -1)
		close(fd);
	else
		throw SystemException(Poco::format("cannot create named event %s (lockfile)", fileName), _name);
	key_t key = ftok(fileName.c_str(), 'p');
	if (key == -1)
		throw SystemException(Poco::format("cannot create named mutex %s (ftok() failed, errno=%d)", fileName, errno), _name);
	_semid = semget(key, 1, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH | IPC_CREAT | IPC_EXCL);
	if (_semid >= 0)
	{
		union semun arg;
		arg.val = 0;
		semctl(_semid, 0, SETVAL, arg);
	}
	else if (errno == EEXIST)
	{
		_semid = semget(key, 1, 0);
	}
	else throw SystemException(Poco::format("cannot create named mutex %s (semget() failed, errno=%d)", fileName, errno), _name);
#endif // defined(sun) || defined(__APPLE__) || defined(__osf__) || defined(__QNX__) || defined(_AIX)
}


NamedEventImpl::~NamedEventImpl()
{
#if defined(sun) || defined(__APPLE__) || defined(__osf__) || defined(__QNX__) || defined(_AIX)
	sem_close(_sem);
#endif
}


void NamedEventImpl::setImpl()
{
#if defined(sun) || defined(__APPLE__) || defined(__osf__) || defined(__QNX__) || defined(_AIX)
	if (sem_post(_sem) != 0)
	   	throw SystemException("cannot set named event", _name);
#else
	struct sembuf op;
	op.sem_num = 0;
	op.sem_op  = 1;
	op.sem_flg = 0;
	if (semop(_semid, &op, 1) != 0)
	   	throw SystemException("cannot set named event", _name);
#endif
}


void NamedEventImpl::waitImpl()
{
#if defined(sun) || defined(__APPLE__) || defined(__osf__) || defined(__QNX__) || defined(_AIX)
	int err;
	do
	{
		err = sem_wait(_sem);
	}
	while (err && errno == EINTR);
	if (err) throw SystemException("cannot wait for named event", _name);
#else
	struct sembuf op;
	op.sem_num = 0;
	op.sem_op  = -1;
	op.sem_flg = 0;
	int err;
	do
	{
		err = semop(_semid, &op, 1);
	}
	while (err && errno == EINTR);
	if (err) throw SystemException("cannot wait for named event", _name);
#endif
}


std::string NamedEventImpl::getFileName()
{
#if defined(sun) || defined(__APPLE__) || defined(__QNX__)
	std::string fn = "/";
#else
	std::string fn = "/tmp/";
#endif
	fn.append(_name);
	fn.append(".event");
	return fn;
}


} // namespace Poco
