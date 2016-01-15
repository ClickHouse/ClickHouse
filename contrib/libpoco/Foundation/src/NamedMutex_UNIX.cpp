//
// NamedMutex_UNIX.cpp
//
// $Id: //poco/1.4/Foundation/src/NamedMutex_UNIX.cpp#1 $
//
// Library: Foundation
// Package: Processes
// Module:  NamedMutex
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/NamedMutex_UNIX.h"
#include "Poco/Format.h"
#include "Poco/Exception.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#if defined(sun) || defined(__APPLE__) || defined(__osf__) || defined(_AIX)
#include <semaphore.h>
#else
#include <unistd.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#endif


namespace Poco {


#if (POCO_OS == POCO_OS_LINUX) || (POCO_OS == POCO_OS_CYGWIN) || (POCO_OS == POCO_OS_FREE_BSD)
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


NamedMutexImpl::NamedMutexImpl(const std::string& name):
	_name(name)
{
	std::string fileName = getFileName();
#if defined(sun) || defined(__APPLE__) || defined(__osf__) || defined(__QNX__) || defined(_AIX)
	_sem = sem_open(fileName.c_str(), O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO, 1);
	if ((long) _sem == (long) SEM_FAILED) 
		throw SystemException(Poco::format("cannot create named mutex %s (sem_open() failed, errno=%d)", fileName, errno), _name);
#else
	int fd = open(fileName.c_str(), O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
	if (fd != -1)
		close(fd);
	else 
		throw SystemException(Poco::format("cannot create named mutex %s (lockfile)", fileName), _name);
	key_t key = ftok(fileName.c_str(), 0);
	if (key == -1)
		throw SystemException(Poco::format("cannot create named mutex %s (ftok() failed, errno=%d)", fileName, errno), _name);
	_semid = semget(key, 1, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH | IPC_CREAT | IPC_EXCL);
	if (_semid >= 0)
	{
		union semun arg;
		arg.val = 1;
		semctl(_semid, 0, SETVAL, arg);
	}
	else if (errno == EEXIST)
	{
		_semid = semget(key, 1, 0);
	}
	else throw SystemException(Poco::format("cannot create named mutex %s (semget() failed, errno=%d)", fileName, errno), _name);
#endif // defined(sun) || defined(__APPLE__) || defined(__osf__) || defined(__QNX__) || defined(_AIX)
}


NamedMutexImpl::~NamedMutexImpl()
{
#if defined(sun) || defined(__APPLE__) || defined(__osf__) || defined(__QNX__) || defined(_AIX)
	sem_close(_sem);
#endif
}


void NamedMutexImpl::lockImpl()
{
#if defined(sun) || defined(__APPLE__) || defined(__osf__) || defined(__QNX__) || defined(_AIX)
	int err;
	do
	{
		err = sem_wait(_sem);
	}
	while (err && errno == EINTR);
	if (err) throw SystemException("cannot lock named mutex", _name);
#else
	struct sembuf op;
	op.sem_num = 0;
	op.sem_op  = -1;
	op.sem_flg = SEM_UNDO;
	int err;
	do
	{
		err = semop(_semid, &op, 1);
	}
	while (err && errno == EINTR);
	if (err) throw SystemException("cannot lock named mutex", _name);
#endif
}


bool NamedMutexImpl::tryLockImpl()
{
#if defined(sun) || defined(__APPLE__) || defined(__osf__) || defined(__QNX__) || defined(_AIX)
	return sem_trywait(_sem) == 0;
#else
	struct sembuf op;
	op.sem_num = 0;
	op.sem_op  = -1;
	op.sem_flg = SEM_UNDO | IPC_NOWAIT;
	return semop(_semid, &op, 1) == 0;
#endif
}


void NamedMutexImpl::unlockImpl()
{
#if defined(sun) || defined(__APPLE__) || defined(__osf__) || defined(__QNX__) || defined(_AIX)
	if (sem_post(_sem) != 0)
	   	throw SystemException("cannot unlock named mutex", _name);
#else
	struct sembuf op;
	op.sem_num = 0;
	op.sem_op  = 1;
	op.sem_flg = SEM_UNDO;
	if (semop(_semid, &op, 1) != 0)
	   	throw SystemException("cannot unlock named mutex", _name);
#endif
}


std::string NamedMutexImpl::getFileName()
{
#if defined(sun) || defined(__APPLE__) || defined(__QNX__)
	std::string fn = "/";
#else
	std::string fn = "/tmp/";
#endif
	fn.append(_name);
	fn.append(".mutex");
	return fn;
}


} // namespace Poco

