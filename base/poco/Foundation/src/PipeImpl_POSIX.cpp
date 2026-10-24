//
// PipeImpl_POSIX.cpp
//
// Library: Foundation
// Package: Processes
// Module:  PipeImpl
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/PipeImpl_POSIX.h"
#include "Poco/Exception.h"
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>


namespace Poco {


PipeImpl::PipeImpl()
{
	int fds[2];
	// Close-on-exec, so that processes started by the program do not inherit the pipe.
#if defined(__APPLE__)
	int rc = pipe(fds);
	if (rc == 0 && (fcntl(fds[0], F_SETFD, FD_CLOEXEC) == -1 || fcntl(fds[1], F_SETFD, FD_CLOEXEC) == -1))
	{
		close(fds[0]);
		close(fds[1]);
		rc = -1;
	}
#else
	int rc = pipe2(fds, O_CLOEXEC);
#endif
	if (rc == 0)
	{
		_readfd  = fds[0];
		_writefd = fds[1];
	}
	else throw CreateFileException("anonymous pipe");
}


PipeImpl::~PipeImpl()
{
	closeRead();
	closeWrite();
}


int PipeImpl::writeBytes(const void* buffer, int length)
{
	poco_assert (_writefd != -1);

	int n;
	do
	{
		n = write(_writefd, buffer, length);
	}
	while (n < 0 && errno == EINTR);
	if (n >= 0)
		return n;
	else
		throw WriteFileException("anonymous pipe");
}


int PipeImpl::readBytes(void* buffer, int length)
{
	poco_assert (_readfd != -1);

	int n;
	do
	{
		n = read(_readfd, buffer, length);
	}
	while (n < 0 && errno == EINTR);
	if (n >= 0)
		return n;
	else
		throw ReadFileException("anonymous pipe");
}


PipeImpl::Handle PipeImpl::readHandle() const
{
	return _readfd;
}


PipeImpl::Handle PipeImpl::writeHandle() const
{
	return _writefd;
}


void PipeImpl::closeRead()
{
	if (_readfd != -1)
	{
		close(_readfd);
		_readfd = -1;
	}
}


void PipeImpl::closeWrite()
{
	if (_writefd != -1)
	{
		close(_writefd);
		_writefd = -1;
	}
}


} // namespace Poco
