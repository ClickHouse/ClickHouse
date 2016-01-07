//
// PipeImpl_WIN32.cpp
//
// $Id: //poco/1.4/Foundation/src/PipeImpl_WIN32.cpp#1 $
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


#include "Poco/PipeImpl_WIN32.h"
#include "Poco/Exception.h"


namespace Poco {


PipeImpl::PipeImpl()
{
	SECURITY_ATTRIBUTES attr;
	attr.nLength              = sizeof(attr);
	attr.lpSecurityDescriptor = NULL;
	attr.bInheritHandle       = FALSE;
	
	if (!CreatePipe(&_readHandle, &_writeHandle, &attr, 0))
		throw CreateFileException("anonymous pipe");
}


PipeImpl::~PipeImpl()
{
	closeRead();
	closeWrite();
}


int PipeImpl::writeBytes(const void* buffer, int length)
{
	poco_assert (_writeHandle != INVALID_HANDLE_VALUE);

	DWORD bytesWritten = 0;
	if (!WriteFile(_writeHandle, buffer, length, &bytesWritten, NULL))
		throw WriteFileException("anonymous pipe");
	return bytesWritten;
}


int PipeImpl::readBytes(void* buffer, int length)
{
	poco_assert (_readHandle != INVALID_HANDLE_VALUE);

	DWORD bytesRead = 0;
	BOOL ok = ReadFile(_readHandle, buffer, length, &bytesRead, NULL);
	if (ok || GetLastError() == ERROR_BROKEN_PIPE)
		return bytesRead;
	else
		throw ReadFileException("anonymous pipe");
}


PipeImpl::Handle PipeImpl::readHandle() const
{
	return _readHandle;
}


PipeImpl::Handle PipeImpl::writeHandle() const
{
	return _writeHandle;
}


void PipeImpl::closeRead()
{
	if (_readHandle != INVALID_HANDLE_VALUE)
	{
		CloseHandle(_readHandle);
		_readHandle = INVALID_HANDLE_VALUE;
	}
}


void PipeImpl::closeWrite()
{
	if (_writeHandle != INVALID_HANDLE_VALUE)
	{
		CloseHandle(_writeHandle);
		_writeHandle = INVALID_HANDLE_VALUE;
	}
}


} // namespace Poco
