//
// SharedMemoryImpl.cpp
//
// $Id: //poco/1.4/Foundation/src/SharedMemory_WIN32.cpp#1 $
//
// Library: Foundation
// Package: Processes
// Module:  SharedMemoryImpl
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/SharedMemory_WIN32.h"
#include "Poco/Error.h"
#include "Poco/Exception.h"
#include "Poco/File.h"
#include "Poco/Format.h"
#if defined (POCO_WIN32_UTF8)
#include "Poco/UnicodeConverter.h"
#endif
#include "Poco/UnWindows.h"


namespace Poco {


SharedMemoryImpl::SharedMemoryImpl(const std::string& name, std::size_t size, SharedMemory::AccessMode mode, const void*, bool):
	_name(name),
	_memHandle(INVALID_HANDLE_VALUE),
	_fileHandle(INVALID_HANDLE_VALUE),
	_size(static_cast<DWORD>(size)),
	_mode(PAGE_READONLY),
	_address(0)
{
	if (mode == SharedMemory::AM_WRITE)
		_mode = PAGE_READWRITE;

#if defined (POCO_WIN32_UTF8)
	std::wstring utf16name;
	UnicodeConverter::toUTF16(_name, utf16name);
	_memHandle = CreateFileMappingW(INVALID_HANDLE_VALUE, NULL, _mode, 0, _size, utf16name.c_str());
#else
	_memHandle = CreateFileMappingA(INVALID_HANDLE_VALUE, NULL, _mode, 0, _size, _name.c_str());
#endif

	if (!_memHandle)
	{
		DWORD dwRetVal = GetLastError();
#if defined (_WIN32_WCE)
		throw SystemException(format("Cannot create shared memory object %s [Error %d: %s]", _name, static_cast<int>(dwRetVal), Error::getMessage(dwRetVal)));
#else
		if (_mode != PAGE_READONLY || dwRetVal != 5)
			throw SystemException(format("Cannot create shared memory object %s [Error %d: %s]", _name, static_cast<int>(dwRetVal), Error::getMessage(dwRetVal)));

#if defined (POCO_WIN32_UTF8)
		_memHandle = OpenFileMappingW(PAGE_READONLY, FALSE, utf16name.c_str());
#else
		_memHandle = OpenFileMappingA(PAGE_READONLY, FALSE, _name.c_str());
#endif
		if (!_memHandle)
		{
			dwRetVal = GetLastError();
			throw SystemException(format("Cannot open shared memory object %s [Error %d: %s]", _name, static_cast<int>(dwRetVal), Error::getMessage(dwRetVal)));
		}
#endif 
	}
	map();
}


SharedMemoryImpl::SharedMemoryImpl(const Poco::File& file, SharedMemory::AccessMode mode, const void*):
	_name(file.path()),
	_memHandle(INVALID_HANDLE_VALUE),
	_fileHandle(INVALID_HANDLE_VALUE),
	_size(0),
	_mode(PAGE_READONLY),
	_address(0)
{
	if (!file.exists() || !file.isFile())
		throw FileNotFoundException(_name);

	_size = static_cast<DWORD>(file.getSize());

	DWORD shareMode = FILE_SHARE_READ | FILE_SHARE_WRITE;
	DWORD fileMode  = GENERIC_READ;

	if (mode == SharedMemory::AM_WRITE)
	{
		_mode = PAGE_READWRITE;
		fileMode |= GENERIC_WRITE;
	}

#if defined (POCO_WIN32_UTF8)
	std::wstring utf16name;
	UnicodeConverter::toUTF16(_name, utf16name);
	_fileHandle = CreateFileW(utf16name.c_str(), fileMode, shareMode, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
#else
	_fileHandle = CreateFileA(_name.c_str(), fileMode, shareMode, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
#endif

	if (_fileHandle == INVALID_HANDLE_VALUE)
		throw OpenFileException("Cannot open memory mapped file", _name);

	_memHandle = CreateFileMapping(_fileHandle, NULL, _mode, 0, 0, NULL);
	if (!_memHandle)
	{
		DWORD dwRetVal = GetLastError();
		CloseHandle(_fileHandle);
		_fileHandle = INVALID_HANDLE_VALUE;
		throw SystemException(format("Cannot map file into shared memory %s [Error %d: %s]", _name, (int)dwRetVal, Error::getMessage(dwRetVal)));
	}
	map();
}


SharedMemoryImpl::~SharedMemoryImpl()
{
	unmap();
	close();
}


void SharedMemoryImpl::map()
{
	DWORD access = FILE_MAP_READ;
	if (_mode == PAGE_READWRITE)
		access = FILE_MAP_WRITE;
	LPVOID addr = MapViewOfFile(_memHandle, access, 0, 0, _size);
	if (!addr)
	{
		DWORD dwRetVal = GetLastError();
		throw SystemException(format("Cannot map shared memory object %s [Error %d: %s]", _name, (int)dwRetVal, Error::getMessage(dwRetVal)));
	}

	_address = static_cast<char*>(addr);
}


void SharedMemoryImpl::unmap()
{
	if (_address)
	{
		UnmapViewOfFile(_address);
		_address = 0;
		return;
	}
}


void SharedMemoryImpl::close()
{
	if (_memHandle != INVALID_HANDLE_VALUE)
	{
		CloseHandle(_memHandle);
		_memHandle = INVALID_HANDLE_VALUE;
	}

	if (_fileHandle != INVALID_HANDLE_VALUE)
	{
		CloseHandle(_fileHandle);
		_fileHandle = INVALID_HANDLE_VALUE;
	}
}


} // namespace Poco
