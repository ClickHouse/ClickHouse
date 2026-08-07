//
// SharedMemoryImpl.cpp
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


#include "Poco/SharedMemory_POSIX.h"
#include "Poco/Exception.h"
#include "Poco/File.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>


namespace Poco {


SharedMemoryImpl::SharedMemoryImpl(const std::string& name, std::size_t size, SharedMemory::AccessMode mode, const void* addrHint, bool server):
	_size(size),
	_fd(-1),
	_address(0),
	_access(mode),
	_name("/"),
	_fileMapped(false),
	_server(server)
{
#if POCO_OS == POCO_OS_HPUX
	_name.append("tmp/");
#endif

	_name.append(name);

	int flags = _server ? O_CREAT : 0;
	if (_access == SharedMemory::AM_WRITE)
		flags |= O_RDWR;
	else
		flags |= O_RDONLY;

	// open the shared memory segment
	_fd = ::shm_open(_name.c_str(), flags, S_IRUSR | S_IWUSR);
	if (_fd == -1)
		throw SystemException("Cannot create shared memory object", _name);

	// now set the correct size for the segment
	if (_server && -1 == ::ftruncate(_fd, size))
	{
		::close(_fd);
		_fd = -1;
		::shm_unlink(_name.c_str());
		throw SystemException("Cannot resize shared memory object", _name);
	}
	map(addrHint);
}


SharedMemoryImpl::SharedMemoryImpl(const Poco::File& file, SharedMemory::AccessMode mode, const void* addrHint):
	_size(0),
	_fd(-1),
	_address(0),
	_access(mode),
	_name(file.path()),
	_fileMapped(true),
	_server(false)
{
	if (!file.exists() || !file.isFile())
		throw FileNotFoundException(file.path());

	_size = file.getSize();
	int flag = O_RDONLY;
	if (mode == SharedMemory::AM_WRITE)
		flag = O_RDWR;
	_fd = ::open(_name.c_str(), flag);
	if (-1 == _fd)
		throw OpenFileException("Cannot open memory mapped file", _name);

	map(addrHint);
}


SharedMemoryImpl::~SharedMemoryImpl()
{
	unmap();
	close();
}


void SharedMemoryImpl::map(const void* addrHint)
{
	int access = PROT_READ;
	if (_access == SharedMemory::AM_WRITE)
		access |= PROT_WRITE;

	void* addr = ::mmap(const_cast<void*>(addrHint), _size, access, MAP_SHARED, _fd, 0);
	if (addr == MAP_FAILED)
		throw SystemException("Cannot map file into shared memory", _name);

	_address = static_cast<char*>(addr);
}


void SharedMemoryImpl::unmap()
{
	if (_address)
	{
		::munmap(_address, _size);
	}
}


void SharedMemoryImpl::close()
{
	if (_fd != -1)
	{
		::close(_fd);
		_fd = -1;
	}
	if (!_fileMapped && _server)
	{
		::shm_unlink(_name.c_str());
	}
}


} // namespace Poco
