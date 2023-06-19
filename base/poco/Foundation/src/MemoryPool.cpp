//
// MemoryPool.cpp
//
// Library: Foundation
// Package: Core
// Module:  MemoryPool
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MemoryPool.h"
#include "Poco/Exception.h"


namespace Poco {


MemoryPool::MemoryPool(std::size_t blockSize, int preAlloc, int maxAlloc):
	_blockSize(blockSize),
	_maxAlloc(maxAlloc),
	_allocated(preAlloc)
{
	poco_assert (maxAlloc == 0 || maxAlloc >= preAlloc);
	poco_assert (preAlloc >= 0 && maxAlloc >= 0);

	int r = BLOCK_RESERVE;
	if (preAlloc > r)
		r = preAlloc;
	if (maxAlloc > 0 && maxAlloc < r)
		r = maxAlloc;
	_blocks.reserve(r);
	
	try
	{
		for (int i = 0; i < preAlloc; ++i)
		{
			_blocks.push_back(new char[_blockSize]);
		}
	}
	catch (...)
	{
		clear();
		throw;
	}
}

	
MemoryPool::~MemoryPool()
{
	clear();
}


void MemoryPool::clear()
{
	for (BlockVec::iterator it = _blocks.begin(); it != _blocks.end(); ++it)
	{
		delete [] *it;
	}
	_blocks.clear();
}


void* MemoryPool::get()
{
	FastMutex::ScopedLock lock(_mutex);
	
	if (_blocks.empty())
	{
		if (_maxAlloc == 0 || _allocated < _maxAlloc)
		{
			++_allocated;
			return new char[_blockSize];
		}
		else throw OutOfMemoryException("MemoryPool exhausted");
	}
	else
	{
		char* ptr = _blocks.back();
		_blocks.pop_back();
		return ptr;
	}
}

	
void MemoryPool::release(void* ptr)
{
	FastMutex::ScopedLock lock(_mutex);
	
	try
	{
		_blocks.push_back(reinterpret_cast<char*>(ptr));
	}
	catch (...)
	{
		delete [] reinterpret_cast<char*>(ptr);
	}
}


} // namespace Poco
