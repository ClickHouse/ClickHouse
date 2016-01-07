//
// MemoryPool.h
//
// $Id: //poco/1.4/Foundation/include/Poco/MemoryPool.h#1 $
//
// Library: Foundation
// Package: Core
// Module:  MemoryPool
//
// Definition of the MemoryPool class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_MemoryPool_INCLUDED
#define Foundation_MemoryPool_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Mutex.h"
#include <vector>
#include <cstddef>


namespace Poco {


class Foundation_API MemoryPool
	/// A simple pool for fixed-size memory blocks.
	///
	/// The main purpose of this class is to speed-up
	/// memory allocations, as well as to reduce memory
	/// fragmentation in situations where the same blocks
	/// are allocated all over again, such as in server
	/// applications.
	///
	/// All allocated blocks are retained for future use.
	/// A limit on the number of blocks can be specified.
	/// Blocks can be preallocated.
{
public:
	MemoryPool(std::size_t blockSize, int preAlloc = 0, int maxAlloc = 0);
		/// Creates a MemoryPool for blocks with the given blockSize.
		/// The number of blocks given in preAlloc are preallocated.
		
	~MemoryPool();

	void* get();
		/// Returns a memory block. If there are no more blocks
		/// in the pool, a new block will be allocated.
		///
		/// If maxAlloc blocks are already allocated, an
		/// OutOfMemoryException is thrown.
		
	void release(void* ptr);
		/// Releases a memory block and returns it to the pool.
	
	std::size_t blockSize() const;
		/// Returns the block size.
		
	int allocated() const;
		/// Returns the number of allocated blocks.
		
	int available() const;
		/// Returns the number of available blocks in the pool.

private:
	MemoryPool();
	MemoryPool(const MemoryPool&);
	MemoryPool& operator = (const MemoryPool&);
	
	enum
	{
		BLOCK_RESERVE = 128
	};
	
	typedef std::vector<char*> BlockVec;
	
	std::size_t _blockSize;
	int         _maxAlloc;
	int         _allocated;
	BlockVec    _blocks;
	FastMutex   _mutex;
};


//
// inlines
//
inline std::size_t MemoryPool::blockSize() const
{
	return _blockSize;
}


inline int MemoryPool::allocated() const
{
	return _allocated;
}


inline int MemoryPool::available() const
{
	return (int) _blocks.size();
}


} // namespace Poco


#endif // Foundation_MemoryPool_INCLUDED
