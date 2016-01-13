//
// HTTPBufferAllocator.cpp
//
// $Id: //poco/Main/template/class.cpp#4 $
//
// Library: Net
// Package: HTTP
// Module:  HTTPBufferAllocator
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/HTTPBufferAllocator.h"


using Poco::MemoryPool;


namespace Poco {
namespace Net {


MemoryPool HTTPBufferAllocator::_pool(HTTPBufferAllocator::BUFFER_SIZE, 16);


char* HTTPBufferAllocator::allocate(std::streamsize size)
{
	poco_assert_dbg (size == BUFFER_SIZE);

	return reinterpret_cast<char*>(_pool.get());
}


void HTTPBufferAllocator::deallocate(char* ptr, std::streamsize size)
{
	poco_assert_dbg (size == BUFFER_SIZE);

	_pool.release(ptr);
}


} } // namespace Poco::Net
