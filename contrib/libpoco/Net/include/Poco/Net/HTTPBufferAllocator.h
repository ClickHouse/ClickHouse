//
// HTTPBufferAllocator.h
//
// $Id: //poco/Main/template/class.h#4 $
//
// Library: Net
// Package: HTTP
// Module:  HTTPBufferAllocator
//
// Definition of the HTTPBufferAllocator class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPBufferAllocator_INCLUDED
#define Net_HTTPBufferAllocator_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/MemoryPool.h"
#include <ios>


namespace Poco {
namespace Net {


class Net_API HTTPBufferAllocator
	/// A BufferAllocator for HTTP streams.
{
public:
	static char* allocate(std::streamsize size);
	static void deallocate(char* ptr, std::streamsize size);

	enum
	{
		BUFFER_SIZE = 4096
	};

private:
	static Poco::MemoryPool _pool;
};


} } // namespace Poco::Net


#endif // Net_HTTPBufferAllocator_INCLUDED
