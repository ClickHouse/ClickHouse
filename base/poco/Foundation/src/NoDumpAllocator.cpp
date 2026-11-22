//
// NoDumpAllocator.cpp
//
// Library: Foundation
// Package: Core
// Module:  NoDumpAllocator
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/NoDumpAllocator.h"


#include <cstdlib>
#include <new>


namespace Poco {


__attribute__((weak)) void* allocateNoDump(std::size_t bytes)
{
	if (void* ptr = std::malloc(bytes))
		return ptr;
	throw std::bad_alloc();
}


__attribute__((weak)) void deallocateNoDump(void* ptr) noexcept
{
	std::free(ptr);
}


} // namespace Poco
