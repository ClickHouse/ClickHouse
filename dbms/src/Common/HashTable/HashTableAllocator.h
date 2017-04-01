#pragma once

#include <Common/Allocator.h>


using HashTableAllocator = Allocator<true>;

template <size_t N = 64>
using HashTableAllocatorWithStackMemory = AllocatorWithStackMemory<HashTableAllocator, N>;
