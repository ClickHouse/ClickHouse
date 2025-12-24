#pragma once

#include <Common/AllocatorWithMemoryTracking.h>

#include <list>
#include <vector>

namespace DB
{

template <typename T>
using SafeList = std::list<T, AllocatorWithMemoryTracking<T>>;

template <typename T>
using SafeVector = std::vector<T, AllocatorWithMemoryTracking<T>>;

}
