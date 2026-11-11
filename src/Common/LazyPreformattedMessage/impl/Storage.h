#pragma once

#include <cstddef>
#include <utility>

namespace DB::LazyPreformattedMessageImpl
{

struct Storage
{
    static std::pair<size_t, void *> allocate(uint64_t hint, size_t size, size_t align);
    static void deallocate(size_t lane) noexcept;
};

}
