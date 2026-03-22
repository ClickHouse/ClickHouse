#pragma once

#if defined(__ELF__) && !defined(OS_FREEBSD)

#include <string_view>
#include <unordered_map>
#include <Common/Arena.h>
#include <Common/Dwarf.h>
#include <Common/HashTable/HashMap.h>
#include <Common/SharedMutex.h>

namespace DB
{

/// Cached address-to-"file:line:column" resolution.
/// Thread-safe: uses a SharedMutex for concurrent reads.
class AddressToLineCache
{
public:
    static std::string_view get(uintptr_t addr);

private:
    void setResult(std::string_view & result, const Dwarf::LocationInfo & location);
    std::string_view impl(uintptr_t addr);
    std::string_view getCached(uintptr_t addr);

    Arena arena;
    using Map = HashMap<uintptr_t, std::string_view>;
    Map map;
    std::unordered_map<std::string, Dwarf> dwarfs;
    mutable SharedMutex mutex;
};

}

#endif
