#pragma once
#if defined(__ELF__) && !defined(OS_FREEBSD)

#include <IO/WriteBufferFromArena.h>
#include <Common/Dwarf.h>
#include <Common/HashTable/HashMap.h>


namespace DB
{

class AddressToLineCache
{
public:
    static StringRef get(uintptr_t addr);

protected:
    void setResult(StringRef & result, const Dwarf::LocationInfo & location, const std::vector<Dwarf::SymbolizedFrame> &);
    StringRef impl(uintptr_t addr);
    StringRef implCached(uintptr_t addr);

private:
    using Map = HashMap<uintptr_t, StringRef>;
    Arena arena;
    Map map;
    std::unordered_map<std::string, Dwarf> dwarfs;
};

}
#endif
