#if defined(__ELF__) && !defined(OS_FREEBSD)
#include <Common/AddressToLineCache.h>
#include <Common/SymbolIndex.h>
#include <IO/WriteHelpers.h>

#include <filesystem>

namespace DB
{


void AddressToLineCache::setResult(StringRef & result, const Dwarf::LocationInfo & location, const std::vector<Dwarf::SymbolizedFrame> &)
{
    const char * arena_begin = nullptr;
    WriteBufferFromArena out(arena, arena_begin);

    writeString(location.file.toString(), out);
    writeChar(':', out);
    writeIntText(location.line, out);

    out.finalize();
    result = out.complete();
}

StringRef AddressToLineCache::impl(uintptr_t addr)
{
    const SymbolIndex & symbol_index = SymbolIndex::instance();

    if (const auto * object = symbol_index.findObject(reinterpret_cast<const void *>(addr)))
    {
        auto dwarf_it = dwarfs.try_emplace(object->name, object->elf).first;
        if (!std::filesystem::exists(object->name))
            return {};

        Dwarf::LocationInfo location;
        std::vector<Dwarf::SymbolizedFrame> frames; // NOTE: not used in FAST mode.
        StringRef result;
        if (dwarf_it->second.findAddress(addr - uintptr_t(object->address_begin), location, Dwarf::LocationInfoMode::FAST, frames))
        {
            setResult(result, location, frames);
            return result;
        }
        return {object->name};
    }
    return {};
}

StringRef AddressToLineCache::implCached(uintptr_t addr)
{
    typename Map::LookupResult it;
    bool inserted;
    map.emplace(addr, it, inserted);
    if (inserted)
        it->getMapped() = impl(addr);
    return it->getMapped();
}

StringRef AddressToLineCache::get(uintptr_t addr)
{
    static AddressToLineCache cache;
    return cache.implCached(addr);
}

}
#endif
