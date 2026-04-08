#if defined(__ELF__) && !defined(OS_FREEBSD)

#include <Common/AddressToLineCache.h>
#include <Common/SymbolIndex.h>
#include <IO/WriteBufferFromArena.h>
#include <IO/WriteHelpers.h>

#include <filesystem>
#include <shared_mutex>

namespace DB
{

void AddressToLineCache::setResult(std::string_view & result, const Dwarf::LocationInfo & location)
{
    const char * arena_begin = nullptr;
    WriteBufferFromArena out(arena, arena_begin);

    writeString(location.file.toString(), out);
    writeChar(':', out);
    writeIntText(location.line, out);
    writeChar(':', out);
    writeIntText(location.column, out);

    out.finalize();
    result = out.complete();
}

std::string_view AddressToLineCache::impl(uintptr_t addr)
{
    const SymbolIndex & symbol_index = SymbolIndex::instance();

    /// Convert virtual address to file offset if it falls within a loaded object.
    /// Callers may pass either absolute runtime addresses or file offsets.
    const auto * object = symbol_index.findObject(reinterpret_cast<const void *>(addr));
    uintptr_t physical_addr = addr;
    if (object)
        physical_addr = addr - reinterpret_cast<uintptr_t>(object->address_begin);
    else
        object = symbol_index.thisObject();

    if (object)
    {
        auto dwarf_it = dwarfs.try_emplace(object->name, object->elf).first;
        if (!std::filesystem::exists(object->name))
            return {};

        Dwarf::LocationInfo location;
        std::vector<Dwarf::SymbolizedFrame> frames; // NOTE: not used in FAST mode.
        std::string_view result;
        if (dwarf_it->second.findAddress(physical_addr, location, Dwarf::LocationInfoMode::FAST, frames))
        {
            setResult(result, location);
            return result;
        }
        return object->name;
    }
    return {};
}

std::string_view AddressToLineCache::implCached(uintptr_t addr)
{
    /// Fast path: read lock — concurrent reads don't block each other
    {
        std::shared_lock read_lock(mutex);
        if (auto * it = map.find(addr); it)
            return it->getMapped();
    }

    /// Slow path: write lock — DWARF lookup + insert
    std::unique_lock write_lock(mutex);

    /// Double-check: another thread may have inserted while we waited
    typename Map::LookupResult it;
    bool inserted;
    map.emplace(addr, it, inserted);
    if (inserted)
        it->getMapped() = impl(addr);
    return it->getMapped();
}

std::string_view AddressToLineCache::get(uintptr_t addr)
{
    static AddressToLineCache cache;
    return cache.implCached(addr);
}

}

#endif
