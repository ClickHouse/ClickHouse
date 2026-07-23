#if (defined(__ELF__) && !defined(OS_FREEBSD)) || defined(OS_DARWIN)

#include <Common/AddressToLineCache.h>
#include <Common/SymbolIndex.h>
#include <Common/VectorWithMemoryTracking.h>
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

    /// Locate the loaded object that contains this address and convert the runtime address to the
    /// address expected by the DWARF lookup. Callers may pass either absolute runtime addresses or
    /// file offsets; if the address does not fall within any object, fall back to the current object
    /// and treat the address as an already-converted lookup address.
    const auto * object = symbol_index.findObject(reinterpret_cast<const void *>(addr));
    uintptr_t lookup_addr = addr;
    if (object)
    {
#if defined(OS_DARWIN)
        /// On macOS, subtract the ASLR slide to get the linked (pre-ASLR) address (see StackTrace.cpp).
        lookup_addr = addr - object->slide;
#else
        /// On ELF, subtract the load address to get the file offset within the object.
        lookup_addr = addr - reinterpret_cast<uintptr_t>(object->address_begin);
#endif
    }
    else
        object = symbol_index.thisObject();

    if (object)
    {
#if defined(OS_DARWIN)
        /// File/line info comes from a dSYM bundle located next to the binary, if present.
        if (!object->dsym)
            return {};
        auto dwarf_it = dwarfs.try_emplace(object->name, object->dsym).first;
#else
        auto dwarf_it = dwarfs.try_emplace(object->name, object->elf).first;
        if (!std::filesystem::exists(object->name))
            return {};
#endif

        Dwarf::LocationInfo location;
        VectorWithMemoryTracking<Dwarf::SymbolizedFrame> frames; // NOTE: not used in FAST mode.
        std::string_view result;
        if (dwarf_it->second.findAddress(lookup_addr, location, Dwarf::LocationInfoMode::FAST, frames))
        {
            setResult(result, location);
            return result;
        }
        /// The result holds source locations only; an unresolved frame stays empty rather than
        /// borrowing the object path (that would violate the file:line:col column contract).
        return {};
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
    typename Map::LookupResult it = nullptr;
    bool inserted = false;
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
