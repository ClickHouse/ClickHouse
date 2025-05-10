#if defined(__ELF__) && !defined(OS_FREEBSD)
#include <Common/AddressToLineCache.h>
#include <Common/SymbolIndex.h>
#include <IO/WriteHelpers.h>
//#include <Core/Field.h>
//#include <base/demangle.h>

#include <filesystem>

namespace DB
{

//std::pair<Array, Array> generateArraysSymbolsLines(std::vector<UInt64> trace)
//{
//    Array symbols;
//    Array lines;
//    size_t num_frames = trace.size();
//    symbols.reserve(num_frames);
//    lines.reserve(num_frames);
//
//    const SymbolIndex & symbol_index = SymbolIndex::instance();
//
//    for (size_t frame = 0; frame < num_frames; ++frame)
//    {
//        if (const auto * symbol = symbol_index.findSymbol(reinterpret_cast<const void *>(trace[frame])))
//        {
//            std::string_view mangled_symbol(symbol->name);
//
//            auto demangled = tryDemangle(symbol->name);
//            if (demangled)
//                symbols.emplace_back(std::string_view(demangled.get()));
//            else
//                symbols.emplace_back(std::string_view(symbol->name));
//
//            lines.emplace_back(AddressToLineCache::get(trace[frame]).toView());
//        }
//        else
//        {
//            symbols.emplace_back(String());
//            lines.emplace_back(String());
//        }
//    }
//
//    return {symbols, lines};
//}

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
