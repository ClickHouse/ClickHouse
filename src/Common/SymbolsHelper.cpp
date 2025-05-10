#if defined(__ELF__) && !defined(OS_FREEBSD)
#include <Core/Field.h>
#include <base/demangle.h>
#include <Common/SymbolIndex.h>
#include <Common/AddressToLineCache.h>


namespace DB
{

std::pair<Array, Array> generateArraysSymbolsLines(const std::vector<UInt64> & trace)
{
    Array symbols;
    Array lines;
    size_t num_frames = trace.size();
    symbols.reserve(num_frames);
    lines.reserve(num_frames);

    const SymbolIndex & symbol_index = SymbolIndex::instance();

    for (size_t frame = 0; frame < num_frames; ++frame)
    {
        if (const auto * symbol = symbol_index.findSymbol(reinterpret_cast<const void *>(trace[frame])))
        {
            std::string_view mangled_symbol(symbol->name);

            auto demangled = tryDemangle(symbol->name);
            if (demangled)
                symbols.emplace_back(std::string_view(demangled.get()));
            else
                symbols.emplace_back(std::string_view(symbol->name));

            lines.emplace_back(AddressToLineCache::get(trace[frame]).toView());
        }
        else
        {
            symbols.emplace_back(String());
            lines.emplace_back(String());
        }
    }

    return {symbols, lines};
}

}
#endif
