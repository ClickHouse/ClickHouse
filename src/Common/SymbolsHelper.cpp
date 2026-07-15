#if (defined(__ELF__) && !defined(OS_FREEBSD)) || defined(OS_DARWIN)

#include <Common/SymbolsHelper.h>
#include <Common/AddressToLineCache.h>
#include <Common/SymbolIndex.h>
#include <base/demangle.h>

#include <cstring>

namespace DB
{

std::pair<std::vector<String>, std::vector<String>>
symbolizeTrace(const void * const * frame_pointers, size_t size)
{
    std::vector<String> symbols;
    std::vector<String> lines;
    symbols.reserve(size);
    lines.reserve(size);

    const SymbolIndex & symbol_index = SymbolIndex::instance();
    for (size_t i = 0; i < size; ++i)
    {
        const void * addr = frame_pointers[i];

        /// The symbol name (from the symbol table) and the source location (from DWARF debug info)
        /// are looked up independently: DWARF line resolution does not depend on the symbol table.
        /// So an address with no matching symbol can still have a valid `file:line:column`, and vice
        /// versa. Each column defaults to an empty string only when its own lookup fails.
        if (const auto * symbol = symbol_index.findSymbol(addr))
        {
            auto demangled = tryDemangle(symbol->name);
            if (demangled)
                symbols.emplace_back(demangled.get(), strlen(demangled.get()));
            else
                symbols.emplace_back(symbol->name, strlen(symbol->name));
        }
        else
        {
            symbols.emplace_back();
        }

        lines.emplace_back(AddressToLineCache::get(reinterpret_cast<uintptr_t>(addr)));
    }

    return {std::move(symbols), std::move(lines)};
}

}

#endif
