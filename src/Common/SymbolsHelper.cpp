#if defined(__ELF__) && !defined(OS_FREEBSD)

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

        /// Convert virtual address to file offset if it falls within a loaded object.
        /// Callers may pass either absolute runtime addresses or file offsets.
        const auto * object = symbol_index.findObject(addr);
        const void * offset = addr;

        if (object)
            offset = reinterpret_cast<const void *>(
                reinterpret_cast<uintptr_t>(addr) - reinterpret_cast<uintptr_t>(object->address_begin));

        if (const auto * symbol = symbol_index.findSymbol(offset))
        {
            auto demangled = tryDemangle(symbol->name);
            if (demangled)
                symbols.emplace_back(demangled.get(), strlen(demangled.get()));
            else
                symbols.emplace_back(symbol->name, strlen(symbol->name));

            lines.emplace_back(AddressToLineCache::get(reinterpret_cast<uintptr_t>(offset)));
        }
        else
        {
            symbols.emplace_back();
            lines.emplace_back();
        }
    }

    return {std::move(symbols), std::move(lines)};
}

}

#endif
