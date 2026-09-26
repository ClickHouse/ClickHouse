#if (defined(__ELF__) && !defined(OS_FREEBSD)) || defined(OS_DARWIN)

#include <Common/SymbolsHelper.h>
#include <Common/AddressToLineCache.h>
#include <Common/SymbolIndex.h>
#include <base/demangle.h>

#include <cstring>

namespace DB
{

std::pair<std::vector<String>, std::vector<String>>
symbolizeTrace(const void * const * frame_pointers, size_t size, bool need_symbols, bool need_lines)
{
    std::vector<String> symbols;
    std::vector<String> lines;
    if (need_symbols)
        symbols.reserve(size);
    if (need_lines)
        lines.reserve(size);

    const SymbolIndex & symbol_index = SymbolIndex::instance();
    for (size_t i = 0; i < size; ++i)
    {
        const void * addr = frame_pointers[i];

        /// The symbol name (from the symbol table) and the source location (from DWARF debug info)
        /// are looked up independently: DWARF line resolution does not depend on the symbol table.
        /// So an address with no matching symbol can still have a valid `file:line:column`, and vice
        /// versa. Each column defaults to an empty string only when its own lookup fails.
        if (need_symbols)
        {
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
        }

        if (need_lines)
        {
            /// For non-innermost frames the address is a return address, i.e. it points to the
            /// instruction after the `call`. Subtract 1 so DWARF resolves the `call` itself instead
            /// of the next source line (mirrors the adjustment in `StackTrace::forEachFrame`).
            /// The symbol lookup above intentionally uses the unadjusted address, as `StackTrace` does.
            uintptr_t line_addr = reinterpret_cast<uintptr_t>(addr) - (i > 0 ? 1 : 0);
            lines.emplace_back(AddressToLineCache::get(line_addr));
        }
    }

    return {std::move(symbols), std::move(lines)};
}

}

#endif
