#pragma once

#if (defined(__ELF__) && !defined(OS_FREEBSD)) || defined(OS_DARWIN)

#include <utility>
#include <vector>
#include <base/types.h>

namespace DB
{

/// Symbolizes a stack trace (array of frame pointers) into:
///   - first:  demangled function names (empty vector if `need_symbols` is false)
///   - second: file:line:column strings (empty vector if `need_lines` is false)
/// The two lookups are independent and expensive (demangling vs DWARF), so there are no defaults:
/// callers must decide explicitly which of them they need.
std::pair<std::vector<String>, std::vector<String>>
symbolizeTrace(const void * const * frame_pointers, size_t size, bool need_symbols, bool need_lines);

}

#endif
