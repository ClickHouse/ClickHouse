#pragma once

#if defined(__ELF__) && !defined(OS_FREEBSD)

#include <utility>
#include <vector>
#include <base/types.h>

namespace DB
{

/// Symbolizes a stack trace (array of frame pointers) into:
///   - first:  demangled function names
///   - second: file:line:column strings
std::pair<std::vector<String>, std::vector<String>>
symbolizeTrace(const void * const * frame_pointers, size_t size);

}

#endif
