#pragma once

#include <cstddef>
#include <cstring>

#if defined(OS_WINDOWS)

/// `memmem` is a GNU extension: `strstr` for buffers that may contain nul bytes. The Windows CRT
/// does not have it.
///
/// This is the naive two-loop search rather than anything cleverer. Every caller here looks for a
/// short needle - a URL parameter name, a `splitByString` separator - in which case the constant
/// factor of a scan-and-compare beats the setup cost of building a shift table, and glibc's own
/// implementation does the same below its threshold.
inline const void * memmem(const void * haystack, size_t haystack_length, const void * needle, size_t needle_length)
{
    if (needle_length == 0)
        return haystack;
    if (needle_length > haystack_length)
        return nullptr;

    const auto * begin = static_cast<const char *>(haystack);
    const auto * last = begin + haystack_length - needle_length;
    const char first = *static_cast<const char *>(needle);

    for (const char * pos = begin; pos <= last; ++pos)
    {
        pos = static_cast<const char *>(std::memchr(pos, first, static_cast<size_t>(last - pos) + 1));
        if (!pos)
            return nullptr;
        if (0 == std::memcmp(pos, needle, needle_length))
            return pos;
    }

    return nullptr;
}

#endif
