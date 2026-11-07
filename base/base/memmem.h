#pragma once

#include <cstddef>
#include <cstring>

#if defined(OS_WINDOWS)

#include <vector>

/// `memmem` is a GNU extension: `strstr` for buffers that may contain nul bytes. The Windows CRT
/// does not have it.
///
/// Most callers look for a short needle - a URL parameter name, a `splitByString` separator - in
/// which case the constant factor of a scan-and-compare beats the setup cost of building a shift
/// table, and glibc's own implementation does the same below its threshold. The needle of
/// `splitByString` is an arbitrary constant string, though, and the scan-and-compare is quadratic
/// for a long needle that nearly matches everywhere, so long needles go through Knuth-Morris-Pratt,
/// which is linear in the total length.
inline const void * memmem(const void * haystack, size_t haystack_length, const void * needle, size_t needle_length)
{
    if (needle_length == 0)
        return haystack;
    if (needle_length > haystack_length)
        return nullptr;

    const auto * begin = static_cast<const char *>(haystack);
    const auto * end = begin + haystack_length;
    const auto * needle_begin = static_cast<const char *>(needle);

    static constexpr size_t max_short_needle_length = 32;
    if (needle_length > max_short_needle_length)
    {
        /// failure[i] is the length of the longest proper prefix of the needle that is also a
        /// suffix of its first i + 1 bytes.
        std::vector<size_t> failure(needle_length);
        for (size_t i = 1, matched = 0; i < needle_length; ++i)
        {
            while (matched && needle_begin[i] != needle_begin[matched])
                matched = failure[matched - 1];
            if (needle_begin[i] == needle_begin[matched])
                ++matched;
            failure[i] = matched;
        }

        size_t matched = 0;
        for (const char * pos = begin; pos < end; ++pos)
        {
            while (matched && *pos != needle_begin[matched])
                matched = failure[matched - 1];
            if (*pos == needle_begin[matched])
                ++matched;
            if (matched == needle_length)
                return pos + 1 - needle_length;
        }

        return nullptr;
    }

    const auto * last = end - needle_length;
    const char first = *needle_begin;

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
