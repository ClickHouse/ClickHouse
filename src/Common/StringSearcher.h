#pragma once

#include <base/types.h>

#include <algorithm>
#include <cctype>
#include <memory>

namespace DB
{

/** Variants for searching a substring in a string.
  * In most cases, performance is less than Volnitsky (see Volnitsky.h).
  */

namespace impl
{

struct StringSearcherBase
{
    virtual ~StringSearcherBase() = 0;

    virtual bool compare(const UInt8 * haystack, const UInt8 * haystack_end, const UInt8 * pos) const = 0;
    virtual const UInt8 * search(const UInt8 * haystack, const UInt8 * haystack_end) const = 0;

    bool force_fallback = false;
};

/// Performs case-sensitive or case-insensitive search of ASCII or UTF-8 strings
template <bool CaseSensitive, bool ASCII>
struct StringSearcher
{
    StringSearcher(const UInt8 * needle_, size_t needle_size);

    bool compare(const UInt8 * haystack, const UInt8 * haystack_end, const UInt8 * pos) const;
    const UInt8 * search(const UInt8 * haystack, const UInt8 * haystack_end) const;
    const UInt8 * search(const UInt8 * haystack, size_t haystack_size) const;

    bool getForceFallback() const;
    void setForceFallback(bool force_fallback);

private:
    std::unique_ptr<StringSearcherBase> impl;
};

}

extern template struct impl::StringSearcher<true, true>;
extern template struct impl::StringSearcher<false, true>;
extern template struct impl::StringSearcher<true, false>;
extern template struct impl::StringSearcher<false, false>;

using ASCIICaseSensitiveStringSearcher =   impl::StringSearcher<true, true>;
using ASCIICaseInsensitiveStringSearcher = impl::StringSearcher<false, true>;
using UTF8CaseSensitiveStringSearcher =    impl::StringSearcher<true, false>;
using UTF8CaseInsensitiveStringSearcher =  impl::StringSearcher<false, false>;

/// Use only with short haystacks where cheap initialization is required.
template <bool CaseInsensitive>
struct StdLibASCIIStringSearcher
{
    const char * const needle_start;
    const char * const needle_end;

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    StdLibASCIIStringSearcher(const CharT * const needle_start_, size_t needle_size_)
        : needle_start(reinterpret_cast<const char *>(needle_start_))
        , needle_end(reinterpret_cast<const char *>(needle_start) + needle_size_)
    {}

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    const CharT * search(const CharT * haystack_start, const CharT * const haystack_end) const
    {
        if constexpr (CaseInsensitive)
            return std::search(
                haystack_start, haystack_end, needle_start, needle_end,
                [](char c1, char c2) { return std::toupper(c1) == std::toupper(c2); });
        else
            return std::search(
                haystack_start, haystack_end, needle_start, needle_end,
                [](char c1, char c2) { return c1 == c2; });
    }

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    const CharT * search(const CharT * haystack_start, size_t haystack_length) const
    {
        return search(haystack_start, haystack_start + haystack_length);
    }
};

}
