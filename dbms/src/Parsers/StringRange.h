#pragma once

#include <Core/Types.h>
#include <Parsers/TokenIterator.h>
#include <map>
#include <memory>
#include <Common/SipHash.h>


namespace DB
{

struct StringRange
{
    const char * first = nullptr;
    const char * second = nullptr;

    StringRange() = default;
    StringRange(const StringRange & other) = default;
    StringRange(const char * begin, const char * end) : first(begin), second(end) {}
    explicit StringRange(TokenIterator token) : first(token->begin), second(token->end) {}

    StringRange(TokenIterator token_begin, TokenIterator token_end)
    {
        /// Empty range.
        if (token_begin == token_end)
        {
            first = token_begin->begin;
            second = token_begin->begin;
            return;
        }

        TokenIterator token_last = token_end;
        --token_last;

        first = token_begin->begin;
        second = token_last->end;
    }
};

using StringPtr = std::shared_ptr<String>;


inline String toString(const StringRange & range)
{
    return range.first ? String(range.first, range.second) : String();
}

/// Hashes only the values of pointers in StringRange. Is used with StringRangePointersEqualTo comparator.
struct StringRangePointersHash
{
    UInt64 operator()(const StringRange & range) const
    {
        SipHash hash;
        hash.update(range.first);
        hash.update(range.second);
        return hash.get64();
    }
};

/// Ranges are equal only when they point to the same memory region.
/// It may be used when it's enough to compare substrings by their position in the same string.
struct StringRangePointersEqualTo
{
    constexpr bool operator()(const StringRange &lhs, const StringRange &rhs) const
    {
        return std::tie(lhs.first, lhs.second) == std::tie(rhs.first, rhs.second);
    }
};

};

