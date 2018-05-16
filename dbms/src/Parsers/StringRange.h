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

    bool operator==(const StringRange & rhs) const { return std::tie(first, second) == std::tie(rhs.first, rhs.second); }
};

using StringPtr = std::shared_ptr<String>;


inline String toString(const StringRange & range)
{
    return range.first ? String(range.first, range.second) : String();
}

struct StringRangeHash
{
    UInt64 operator()(const StringRange & range) const
    {
        SipHash hash;
        hash.update(range.first);
        hash.update(range.second);
        return hash.get64();
    }
};

}

