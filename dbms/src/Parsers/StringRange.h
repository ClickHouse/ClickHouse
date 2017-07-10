#pragma once

#include <Core/Types.h>
#include <Parsers/TokenIterator.h>
#include <map>
#include <memory>


namespace DB
{

struct StringRange
{
    const char * first;
    const char * second;

    StringRange() {}
    StringRange(const char * begin, const char * end) : first(begin), second(end) {}
    StringRange(TokenIterator token_begin, TokenIterator token_end) : first(token_begin->begin), second(token_end->begin) {}
};

using StringPtr = std::shared_ptr<String>;


inline String toString(const StringRange & range)
{
    return String(range.first, range.second);
}

}
