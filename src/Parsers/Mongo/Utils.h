#pragma once

#include <optional>
#include <string_view>

#include <rapidjson/document.h>

#include <base/types.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace Mongo
{

template <char token>
const char * findKth(const char * begin, const char * end, size_t k)
{
    const char * iter = begin;
    for (size_t i = 0; i < k; ++i)
    {
        if (i != 0 && iter != end)
        {
            iter++;
        }
        while (iter < end && iter[0] != token)
        {
            iter++;
        }
        if (iter == end)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid query: there is less than {} tokens {}", k, token);
        }
    }
    return iter;
}

std::pair<const char *, const char *> getMetadataSubstring(const char * begin, const char * end);

std::pair<const char *, const char *> getSettingsSubstring(const char * begin, const char * end);

/** Finds the `;` that terminates the statement starting at `begin`, skipping string literals: a
  * `;` inside a value such as `{"name": "a;b"}` is data, not a terminator. Returns `end` when the
  * statement is the last one of the input and carries no terminator, which is how a single query
  * arrives over `--query` or the wire.
  */
const char * findStatementEnd(const char * begin, const char * end);

/** The scale of the `Decimal128` column that holds a Mongo `$numberDecimal` value exactly, or
  * nothing when no such scale exists. Mongo's `Decimal128` is a 34 digit decimal floating point
  * number with an exponent of its own, so a single fixed scale cannot hold all of them: the scale
  * is derived from the value, and a value whose digits do not fit (or that is not a decimal
  * number at all, such as `NaN`) is rejected by the caller rather than silently rounded.
  * Trailing fractional zeros count: `1.50` keeps scale 2, because to Mongo it is a different
  * member of the cohort of `1.5`.
  */
std::optional<UInt32> decimalScaleOfNumberDecimal(std::string_view text);

/** Copies a value into `allocator`, which must outlive the returned value. The allocator is
  * always the one owned by the `QueryMetadata` of the query being parsed - a process wide
  * allocator would both race between concurrent queries and never release its memory.
  */
template <typename T>
rapidjson::Value copyValue(const T & value, rapidjson::Document::AllocatorType & allocator)
{
    rapidjson::Value result;
    result.CopyFrom(value, allocator);
    return result;
}

std::optional<rapidjson::Value>
findField(const rapidjson::Value & value, const std::string & key, rapidjson::Document::AllocatorType & allocator);

rapidjson::Value
parseData(const char * begin, const char * end, rapidjson::Document::AllocatorType & allocator, bool wrap_into_array = true);


class MongoQueryKeyNameExtractor
{
public:
    explicit MongoQueryKeyNameExtractor(const std::string & pattern_) : pattern(pattern_) { }

    std::optional<int> extractInt(const char * begin, const char * end);

    std::optional<std::string> extractString(const char * begin, const char * end);

private:
    std::optional<size_t> findPosition(const char * begin, const char * end);

    std::string pattern;
};

}

}
