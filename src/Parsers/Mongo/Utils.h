#pragma once

#include <optional>
#include <string_view>
#include <vector>

#include <rapidjson/document.h>

#include <base/types.h>
#include <Parsers/IAST_fwd.h>
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

/** Finds the parenthesis that closes the one at `begin`, skipping string literals and nested
  * parentheses, and throws when the input ends before it. `begin` must point at a `(`.
  */
const char * findMatchingParenthesis(const char * begin, const char * end);

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

/** A nested document is stored as one column per leaf, named by its dotted path, so a field path
  * stands for the column of that name and for every column under it: `profile` is `profile`,
  * `profile.name` and `profile.age`, but not `profiles`. Whether `column` is one of the columns
  * `path` stands for.
  */
bool isInFieldSubtree(std::string_view column, std::string_view path);

/** The regular expression of a `COLUMNS` matcher or an `EXCEPT` transformer that matches every
  * column one of `paths` stands for, see `isInFieldSubtree`. A projection, an exclusion or a
  * replacement of a parent path thus covers the whole subtree without knowing the columns of the
  * collection, while a leaf path matches just its own column.
  */
String makeFieldSubtreePattern(const std::vector<String> & paths);

std::optional<rapidjson::Value>
findField(const rapidjson::Value & value, const std::string & key, rapidjson::Document::AllocatorType & allocator);

/** A key of a Mongo sort. Mongo sorts an array by its smallest element in an ascending sort and
  * by its largest one in a descending sort, while ClickHouse compares arrays lexicographically, so
  * a sort on an array is rejected rather than answered in a different order. The rejection is a
  * `throwIf` on the type name of the key, which is a constant, so it fails when the query is
  * analyzed, and otherwise the condition folds away and leaves the key itself.
  */
ASTPtr makeMongoSortKey(ASTPtr key);

/** An element of the `ORDER BY` of a Mongo sort, see `makeMongoSortKey`. Mongo orders `null`
  * below every other value, so it comes first in an ascending sort and last in a descending one.
  * The placement is marked as given explicitly, so that it survives formatting the query as text.
  */
ASTPtr makeMongoOrderByElement(ASTPtr key, int direction);

/** Parses the JSON text of a query. A nesting deeper than `max_depth` - the `max_parser_depth` of
  * the query, zero for no limit - is rejected before anything recursive walks the value.
  */
rapidjson::Value parseData(
    const char * begin, const char * end, rapidjson::Document::AllocatorType & allocator, size_t max_depth, bool wrap_into_array = true);


class MongoQueryKeyNameExtractor
{
public:
    explicit MongoQueryKeyNameExtractor(const std::string & pattern_) : pattern(pattern_) { }

    std::optional<Int64> extractInt(const char * begin, const char * end);

    std::optional<std::string> extractString(const char * begin, const char * end);

private:
    std::optional<size_t> findPosition(const char * begin, const char * end);

    std::string pattern;
};

}

}
