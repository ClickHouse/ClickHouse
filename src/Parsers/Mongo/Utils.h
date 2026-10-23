#pragma once

#include <optional>
#include <string>
#include <vector>
#include <string_view>

#include <rapidjson/document.h>

#include <base/types.h>
#include <Common/Exception.h>
#include <Parsers/IAST_fwd.h>

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

std::optional<rapidjson::Value>
findField(const rapidjson::Value & value, const std::string & key, rapidjson::Document::AllocatorType & allocator);

/** Parses the JSON of a request. `max_depth` bounds how deeply its arrays and documents may nest - the
  * parse and the translation that follows it both recurse once per level - and is checked before
  * either of them runs; `0` means no bound, as `max_parser_depth` does.
  */
rapidjson::Value parseData(
    const char * begin,
    const char * end,
    rapidjson::Document::AllocatorType & allocator,
    bool wrap_into_array = true,
    size_t max_depth = 0);


/** The pattern of the matcher below: the column of the field `path` and the columns of every field
  * under it.
  */
std::string fieldSubtreePattern(const std::string & path);

/** `COLUMNS('^<path>(\.|$)')` - the field itself and the fields below it.
  *
  * A nested document of a table that was created in ClickHouse is a set of columns whose names are
  * the dotted paths of its fields, and a read answers with the document those columns are rebuilt
  * into. So a projection of a field is a projection of its whole subtree: `{"profile": 1}` asks for
  * `profile` and for every `profile.<...>` there is, the way Mongo answers with the whole
  * subdocument.
  */
ASTPtr makeFieldSubtreeMatcher(const std::string & path);

/** A condition that holds for every row, attached to the `WHERE` of a select that sorts on `field`,
  * which fails the query when it is analyzed if `field` is a column of arrays. Mongo sorts a document
  * by the smallest element of its array in an ascending sort and by the largest in a descending
  * one, while an `ORDER BY` of an array compares the arrays element by element, which is a
  * different order. The type of a column is not known when the request is translated, and a sort
  * on a column of any other type has to stay a plain `ORDER BY` an index can serve, so the sort on
  * an array is rejected rather than answered in the other order. The type name is a constant, so
  * the condition is folded away when the column is not an array.
  */
ASTPtr makeSortKeyIsNotArrayCondition(const std::string & field);

/// The field a matcher built by `makeFieldSubtreeMatcher` selects the subtree of, if it is one.
std::optional<std::string> fieldOfSubtreeMatcher(const IAST & node);

/** The fields whose subtrees a pattern of `|` joined `fieldSubtreePattern` covers - the pattern an
  * exclusion of fields is built out of - or nothing when the pattern is not one of those.
  */
std::optional<std::vector<std::string>> fieldsOfSubtreePattern(const std::string & pattern);


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
