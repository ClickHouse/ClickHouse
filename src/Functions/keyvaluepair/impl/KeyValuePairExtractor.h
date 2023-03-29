#pragma once

#include <Columns/ColumnString.h>

#include <string>
#include <string_view>

namespace DB
{
/*
 * Extracts key value pairs from strings. Strings do not need to be key-value pair only,
 * it can contain "noise". The below grammar is a simplified representation of what is expected/ supported:
 *
 * line = (reserved_char* key_value_pair)*  reserved_char*
 * key_value_pair = key kv_separator value
 * key = <quoted_string> |  asciichar asciialphanumeric*
 * kv_separator = ':'
 * value = <quoted_string> | asciialphanum*
 * item_delimiter = ','
 *
 * Both key and values accepts underscores as well. Special characters must be escaped.
 * Control characters (key_value_pair_separator, item_delimiter, escape_character and enclosing_character) are customizable
 *
 * The return type is templated and defaults to std::unordered_map<std::string, std::string>. By design, the KeyValuePairExtractor
 * should extract key value pairs and return them properly escaped (in order to escape, strings are necessary. string_views cannot be used).
 * The built-in SimpleKeyValuePairEscapingProcessor implements a very simple and non optimized escaping algorithm. For clients that need
 * better performance, this abstraction allows custom escaping processors to be injected.
 *
 * ClickHouse injects a NoOp escaping processor that returns an unescaped std::unordered_map<std::string_view, std::string_view>. This avoids
 * unnecessary copies and allows escaping to do be done on client side. At the same time, the KeyValuePairExtractor class can be unit tested
 * in a standalone manner by using the SimpleKeyValuePairEscapingProcessor for escaping.
 *
 * If we want to simplify this in the future, approach #2 in https://github.com/ClickHouse/ClickHouse/pull/43606#discussion_r1049541759 seems
 * to be the best bet.
 * */
struct KeyValuePairExtractor
{
    virtual ~KeyValuePairExtractor() = default;

    virtual uint64_t extract(const std::string & data, ColumnString::MutablePtr & keys, ColumnString::MutablePtr & values) = 0;

    virtual uint64_t extract(std::string_view data, ColumnString::MutablePtr & keys, ColumnString::MutablePtr & values) = 0;
};

}
