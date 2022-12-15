#pragma once

#include <string>
#include <unordered_map>

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
 * */
template <typename R = std::unordered_map<std::string, std::string>>
struct KeyValuePairExtractor
{
    using Response = R;

    virtual ~KeyValuePairExtractor() = default;

    virtual Response extract(const std::string & data) = 0;
};

}
