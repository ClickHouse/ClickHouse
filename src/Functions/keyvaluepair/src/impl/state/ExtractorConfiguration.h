#pragma once

#include <unordered_set>
#include <vector>

namespace DB
{

struct ExtractorConfiguration
{
    ExtractorConfiguration(
        char key_value_delimiter_,
        std::unordered_set<char> pair_delimiters_,
        std::unordered_set<char> quoting_characters_
        ) : key_value_delimiter(key_value_delimiter_), pair_delimiters(std::move(pair_delimiters_)), quoting_characters(std::move(quoting_characters_))
    {}

    const char key_value_delimiter;
    const std::unordered_set<char> pair_delimiters;
    const std::unordered_set<char> quoting_characters;
};

}
