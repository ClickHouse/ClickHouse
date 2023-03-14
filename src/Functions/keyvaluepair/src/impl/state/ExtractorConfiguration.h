#pragma once

#include <unordered_set>
#include <vector>

namespace DB
{

struct ExtractorConfiguration
{
    ExtractorConfiguration(
        char key_value_delimiter_,
        std::vector<char> pair_delimiters_,
        std::vector<char> quoting_characters_
        ) : key_value_delimiter(key_value_delimiter_), pair_delimiters(std::move(pair_delimiters_)), quoting_characters(std::move(quoting_characters_))
    {}

    const char key_value_delimiter;
    const std::vector<char> pair_delimiters;
    const std::vector<char> quoting_characters;
};

}
