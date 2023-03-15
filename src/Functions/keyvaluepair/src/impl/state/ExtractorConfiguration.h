#pragma once

#include <unordered_set>
#include <vector>

namespace DB
{

struct ExtractorConfiguration
{
    ExtractorConfiguration(
        char key_value_delimiter_,
        char quoting_character_,
        std::vector<char> pair_delimiters_
    ) : key_value_delimiter(key_value_delimiter_), quoting_character(quoting_character_), pair_delimiters(std::move(pair_delimiters_)) {}

    const char key_value_delimiter;
    const char quoting_character;
    const std::vector<char> pair_delimiters;
};

}
