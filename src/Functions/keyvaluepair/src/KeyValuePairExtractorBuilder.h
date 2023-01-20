#pragma once

#include <unordered_set>
#include "KeyValuePairExtractor.h"
#include <Functions/keyvaluepair/src/impl/InlineKeyValuePairExtractor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class KeyValuePairExtractorBuilder
{
public:

    KeyValuePairExtractorBuilder & withKeyValuePairDelimiter(char key_value_pair_delimiter_);

    KeyValuePairExtractorBuilder & withEscapeCharacter(char escape_character_);

    KeyValuePairExtractorBuilder & withItemDelimiter(char item_delimiter_);

    KeyValuePairExtractorBuilder & withEnclosingCharacter(std::optional<char> enclosing_character_);

    std::shared_ptr<KeyValuePairExtractor<std::unordered_map<std::string, std::string>>> build();

private:
    char key_value_pair_delimiter = ':';
    char escape_character = '\\';
    char item_delimiter = ',';
    std::optional<char> enclosing_character;
    std::unordered_set<char> value_special_character_allowlist;
};

}
