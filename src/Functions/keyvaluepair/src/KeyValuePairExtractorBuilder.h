#pragma once

#include "KeyValuePairExtractor.h"
#include "impl/CHKeyValuePairExtractor.h"
#include "impl/state/NoEscapingKeyStateHandler.h"
#include "impl/state/NoEscapingValueStateHandler.h"

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

    KeyValuePairExtractorBuilder & withValueSpecialCharacterAllowlist(std::unordered_set<char> special_character_allow_list);

    std::shared_ptr<KeyValuePairExtractor> build();

private:
    std::optional<char> key_value_pair_delimiter;
    std::optional<char> escape_character;
    std::optional<char> item_delimiter;
    std::optional<char> enclosing_character;
    std::unordered_set<char> value_special_character_allowlist;

    std::shared_ptr<KeyValuePairExtractor> buildWithEscaping();

    std::shared_ptr<KeyValuePairExtractor> buildWithoutEscaping();
};

}
