#pragma once

#include "KeyValuePairExtractor.h"
#include "impl/CHKeyValuePairExtractor.h"
#include "impl/state/NoEscapingKeyStateHandler.h"
#include "impl/state/NoEscapingValueStateHandler.h"

namespace DB
{

class KeyValuePairExtractorBuilder
{
public:

    KeyValuePairExtractorBuilder & withKeyValuePairDelimiter(char key_value_pair_delimiter_);

    KeyValuePairExtractorBuilder & withItemDelimiter(char item_delimiter_);

    KeyValuePairExtractorBuilder & withEnclosingCharacter(std::optional<char> enclosing_character_);

    KeyValuePairExtractorBuilder & withValueSpecialCharacterAllowlist(std::unordered_set<char> special_character_allow_list);

    KeyValuePairExtractorBuilder & withEscaping();

    std::shared_ptr<KeyValuePairExtractor> build();

private:
    bool with_escaping = false;
    std::optional<char> key_value_pair_delimiter;
    std::optional<char> item_delimiter;
    std::optional<char> enclosing_character;
    std::unordered_set<char> value_special_character_allowlist;

    std::shared_ptr<KeyValuePairExtractor> buildWithEscaping();

    std::shared_ptr<KeyValuePairExtractor> buildWithoutEscaping();
};

}
