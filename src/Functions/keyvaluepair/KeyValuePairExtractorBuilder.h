#pragma once

#include <memory>
#include <optional>
#include <unordered_set>
#include "KeyValuePairExtractor.h"

class KeyValuePairExtractorBuilder {
public:
    KeyValuePairExtractorBuilder & withKeyValuePairDelimiter(char key_value_pair_delimiter);
    KeyValuePairExtractorBuilder & withEscapeCharacter(char escape_character);
    KeyValuePairExtractorBuilder & withItemDelimiter(char item_delimiter);
    KeyValuePairExtractorBuilder & withEnclosingCharacter(std::optional<char> enclosing_character);
    KeyValuePairExtractorBuilder & withValueSpecialCharacterAllowList(std::unordered_set<char> value_special_character_allowlist);

    std::shared_ptr<KeyValuePairExtractor> build();

private:
    char key_value_pair_delimiter = ':';
    char escape_character = '\\';
    char item_delimiter = ',';
    std::optional<char> enclosing_character;
    std::unordered_set<char> value_special_character_allowlist;
};
