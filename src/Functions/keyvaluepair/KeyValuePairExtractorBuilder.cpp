#include "KeyValuePairExtractorBuilder.h"
#include <Functions/keyvaluepair/impl/state/KeyStateHandler.h>
#include <Functions/keyvaluepair/impl/state/ValueStateHandler.h>
#include <Functions/keyvaluepair/impl//LazyEscapingKeyValuePairExtractor.h>

KeyValuePairExtractorBuilder &KeyValuePairExtractorBuilder::withKeyValuePairDelimiter(char key_value_pair_delimiter_) {
    key_value_pair_delimiter = key_value_pair_delimiter_;
    return *this;
}

KeyValuePairExtractorBuilder &KeyValuePairExtractorBuilder::withEscapeCharacter(char escape_character_) {
    escape_character = escape_character_;
    return *this;
}

KeyValuePairExtractorBuilder &KeyValuePairExtractorBuilder::withItemDelimiter(char item_delimiter_) {
    item_delimiter = item_delimiter_;
    return *this;
}

KeyValuePairExtractorBuilder & KeyValuePairExtractorBuilder::withEnclosingCharacter(std::optional<char> enclosing_character_) {
    enclosing_character = enclosing_character_;
    return *this;
}

KeyValuePairExtractorBuilder &KeyValuePairExtractorBuilder::withValueSpecialCharacterAllowList(std::unordered_set<char> value_special_character_allowlist_) {
    value_special_character_allowlist = std::move(value_special_character_allowlist_);
    return *this;
}

std::shared_ptr<KeyValuePairExtractor> KeyValuePairExtractorBuilder::build() {
    KeyStateHandler key_state_handler(key_value_pair_delimiter, escape_character, enclosing_character);
    ValueStateHandler value_state_handler(escape_character, item_delimiter, enclosing_character, value_special_character_allowlist);
    KeyValuePairEscapingProcessor escaping_processor(escape_character);

    return std::make_shared<LazyEscapingKeyValuePairExtractor>(key_state_handler, value_state_handler, escaping_processor);
}
