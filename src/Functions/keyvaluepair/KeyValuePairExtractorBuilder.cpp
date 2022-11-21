#include "KeyValuePairExtractorBuilder.h"
#include "impl/LazyEscapingKeyValuePairExtractor.h"

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

std::shared_ptr<KeyValuePairExtractor> KeyValuePairExtractorBuilder::build() {
    return std::make_shared<LazyEscapingKeyValuePairExtractor>(item_delimiter, key_value_pair_delimiter, escape_character, enclosing_character);
}
