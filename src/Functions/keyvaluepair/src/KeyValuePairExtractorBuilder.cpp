#include "KeyValuePairExtractorBuilder.h"

namespace DB
{

KeyValuePairExtractorBuilder & KeyValuePairExtractorBuilder::withKeyValuePairDelimiter(char key_value_pair_delimiter_)
{
    key_value_pair_delimiter = key_value_pair_delimiter_;
    return *this;
}

KeyValuePairExtractorBuilder & KeyValuePairExtractorBuilder::withEscapeCharacter(char escape_character_)
{
    escape_character = escape_character_;
    return *this;
}

KeyValuePairExtractorBuilder & KeyValuePairExtractorBuilder::withItemDelimiter(char item_delimiter_)
{
    item_delimiter = item_delimiter_;
    return *this;
}

KeyValuePairExtractorBuilder & KeyValuePairExtractorBuilder::withEnclosingCharacter(std::optional<char> enclosing_character_)
{
    enclosing_character = enclosing_character_;
    return *this;
}

std::shared_ptr<KeyValuePairExtractor<std::unordered_map<std::string, std::string>>> KeyValuePairExtractorBuilder::build()
{
    InlineEscapingKeyStateHandler key_state_handler(key_value_pair_delimiter, escape_character, enclosing_character);
    InlineEscapingValueStateHandler value_state_handler(escape_character, item_delimiter, enclosing_character);

    return std::make_shared<InlineKeyValuePairExtractor>(key_state_handler, value_state_handler);
}

}
