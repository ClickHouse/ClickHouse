#include "KeyValuePairExtractorBuilder.h"
#include "impl/CHKeyValuePairExtractor.h"
#include "impl/state/InlineEscapingKeyStateHandler.h"
#include "impl/state/InlineEscapingValueStateHandler.h"
#include "impl/state/NoEscapingKeyStateHandler.h"
#include "impl/state/NoEscapingValueStateHandler.h"

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

std::shared_ptr<KeyValuePairExtractor> KeyValuePairExtractorBuilder::build()
{
    if (escape_character)
    {
        return buildWithEscaping();
    }

    return buildWithoutEscaping();
}

std::shared_ptr<KeyValuePairExtractor> KeyValuePairExtractorBuilder::buildWithoutEscaping()
{
    using KeyStateHandler = NoEscapingKeyStateHandler<QuotingStrategy::WithQuoting>;
    using ValueStateHandler = NoEscapingValueStateHandler<QuotingStrategy::WithQuoting>;

    CKeyStateHandler auto key_state_handler = KeyStateHandler(
        key_value_pair_delimiter.value_or(':'),
        enclosing_character
    );

    CValueStateHandler auto value_state_handler = ValueStateHandler(
        item_delimiter.value_or(','),
        enclosing_character,
        value_special_character_allowlist
    );

    return std::make_shared<CHKeyValuePairExtractor<KeyStateHandler, ValueStateHandler>>(key_state_handler, value_state_handler);
}

std::shared_ptr<KeyValuePairExtractor> KeyValuePairExtractorBuilder::buildWithEscaping()
{
    using KeyStateHandler = InlineEscapingKeyStateHandler<QuotingStrategy::WithQuoting>;
    using ValueStateHandler = InlineEscapingValueStateHandler<QuotingStrategy::WithQuoting>;

    CKeyStateHandler auto key_state_handler = KeyStateHandler(
        key_value_pair_delimiter.value_or(':'),
        escape_character.value_or('\\'),
        enclosing_character
    );

    CValueStateHandler auto value_state_handler = ValueStateHandler(
        escape_character.value_or('"'),
        item_delimiter.value_or(','),
        enclosing_character,
        value_special_character_allowlist
    );

    return std::make_shared<CHKeyValuePairExtractor<KeyStateHandler, ValueStateHandler>>(key_state_handler, value_state_handler);
}

}
