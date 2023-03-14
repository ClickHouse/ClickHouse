#include "KeyValuePairExtractorBuilder.h"
#include "Functions/keyvaluepair/src/impl/state/strategies/escaping/InlineEscapingKeyStateHandler.h"
#include "Functions/keyvaluepair/src/impl/state/strategies/escaping/InlineEscapingValueStateHandler.h"
#include "Functions/keyvaluepair/src/impl/state/strategies/noescaping/NoEscapingKeyStateHandler.h"
#include "Functions/keyvaluepair/src/impl/state/strategies/noescaping/NoEscapingValueStateHandler.h"
#include "impl/CHKeyValuePairExtractor.h"
#include "impl/state/ExtractorConfiguration.h"

namespace DB
{

KeyValuePairExtractorBuilder & KeyValuePairExtractorBuilder::withKeyValuePairDelimiter(char key_value_pair_delimiter_)
{
    key_value_pair_delimiter = key_value_pair_delimiter_;
    return *this;
}

KeyValuePairExtractorBuilder & KeyValuePairExtractorBuilder::withItemDelimiter(std::vector<char> item_delimiters_)
{
    item_delimiters = std::move(item_delimiters_);
    return *this;
}

KeyValuePairExtractorBuilder & KeyValuePairExtractorBuilder::withQuotingCharacters(std::vector<char> quoting_characters_)
{
    quoting_characters = std::move(quoting_characters_);
    return *this;
}

KeyValuePairExtractorBuilder & KeyValuePairExtractorBuilder::withEscaping()
{
    with_escaping = true;
    return *this;
}

std::shared_ptr<KeyValuePairExtractor> KeyValuePairExtractorBuilder::build()
{
    if (with_escaping)
    {
        return buildWithEscaping();
    }

    return buildWithoutEscaping();
}

std::shared_ptr<KeyValuePairExtractor> KeyValuePairExtractorBuilder::buildWithoutEscaping()
{
    ExtractorConfiguration configuration(
        key_value_pair_delimiter,
        item_delimiters,
        quoting_characters
    );

    CKeyStateHandler auto key_state_handler = NoEscapingKeyStateHandler(
        configuration
    );

    CValueStateHandler auto value_state_handler = NoEscapingValueStateHandler(
        configuration
    );

    return std::make_shared<CHKeyValuePairExtractor<NoEscapingKeyStateHandler, NoEscapingValueStateHandler>>(key_state_handler, value_state_handler);
}

std::shared_ptr<KeyValuePairExtractor> KeyValuePairExtractorBuilder::buildWithEscaping()
{
    ExtractorConfiguration configuration(
        key_value_pair_delimiter,
        item_delimiters,
        quoting_characters
    );

    CKeyStateHandler auto key_state_handler = InlineEscapingKeyStateHandler(configuration);

    CValueStateHandler auto value_state_handler = InlineEscapingValueStateHandler(configuration);

    return std::make_shared<CHKeyValuePairExtractor<decltype(key_state_handler), InlineEscapingValueStateHandler>>(key_state_handler, value_state_handler);
}

}
