#include <Functions/keyvaluepair/impl/KeyValuePairExtractorBuilder.h>

#include <Functions/keyvaluepair/impl/StateHandlerImpl.h>

namespace DB
{

KeyValuePairExtractorBuilder & KeyValuePairExtractorBuilder::withKeyValueDelimiter(char key_value_delimiter_)
{
    key_value_delimiter = key_value_delimiter_;
    return *this;
}

KeyValuePairExtractorBuilder & KeyValuePairExtractorBuilder::withItemDelimiters(std::vector<char> item_delimiters_)
{
    item_delimiters = std::move(item_delimiters_);
    return *this;
}

KeyValuePairExtractorBuilder & KeyValuePairExtractorBuilder::withQuotingCharacter(char quoting_character_)
{
    quoting_character = quoting_character_;
    return *this;
}

KeyValuePairExtractorBuilder & KeyValuePairExtractorBuilder::withMaxNumberOfPairs(uint64_t max_number_of_pairs_)
{
    max_number_of_pairs = max_number_of_pairs_;
    return *this;
}

KeyValuePairExtractorBuilder & KeyValuePairExtractorBuilder::withUnexpectedQuotingCharacterStrategy(
    extractKV::Configuration::UnexpectedQuotingCharacterStrategy unexpected_quoting_character_strategy_)
{
    unexpected_quoting_character_strategy = unexpected_quoting_character_strategy_;
    return *this;
}

}
