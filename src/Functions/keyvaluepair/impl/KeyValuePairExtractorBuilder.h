#pragma once

#include <vector>
#include <Functions/keyvaluepair/impl/CHKeyValuePairExtractor.h>
#include <Functions/keyvaluepair/impl/Configuration.h>
#include <Functions/keyvaluepair/impl/StateHandlerImpl.h>

namespace DB
{

struct KeyValuePairExtractor;

class KeyValuePairExtractorBuilder
{
public:

    KeyValuePairExtractorBuilder & withKeyValueDelimiter(char key_value_delimiter_);

    KeyValuePairExtractorBuilder & withItemDelimiters(std::vector<char> item_delimiters_);

    KeyValuePairExtractorBuilder & withQuotingCharacter(char quoting_character_);

    KeyValuePairExtractorBuilder & withMaxNumberOfPairs(uint64_t max_number_of_pairs_);

    auto buildWithoutEscaping() const
    {
        auto configuration = extractKV::ConfigurationFactory::createWithoutEscaping(key_value_delimiter, quoting_character, item_delimiters);

        return KeyValuePairExtractorNoEscaping(configuration, max_number_of_pairs);
    }

    auto buildWithEscaping() const
    {
        auto configuration = extractKV::ConfigurationFactory::createWithEscaping(key_value_delimiter, quoting_character, item_delimiters);

        return KeyValuePairExtractorInlineEscaping(configuration, max_number_of_pairs);
    }

    auto buildWithReferenceMap() const
    {
        auto configuration = extractKV::ConfigurationFactory::createWithoutEscaping(key_value_delimiter, quoting_character, item_delimiters);

        return KeyValuePairExtractorReferenceMap(configuration, max_number_of_pairs);
    }

private:
    char key_value_delimiter = ':';
    char quoting_character = '"';
    std::vector<char> item_delimiters = {' ', ',', ';'};
    uint64_t max_number_of_pairs = std::numeric_limits<uint64_t>::max();
};

}
