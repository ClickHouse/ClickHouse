#pragma once

#include "Functions/keyvaluepair/src/impl/state/strategies/noescaping/NoEscapingKeyStateHandler.h"
#include "Functions/keyvaluepair/src/impl/state/strategies/noescaping/NoEscapingValueStateHandler.h"
#include "KeyValuePairExtractor.h"
#include "impl/CHKeyValuePairExtractor.h"

namespace DB
{

class KeyValuePairExtractorBuilder
{
public:

    KeyValuePairExtractorBuilder & withKeyValuePairDelimiter(char key_value_pair_delimiter_);

    KeyValuePairExtractorBuilder & withItemDelimiter(std::vector<char> item_delimiters_);

    KeyValuePairExtractorBuilder & withQuotingCharacters(std::vector<char> quoting_characters_);

    KeyValuePairExtractorBuilder & withEscaping();

    std::shared_ptr<KeyValuePairExtractor> build();

private:
    bool with_escaping = false;
    char key_value_pair_delimiter = ':';
    std::vector<char> item_delimiters = {' ', ',', ';'};
    std::vector<char> quoting_characters = {'"'};

    std::shared_ptr<KeyValuePairExtractor> buildWithEscaping();

    std::shared_ptr<KeyValuePairExtractor> buildWithoutEscaping();
};

}
