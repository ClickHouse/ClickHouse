#pragma once

#include <memory>
#include <vector>

namespace DB
{

struct KeyValuePairExtractor;

class KeyValuePairExtractorBuilder
{
public:

    KeyValuePairExtractorBuilder & withKeyValueDelimiter(char key_value_delimiter_);

    KeyValuePairExtractorBuilder & withItemDelimiters(std::vector<char> item_delimiters_);

    KeyValuePairExtractorBuilder & withQuotingCharacter(char quoting_character_);

    KeyValuePairExtractorBuilder & withEscaping();

    KeyValuePairExtractorBuilder & withMaxNumberOfPairs(uint64_t max_number_of_pairs_);

    std::shared_ptr<KeyValuePairExtractor> build() const;

private:
    bool with_escaping = false;
    char key_value_delimiter = ':';
    char quoting_character = '"';
    std::vector<char> item_delimiters = {' ', ',', ';'};
    uint64_t max_number_of_pairs = std::numeric_limits<uint64_t>::max();

    std::shared_ptr<KeyValuePairExtractor> buildWithEscaping() const;

    std::shared_ptr<KeyValuePairExtractor> buildWithoutEscaping() const;
};

}
