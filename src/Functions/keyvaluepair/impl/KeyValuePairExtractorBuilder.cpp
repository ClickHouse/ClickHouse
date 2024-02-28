#include <Functions/keyvaluepair/impl/KeyValuePairExtractorBuilder.h>

#include <Functions/keyvaluepair/impl/CHKeyValuePairExtractor.h>
#include <Functions/keyvaluepair/impl/Configuration.h>
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

KeyValuePairExtractorBuilder & KeyValuePairExtractorBuilder::withEscaping()
{
    with_escaping = true;
    return *this;
}

KeyValuePairExtractorBuilder & KeyValuePairExtractorBuilder::withMaxNumberOfPairs(uint64_t max_number_of_pairs_)
{
    max_number_of_pairs = max_number_of_pairs_;
    return *this;
}

std::shared_ptr<KeyValuePairExtractor> KeyValuePairExtractorBuilder::build() const
{
    if (with_escaping)
    {
        return buildWithEscaping();
    }

    return buildWithoutEscaping();
}

namespace
{
using namespace extractKV;

template <typename T>
auto makeStateHandler(const T && handler, uint64_t max_number_of_pairs)
{
    return std::make_shared<CHKeyValuePairExtractor<T>>(handler, max_number_of_pairs);
}

}

std::shared_ptr<KeyValuePairExtractor> KeyValuePairExtractorBuilder::buildWithoutEscaping() const
{
    auto configuration = ConfigurationFactory::createWithoutEscaping(key_value_delimiter, quoting_character, item_delimiters);

    return makeStateHandler(NoEscapingStateHandler(configuration), max_number_of_pairs);
}

std::shared_ptr<KeyValuePairExtractor> KeyValuePairExtractorBuilder::buildWithEscaping() const
{
    auto configuration = ConfigurationFactory::createWithEscaping(key_value_delimiter, quoting_character, item_delimiters);

    return makeStateHandler(InlineEscapingStateHandler(configuration), max_number_of_pairs);
}

}
