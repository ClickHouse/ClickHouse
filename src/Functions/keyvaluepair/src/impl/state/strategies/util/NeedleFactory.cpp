#include "NeedleFactory.h"
#include "EscapedCharacterReader.h"

namespace DB
{

std::vector<char> NeedleFactory::getWaitNeedles(const DB::ExtractorConfiguration & extractor_configuration)
{
    const auto & [key_value_delimiter, pair_delimiters, quoting_characters]
        = extractor_configuration;

    std::vector<char> needles;

    needles.reserve(16u);

    needles.push_back(key_value_delimiter);

    std::copy(pair_delimiters.begin(), pair_delimiters.end(), std::back_inserter(needles));

    return needles;
}

std::vector<char> NeedleFactory::getReadNeedles(const ExtractorConfiguration & extractor_configuration)
{
    const auto & [key_value_delimiter, pair_delimiters, quoting_characters]
        = extractor_configuration;

    std::vector<char> needles;

    needles.reserve(16u);

    needles.push_back(key_value_delimiter);

    std::copy(quoting_characters.begin(), quoting_characters.end(), std::back_inserter(needles));
    std::copy(pair_delimiters.begin(), pair_delimiters.end(), std::back_inserter(needles));

    return needles;
}

std::vector<char> NeedleFactory::getReadQuotedNeedles(const ExtractorConfiguration & extractor_configuration)
{
    const auto & quoting_characters = extractor_configuration.quoting_characters;

    std::vector<char> needles;

    needles.reserve(16u);

    std::copy(quoting_characters.begin(), quoting_characters.end(), std::back_inserter(needles));

    return needles;
}

std::vector<char> EscapingNeedleFactory::getWaitNeedles(const DB::ExtractorConfiguration & extractor_configuration)
{
    auto needles = NeedleFactory::getWaitNeedles(extractor_configuration);

    needles.push_back(EscapedCharacterReader::ESCAPE_CHARACTER);

    return needles;
}

std::vector<char> EscapingNeedleFactory::getReadNeedles(const ExtractorConfiguration & extractor_configuration)
{
    auto needles = NeedleFactory::getReadNeedles(extractor_configuration);

    needles.push_back(EscapedCharacterReader::ESCAPE_CHARACTER);

    return needles;
}

std::vector<char> EscapingNeedleFactory::getReadQuotedNeedles(const ExtractorConfiguration & extractor_configuration)
{
    auto needles = NeedleFactory::getReadQuotedNeedles(extractor_configuration);

    needles.push_back(EscapedCharacterReader::ESCAPE_CHARACTER);

    return needles;
}

}
