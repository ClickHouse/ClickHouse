#include <Functions/keyvaluepair/impl/NeedleFactory.h>

namespace DB
{

namespace extractKV
{

std::vector<char> NeedleFactory::getWaitNeedles(const Configuration & extractor_configuration)
{
    const auto & [key_value_delimiter, quoting_character, pair_delimiters]
        = extractor_configuration;

    std::vector<char> needles;

    needles.reserve(NEEDLE_SIZE);

    needles.push_back(key_value_delimiter);

    std::copy(pair_delimiters.begin(), pair_delimiters.end(), std::back_inserter(needles));

    return needles;
}

std::vector<char> NeedleFactory::getReadNeedles(const Configuration & extractor_configuration)
{
    const auto & [key_value_delimiter, quoting_character, pair_delimiters]
        = extractor_configuration;

    std::vector<char> needles;

    needles.reserve(NEEDLE_SIZE);

    needles.push_back(key_value_delimiter);
    needles.push_back(quoting_character);

    std::copy(pair_delimiters.begin(), pair_delimiters.end(), std::back_inserter(needles));

    return needles;
}

std::vector<char> NeedleFactory::getReadQuotedNeedles(const Configuration & extractor_configuration)
{
    const auto quoting_character = extractor_configuration.quoting_character;

    std::vector<char> needles;

    needles.reserve(NEEDLE_SIZE);

    needles.push_back(quoting_character);

    return needles;
}

std::vector<char> EscapingNeedleFactory::getWaitNeedles(const Configuration & extractor_configuration)
{
    auto needles = NeedleFactory::getWaitNeedles(extractor_configuration);

    needles.push_back('\\');

    return needles;
}

std::vector<char> EscapingNeedleFactory::getReadNeedles(const Configuration & extractor_configuration)
{
    auto needles = NeedleFactory::getReadNeedles(extractor_configuration);

    needles.push_back('\\');

    return needles;
}

std::vector<char> EscapingNeedleFactory::getReadQuotedNeedles(const Configuration & extractor_configuration)
{
    auto needles = NeedleFactory::getReadQuotedNeedles(extractor_configuration);

    needles.push_back('\\');

    return needles;
}

}

}
