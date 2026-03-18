#pragma once

#include <Functions/keyvaluepair/impl/Configuration.h>
#include <base/find_symbols.h>

#include <iterator>
#include <vector>

namespace DB
{

namespace extractKV
{

/*
 * `StateHandlerImpl` makes use of string search algorithms to find delimiters. This class creates the needles for each state
 *  based on the contents of `Configuration`.
 * */
template <bool WITH_ESCAPING>
class NeedleFactory
{
public:
    SearchSymbols getWaitNeedles(const Configuration & extractor_configuration)
    {
        const auto & [key_value_delimiter, quoting_character, pair_delimiters]
            = extractor_configuration;

        std::vector<char> needles;

        needles.push_back(key_value_delimiter);

        std::copy(pair_delimiters.begin(), pair_delimiters.end(), std::back_inserter(needles));

        if constexpr (WITH_ESCAPING)
        {
            needles.push_back('\\');
        }

        return SearchSymbols {std::string{needles.data(), needles.size()}};
    }

    SearchSymbols getReadKeyNeedles(const Configuration & extractor_configuration)
    {
        const auto & [key_value_delimiter, quoting_character, pair_delimiters]
            = extractor_configuration;

        std::vector<char> needles;

        needles.push_back(key_value_delimiter);
        needles.push_back(quoting_character);

        std::copy(pair_delimiters.begin(), pair_delimiters.end(), std::back_inserter(needles));

        if constexpr (WITH_ESCAPING)
        {
            needles.push_back('\\');
        }

        return SearchSymbols {std::string{needles.data(), needles.size()}};
    }

    SearchSymbols getReadValueNeedles(const Configuration & extractor_configuration)
    {
        const auto & [key_value_delimiter, quoting_character, pair_delimiters]
            = extractor_configuration;

        std::vector<char> needles;

        needles.push_back(quoting_character);

        std::copy(pair_delimiters.begin(), pair_delimiters.end(), std::back_inserter(needles));

        if constexpr (WITH_ESCAPING)
        {
            needles.push_back('\\');
        }

        return SearchSymbols {std::string{needles.data(), needles.size()}};
    }

    SearchSymbols getReadQuotedNeedles(const Configuration & extractor_configuration)
    {
        const auto quoting_character = extractor_configuration.quoting_character;

        std::vector<char> needles;

        needles.push_back(quoting_character);

        if constexpr (WITH_ESCAPING)
        {
            needles.push_back('\\');
        }

        return SearchSymbols {std::string{needles.data(), needles.size()}};
    }
};

}

}
