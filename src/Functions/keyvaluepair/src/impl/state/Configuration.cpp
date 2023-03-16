#include "Configuration.h"
#include <unordered_set>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

Configuration::Configuration(char key_value_delimiter_, char quoting_character_, std::vector<char> pair_delimiters_)
    : key_value_delimiter(key_value_delimiter_), quoting_character(quoting_character_), pair_delimiters(std::move(pair_delimiters_)) {}


Configuration ConfigurationFactory::create(char key_value_delimiter, char quoting_character, std::vector<char> pair_delimiters)
{
    validateKeyValueDelimiter(key_value_delimiter);
    validateQuotingCharacter(quoting_character);
    validatePairDelimiters(pair_delimiters);

    return Configuration(key_value_delimiter, quoting_character, pair_delimiters);
}

void ConfigurationFactory::validateKeyValueDelimiter(char key_value_delimiter)
{
    static const std::unordered_set<char> VALID_KV_DELIMITERS = {'=', ':'};

    if (!VALID_KV_DELIMITERS.contains(key_value_delimiter))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid key value delimiter '{}'. It must be one of: '{}'", key_value_delimiter, fmt::join(VALID_KV_DELIMITERS, ", "));
    }
}

void ConfigurationFactory::validateQuotingCharacter(char quoting_character)
{
    static const std::unordered_set<char> VALID_QUOTING_CHARACTERS = {'"', '\''};

    if (!VALID_QUOTING_CHARACTERS.contains(quoting_character))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid quoting character '{}'. It must be one of: '{}'", quoting_character, fmt::join(VALID_QUOTING_CHARACTERS, ", "));
    }
}

void ConfigurationFactory::validatePairDelimiters(const std::vector<char> & pair_delimiters)
{
    static const std::unordered_set<char> VALID_PAIR_DELIMITERS_CHARACTERS = {' ', ',', ';', 0x1};

    for (auto delimiter : pair_delimiters)
    {
        if (!VALID_PAIR_DELIMITERS_CHARACTERS.contains(delimiter))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid pair delimiter '{}'. It must be one of: '{}'", delimiter, fmt::join(VALID_PAIR_DELIMITERS_CHARACTERS, ", "));
        }
    }
}

}

