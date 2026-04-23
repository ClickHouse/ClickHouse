#include <Functions/keyvaluepair/impl/Configuration.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace extractKV
{

Configuration::Configuration(
    char key_value_delimiter_,
    char quoting_character_,
    std::vector<char> pair_delimiters_,
    UnexpectedQuotingCharacterStrategy unexpected_quoting_character_strategy_)
    : key_value_delimiter(key_value_delimiter_),
    quoting_character(quoting_character_),
    pair_delimiters(std::move(pair_delimiters_)),
    unexpected_quoting_character_strategy(unexpected_quoting_character_strategy_)
{
}

Configuration ConfigurationFactory::createWithoutEscaping(
    char key_value_delimiter,
    char quoting_character,
    std::vector<char> pair_delimiters,
    Configuration::UnexpectedQuotingCharacterStrategy unexpected_quoting_character_strategy)
{
    validate(key_value_delimiter, quoting_character, pair_delimiters);

    return Configuration(
        key_value_delimiter,
        quoting_character,
        pair_delimiters,
        unexpected_quoting_character_strategy);
}

Configuration ConfigurationFactory::createWithEscaping(
    char key_value_delimiter,
    char quoting_character,
    std::vector<char> pair_delimiters,
    Configuration::UnexpectedQuotingCharacterStrategy unexpected_quoting_character_strategy)
{
    static constexpr char ESCAPE_CHARACTER = '\\';

    if (key_value_delimiter == ESCAPE_CHARACTER
        || quoting_character == ESCAPE_CHARACTER
        || std::find(pair_delimiters.begin(), pair_delimiters.end(), ESCAPE_CHARACTER) != pair_delimiters.end())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid arguments, {} is reserved for the escaping character",
            ESCAPE_CHARACTER);
    }

    return createWithoutEscaping(
        key_value_delimiter,
        quoting_character,
        pair_delimiters,
        unexpected_quoting_character_strategy);
}

void ConfigurationFactory::validate(char key_value_delimiter, char quoting_character, std::vector<char> pair_delimiters)
{
    if (key_value_delimiter == quoting_character)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid arguments, key_value_delimiter and quoting_character can not be the same");
    }

    if (pair_delimiters.size() > MAX_NUMBER_OF_PAIR_DELIMITERS)
    {
        // SSE optimizations require needles to contain up to 16 characters. Needles can be a concatenation of multiple parameters, including
        // quoting_character, key_value_delimiter and pair delimiters. Limiting to 8 to be on the safe side.
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid arguments, pair delimiters can contain at most {} characters", MAX_NUMBER_OF_PAIR_DELIMITERS);
    }

    if (pair_delimiters.empty())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid arguments, pair delimiters list is empty");
    }

    bool is_key_value_delimiter_in_pair_delimiters
        = std::find(pair_delimiters.begin(), pair_delimiters.end(), key_value_delimiter) != pair_delimiters.end();

    if (is_key_value_delimiter_in_pair_delimiters)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid arguments, key_value_delimiter conflicts with pair delimiters");
    }

    bool is_quoting_character_in_pair_delimiters
        = std::find(pair_delimiters.begin(), pair_delimiters.end(), quoting_character) != pair_delimiters.end();

    if (is_quoting_character_in_pair_delimiters)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid arguments, quoting_character conflicts with pair delimiters");
    }
}

}

}
