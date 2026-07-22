#pragma once

#include <vector>

namespace DB
{
namespace extractKV
{
struct ConfigurationFactory;

class Configuration
{
public:
    enum class UnexpectedQuotingCharacterStrategy
    {
        INVALID,
        ACCEPT,
        PROMOTE
    };

    const char key_value_delimiter;
    const char quoting_character;
    const std::vector<char> pair_delimiters;
    const UnexpectedQuotingCharacterStrategy unexpected_quoting_character_strategy;

private:
    friend struct ConfigurationFactory;

    Configuration(
        char key_value_delimiter_,
        char quoting_character_,
        std::vector<char> pair_delimiters_,
        UnexpectedQuotingCharacterStrategy unexpected_quoting_character_strategy_
    );
};

/*
 * Validates (business logic) and creates Configurations for key-value-pair extraction.
 * */
struct ConfigurationFactory
{
public:
    static Configuration createWithoutEscaping(char key_value_delimiter, char quoting_character, std::vector<char> pair_delimiters, Configuration::UnexpectedQuotingCharacterStrategy unexpected_quoting_character_strategy);

    static Configuration createWithEscaping(char key_value_delimiter, char quoting_character, std::vector<char> pair_delimiters, Configuration::UnexpectedQuotingCharacterStrategy unexpected_quoting_character_strategy);

private:
    static void validate(char key_value_delimiter, char quoting_character, std::vector<char> pair_delimiters);

    static constexpr auto MAX_NUMBER_OF_PAIR_DELIMITERS = 8u;
};
}

}
