#pragma once

#include <vector>

namespace DB
{

struct ConfigurationFactory;

class Configuration
{
    friend struct ConfigurationFactory;

    Configuration(
        char key_value_delimiter_,
        char quoting_character_,
        std::vector<char> pair_delimiters_
    );

public:
    const char key_value_delimiter;
    const char quoting_character;
    const std::vector<char> pair_delimiters;
};

struct ConfigurationFactory
{
public:
    static Configuration create(char key_value_delimiter, char quoting_character, std::vector<char> pair_delimiters);

private:
    static void validateKeyValueDelimiter(char key_value_delimiter);
    static void validateQuotingCharacter(char quoting_character);
    static void validatePairDelimiters(const std::vector<char> & pair_delimiters);
};

}
