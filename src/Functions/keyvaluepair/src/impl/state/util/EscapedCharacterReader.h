#pragma once

#include <unordered_map>
#include <string_view>

namespace DB
{

/*
 * Takes in an escape sequence like: \n \t \xHH \0NNN and return its parsed character
 * Example:
 *      Input: "\n", Output: {0xA}
 *      Input: "\xFF", Output: {0xFF}
 *      Input: "\0377", Output: {0xFF}
 * It can also take non-standard escape sequences, in which case it'll simply return the input.
 * Examples:
 *      Input: "\h", Output {'\', 'h'}
 * */
class EscapedCharacterReader
{
    static const std::unordered_map<char, std::pair<uint8_t, uint8_t>> numeric_escape_sequences_number_of_characters;
    static const std::unordered_map<char, char> escape_sequences;

public:
    static constexpr char ESCAPE_CHARACTER = '\\';
    struct ReadResult
    {
        const char * ptr;
        std::vector<uint8_t> characters;
    };

    static bool isEscapeCharacter(char character);
    static ReadResult read(std::string_view element);
    static ReadResult read(std::string_view element, std::size_t offset);

private:
    static bool isEscapeSequence(char character);

    static std::optional<uint8_t> readEscapedSequence(std::string_view & file, char sequence_identifier);
};

}
