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
};

}
