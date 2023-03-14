#include "EscapedCharacterReader.h"

#include <charconv>

namespace DB
{

static std::optional<uint8_t> consumeCharacter(std::string_view & view)
{
    if (view.empty())
        return std::nullopt;

    char character = view[0];

    view = {view.begin() + 1u, view.end()};

    return character;
}

static std::optional<char> toInt(std::string_view & s, int base)
{
    std::size_t value;

    auto conversion_result = std::from_chars(s.begin(), s.end(), value, base);

    s = {conversion_result.ptr, s.end()};

    if (conversion_result.ec == std::errc{})
    {
        return static_cast<uint8_t>(value);
    }

    return std::nullopt;
}

const std::unordered_map<char, std::pair<uint8_t, uint8_t>> EscapedCharacterReader::numeric_escape_sequences_number_of_characters =
    {{'x', std::make_pair(2, 16)}, {'0', std::make_pair(3, 8)}};

const std::unordered_map<char, char> EscapedCharacterReader::escape_sequences = {
    {'\\', '\\'}, {'a', 0x7}, {'b', 0x8}, {'c', 0}, {'e', 0x1B},
    {'f', 0xC}, {'n', 0xA}, {'r', 0xD}, {'t', 0x9}, {'v', 0xB},
};

bool EscapedCharacterReader::isEscapeCharacter(char character)
{
    return character == ESCAPE_CHARACTER;
}

EscapedCharacterReader::ReadResult EscapedCharacterReader::read(std::string_view element, std::size_t offset)
{
    return read({element.begin() + offset, element.end()});
}

EscapedCharacterReader::ReadResult EscapedCharacterReader::read(std::string_view str)
{
    if (auto maybe_escape_character = consumeCharacter(str); isEscapeCharacter(*maybe_escape_character))
    {
        if (auto character = consumeCharacter(str))
        {
            if (isEscapeSequence(*character))
            {
                if (auto read_sequence = readEscapedSequence(str, *character))
                {
                    return {
                        str.begin(),
                        {*read_sequence}
                    };
                }

                return {
                    str.begin(),
                    {}
                };
            }
            else
            {
                return {
                    str.begin(),
                    {'\\', static_cast<uint8_t>(*character)}
                };
            }
        }

    }

    return {
        str.begin(),
        {}
    };
}

bool EscapedCharacterReader::isEscapeSequence(char character)
{
    return escape_sequences.contains(character) || numeric_escape_sequences_number_of_characters.contains(character);
}

std::optional<uint8_t> EscapedCharacterReader::readEscapedSequence(std::string_view & str, char sequence_identifier)
{
    if (numeric_escape_sequences_number_of_characters.contains(sequence_identifier))
    {
        auto [max_number_of_bytes_to_read, base] = numeric_escape_sequences_number_of_characters.at(sequence_identifier);

        auto number_of_bytes_to_read = std::min<std::size_t>(max_number_of_bytes_to_read, str.size());

        auto cut = std::string_view {str.begin(), str.begin() + number_of_bytes_to_read};

        auto read_character = toInt(cut, base);

        str = {cut.begin(), str.end()};

        return read_character;
    }

    return escape_sequences.at(sequence_identifier);
}

}
