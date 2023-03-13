//#pragma once
//
//#include "StateHandler.h"
//
//#include <optional>
//
//#include "util/EscapedCharacterReader.h"
//
//namespace DB
//{
//
//enum class EscapingStrategy
//{
//    WITH_ESCAPING,
//    WITHOUT_ESCAPING
//};
//
//template <EscapingStrategy ESCAPING_STRATEGY>
//class MixedKeyStateHandler : public StateHandler
//{
//public:
//    using ElementType = typename std::conditional<EscapingStrategy::WITH_ESCAPING == ESCAPING_STRATEGY, std::string, std::string_view>::type;
//    MixedKeyStateHandler(char key_value_delimiter_, std::optional<char> enclosing_character_)
//        : StateHandler(enclosing_character_), key_value_delimiter(key_value_delimiter_) {}
//
//    [[nodiscard]] NextState wait(std::string_view file, size_t pos) const
//    {
//        while (pos < file.size())
//        {
//            const auto current_character = file[pos];
//
//            if (isValidCharacter(current_character))
//            {
//                return {pos, State::READING_KEY};
//            }
//            else if (enclosing_character && current_character == enclosing_character)
//            {
//                return {pos + 1u, State::READING_ENCLOSED_KEY};
//            }
//
//            pos++;
//        }
//
//        return {pos, State::END};
//    }
//
//    [[nodiscard]] NextState read(std::string_view file, size_t pos, ElementType & key) const
//    {
//        bool escape = false;
//
//        auto start_index = pos;
//
//        key = {};
//
//        while (pos < file.size())
//        {
//            const auto current_character = file[pos];
//            const auto next_pos = pos + 1u;
//
//            if constexpr (EscapingStrategy::WITH_ESCAPING == ESCAPING_STRATEGY)
//            {
//                if (escape)
//                {
//                    escape = false;
//
////                    if (auto escaped_character = EscapedCharacterReader::read({file.begin() + pos, file.end()}))
////                    {
////                        collect(key, *escaped_character);
////                        pos = next_pos;
////                        continue;
////                    }
////                    else
//                    {
//                        // Discard in case of failures. It can fail either on converting characters into a number (\xHEX \0OCTAL)
//                        // or if there isn't enough characters left in the string
//                        return {next_pos, State::WAITING_KEY };
//                    }
//                }
//                else if (EscapedCharacterReader::isEscapeCharacter(current_character))
//                {
//                    escape = true;
//                    pos = next_pos;
//                    continue;
//                }
//            }
//
//            if (current_character == key_value_delimiter)
//            {
//                finalizeElement(key, file, start_index, pos - 1);
//                return {next_pos, State::WAITING_VALUE};
//            }
//            else if (!isValidCharacter(current_character))
//            {
//                return {next_pos, State::WAITING_KEY};
//            }
//            else
//            {
//                collect(key, current_character);
//            }
//
//            pos = next_pos;
//        }
//
//        return {pos, State::END};
//    }
//
//    [[nodiscard]] NextState readEnclosed(std::string_view file, size_t pos, ElementType & key) const
//    {
//        key = {};
//
//        bool escape = false;
//
//        auto start_index = pos;
//
//        while (pos < file.size())
//        {
//            const auto current_character = file[pos];
//            const auto next_pos = pos + 1u;
//
//            if constexpr (EscapingStrategy::WITH_ESCAPING == ESCAPING_STRATEGY)
//            {
//                if (escape)
//                {
//                    escape = false;
//
////                    if (auto escaped_character = EscapedCharacterReader::read({file.begin() + pos, file.end()}))
////                    {
////                        collect(key, *escaped_character);
////                        pos = next_pos;
////                        continue;
////                    }
////                    else
//                    {
//                        // Discard in case of failures. It can fail either on converting characters into a number (\xHEX \0OCTAL)
//                        // or if there isn't enough characters left in the string
//                        return {next_pos, State::WAITING_KEY };
//                    }
//                }
//                else if (EscapedCharacterReader::isEscapeCharacter(current_character))
//                {
//                    escape = true;
//                    pos = next_pos;
//                    continue;
//                }
//            }
//
//            if (*enclosing_character == current_character)
//            {
//                auto is_key_empty = start_index == pos;
//
//                if (is_key_empty)
//                {
//                    return {next_pos, State::WAITING_KEY};
//                }
//
//                finalizeElement(key, file, start_index, pos - 1);
//
//                return {next_pos, State::READING_KV_DELIMITER};
//            }
//            else
//            {
//                collect(key, current_character);
//            }
//
//            pos = next_pos;
//        }
//
//        return {pos, State::END};
//    }
//
//    [[nodiscard]] NextState readKeyValueDelimiter(std::string_view file, size_t pos) const
//    {
//        if (pos == file.size())
//        {
//            return {pos, State::END};
//        }
//        else
//        {
//            const auto current_character = file[pos++];
//            return {pos, current_character == key_value_delimiter ? State::WAITING_VALUE : State::WAITING_KEY};
//        }
//    }
//
//private:
//    const char key_value_delimiter;
//
//    void collect(ElementType & element, char character) const
//    {
//        if constexpr (ESCAPING_STRATEGY == EscapingStrategy::WITH_ESCAPING)
//        {
//            element.push_back(character);
//        }
//    }
//
//    void finalizeElement(ElementType & element, std::string_view file, std::size_t start_index, std::size_t end_index) const
//    {
//        if constexpr (ESCAPING_STRATEGY != EscapingStrategy::WITH_ESCAPING)
//        {
//            element = createElement(file, start_index, end_index);
//        }
//    }
//
//    static bool isValidCharacter(char character)
//    {
//        return std::isalnum(character) || character == '_';
//    }
//};
//
//}
