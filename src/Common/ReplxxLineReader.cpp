#include <Common/ReplxxLineReader.h>
#include <Common/UTF8Helpers.h>

#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <functional>

namespace
{

/// Trim ending whitespace inplace
void trim(String & s)
{
    s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) { return !std::isspace(ch); }).base(), s.end());
}


void highlightCallback(const String & query, std::vector<replxx::Replxx::Color> & colors)
{
    using namespace DB;
    using namespace replxx;

    static const std::unordered_map<TokenType, Replxx::Color> token_to_color =
    {
        { TokenType::Whitespace, Replxx::Color::DEFAULT },
        { TokenType::Comment, Replxx::Color::GRAY },
        { TokenType::BareWord, Replxx::Color::WHITE },
        { TokenType::Number, Replxx::Color::BRIGHTGREEN },
        { TokenType::StringLiteral, Replxx::Color::BRIGHTCYAN },
        { TokenType::QuotedIdentifier, Replxx::Color::BRIGHTMAGENTA },
        { TokenType::OpeningRoundBracket, Replxx::Color::LIGHTGRAY },
        { TokenType::ClosingRoundBracket, Replxx::Color::LIGHTGRAY },
        { TokenType::OpeningSquareBracket, Replxx::Color::BROWN },
        { TokenType::ClosingSquareBracket, Replxx::Color::BROWN },
        { TokenType::OpeningCurlyBrace, Replxx::Color::WHITE },
        { TokenType::ClosingCurlyBrace, Replxx::Color::WHITE },

        { TokenType::Comma, Replxx::Color::YELLOW },
        { TokenType::Semicolon, Replxx::Color::YELLOW },
        { TokenType::Dot, Replxx::Color::YELLOW },
        { TokenType::Asterisk, Replxx::Color::YELLOW },
        { TokenType::Plus, Replxx::Color::YELLOW },
        { TokenType::Minus, Replxx::Color::YELLOW },
        { TokenType::Slash, Replxx::Color::YELLOW },
        { TokenType::Percent, Replxx::Color::YELLOW },
        { TokenType::Arrow, Replxx::Color::YELLOW },
        { TokenType::QuestionMark, Replxx::Color::YELLOW },
        { TokenType::Colon, Replxx::Color::YELLOW },
        { TokenType::Equals, Replxx::Color::YELLOW },
        { TokenType::NotEquals, Replxx::Color::YELLOW },
        { TokenType::Less, Replxx::Color::YELLOW },
        { TokenType::Greater, Replxx::Color::YELLOW },
        { TokenType::LessOrEquals, Replxx::Color::YELLOW },
        { TokenType::GreaterOrEquals, Replxx::Color::YELLOW },
        { TokenType::Concatenation, Replxx::Color::YELLOW },
        { TokenType::At, Replxx::Color::YELLOW },

        { TokenType::EndOfStream, Replxx::Color::DEFAULT },

        { TokenType::Error, Replxx::Color::RED },
        { TokenType::ErrorMultilineCommentIsNotClosed, Replxx::Color::RED },
        { TokenType::ErrorSingleQuoteIsNotClosed, Replxx::Color::RED },
        { TokenType::ErrorDoubleQuoteIsNotClosed, Replxx::Color::RED },
        { TokenType::ErrorSinglePipeMark, Replxx::Color::RED },
        { TokenType::ErrorWrongNumber, Replxx::Color::RED },
        { TokenType::ErrorMaxQuerySizeExceeded, Replxx::Color::RED }
    };

    const Replxx::Color unknown_token_color = Replxx::Color::RED;

    Lexer lexer(query.data(), query.data() + query.size());
    size_t pos = 0;

    for (Token token = lexer.nextToken(); !token.isEnd(); token = lexer.nextToken())
    {
        size_t utf8_len = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(token.begin), token.size());
        for (size_t code_point_index = 0; code_point_index < utf8_len; ++code_point_index)
        {
            if (token_to_color.find(token.type) != token_to_color.end())
                colors[pos + code_point_index] = token_to_color.at(token.type);
            else
                colors[pos + code_point_index] = unknown_token_color;
        }

        pos += utf8_len;
    }
}

}


ReplxxLineReader::ReplxxLineReader(
    const Suggest & suggest, const String & history_file_path_, bool multiline_, Patterns extenders_, Patterns delimiters_)
    : LineReader(history_file_path_, multiline_, std::move(extenders_), std::move(delimiters_))
{
    using namespace std::placeholders;
    using Replxx = replxx::Replxx;
    using namespace DB;

    if (!history_file_path.empty())
        rx.history_load(history_file_path);

    auto suggesting_callback = [&suggest] (const String & context, size_t context_size)
    {
        auto range = suggest.getCompletions(context, context_size);
        return Replxx::completions_t(range.first, range.second);
    };

    rx.set_highlighter_callback(highlightCallback);
    rx.set_completion_callback(suggesting_callback);
    rx.set_complete_on_empty(false);
    rx.set_word_break_characters(word_break_characters);

    /// By default C-p/C-n binded to COMPLETE_NEXT/COMPLETE_PREV,
    /// bind C-p/C-n to history-previous/history-next like readline.
    rx.bind_key(Replxx::KEY::control('N'), [this](char32_t code) { return rx.invoke(Replxx::ACTION::HISTORY_NEXT, code); });
    rx.bind_key(Replxx::KEY::control('P'), [this](char32_t code) { return rx.invoke(Replxx::ACTION::HISTORY_PREVIOUS, code); });

    /// By default COMPLETE_NEXT/COMPLETE_PREV was binded to C-p/C-n, re-bind
    /// to M-P/M-N (that was used for HISTORY_COMMON_PREFIX_SEARCH before, but
    /// it also binded to M-p/M-n).
    rx.bind_key(Replxx::KEY::meta('N'), [this](char32_t code) { return rx.invoke(Replxx::ACTION::COMPLETE_NEXT, code); });
    rx.bind_key(Replxx::KEY::meta('P'), [this](char32_t code) { return rx.invoke(Replxx::ACTION::COMPLETE_PREVIOUS, code); });
}

ReplxxLineReader::~ReplxxLineReader()
{
    if (!history_file_path.empty())
        rx.history_save(history_file_path);
}

LineReader::InputStatus ReplxxLineReader::readOneLine(const String & prompt)
{
    input.clear();

    const char* cinput = rx.input(prompt);
    if (cinput == nullptr)
        return (errno != EAGAIN) ? ABORT : RESET_LINE;
    input = cinput;

    trim(input);
    return INPUT_LINE;
}

void ReplxxLineReader::addToHistory(const String & line)
{
    rx.history_add(line);
}

void ReplxxLineReader::enableBracketedPaste()
{
    rx.enable_bracketed_paste();
};
