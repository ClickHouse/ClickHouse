#pragma once

#include <unordered_map>

#include "LineReader.h"
#include <Parsers/Lexer.h>

#include <replxx.hxx>

class ReplxxLineReader : public LineReader
{
public:
    ReplxxLineReader(const Suggest & suggest, const String & history_file_path, char extender, char delimiter = 0);
    ~ReplxxLineReader() override;

    void enableBracketedPaste() override;

private:
    InputStatus readOneLine(const String & prompt) override;
    void addToHistory(const String & line) override;

    const std::unordered_map<DB::TokenType, replxx::Replxx::Color> token_to_color = {
        { DB::TokenType::Whitespace, replxx::Replxx::Color::NORMAL },
        { DB::TokenType::Comment, replxx::Replxx::Color::GRAY },
        { DB::TokenType::BareWord, replxx::Replxx::Color::RED },
        { DB::TokenType::Number, replxx::Replxx::Color::BLUE },
        { DB::TokenType::StringLiteral, replxx::Replxx::Color::BRIGHTCYAN },
        { DB::TokenType::QuotedIdentifier, replxx::Replxx::Color::BRIGHTMAGENTA },
        { DB::TokenType::OpeningRoundBracket, replxx::Replxx::Color::LIGHTGRAY },
        { DB::TokenType::ClosingRoundBracket, replxx::Replxx::Color::LIGHTGRAY },
        { DB::TokenType::OpeningSquareBracket, replxx::Replxx::Color::BROWN },
        { DB::TokenType::ClosingSquareBracket, replxx::Replxx::Color::BROWN },
        { DB::TokenType::OpeningCurlyBrace, replxx::Replxx::Color::WHITE },
        { DB::TokenType::ClosingCurlyBrace, replxx::Replxx::Color::WHITE },
        { DB::TokenType::Comma, replxx::Replxx::Color::GREEN },
        { DB::TokenType::Semicolon, replxx::Replxx::Color::GREEN },
        { DB::TokenType::Dot, replxx::Replxx::Color::GREEN },
        { DB::TokenType::Asterisk, replxx::Replxx::Color::GREEN },
        { DB::TokenType::Plus, replxx::Replxx::Color::GREEN },
        { DB::TokenType::Minus, replxx::Replxx::Color::GREEN },
        { DB::TokenType::Slash, replxx::Replxx::Color::GREEN },
        { DB::TokenType::Percent, replxx::Replxx::Color::GREEN },
        { DB::TokenType::Arrow, replxx::Replxx::Color::GREEN },
        { DB::TokenType::QuestionMark, replxx::Replxx::Color::GREEN },
        { DB::TokenType::Colon, replxx::Replxx::Color::CYAN },
        { DB::TokenType::Equals, replxx::Replxx::Color::CYAN },
        { DB::TokenType::NotEquals, replxx::Replxx::Color::CYAN },
        { DB::TokenType::Less, replxx::Replxx::Color::CYAN },
        { DB::TokenType::Greater, replxx::Replxx::Color::CYAN },
        { DB::TokenType::LessOrEquals, replxx::Replxx::Color::CYAN },
        { DB::TokenType::GreaterOrEquals, replxx::Replxx::Color::CYAN },
        { DB::TokenType::Concatenation, replxx::Replxx::Color::YELLOW },
        { DB::TokenType::At, replxx::Replxx::Color::YELLOW },
        { DB::TokenType::EndOfStream, replxx::Replxx::Color::NORMAL },
        { DB::TokenType::Error, replxx::Replxx::Color::RED },
        { DB::TokenType::ErrorMultilineCommentIsNotClosed, replxx::Replxx::Color::RED },
        { DB::TokenType::ErrorSingleQuoteIsNotClosed, replxx::Replxx::Color::RED },
        { DB::TokenType::ErrorDoubleQuoteIsNotClosed, replxx::Replxx::Color::RED },
       // { DB::TokenType::ErrorBackQuoteIsNotClosedErrorSingleExclamationMark, replxx::Replxx::Color::RED },
        { DB::TokenType::ErrorSinglePipeMark, replxx::Replxx::Color::RED },
        { DB::TokenType::ErrorWrongNumber, replxx::Replxx::Color::RED },
        { DB::TokenType::ErrorMaxQuerySizeExceeded, replxx::Replxx::Color::RED }
    };

    const replxx::Replxx::Color unknown_token_color = replxx::Replxx::Color::RED;

    replxx::Replxx rx;
};
