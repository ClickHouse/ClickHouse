#pragma once

#include <Parsers/Lexer.h>

#include <algorithm>
#include <string>
#include <vector>

/** C++ port of `fallbackTokenize` from `programs/server/play.html` - the plain-JS tokenizer the Web
  * UI uses on the request path when the WASM lexer cannot produce a token list (WebAssembly is
  * unavailable, or tokenization failed). It emits tokens with the same type codes for every category
  * the page's token walks classify - bare words, quoted identifiers, string literals, heredocs,
  * numbers, comments, brackets, commas, `=`, `;`, `+`, `-`, `*` - so `detectFramingSetting` and
  * `detectExplicitFormatClause` run the same walk in both paths. The tokenization rules mirror
  * `src/Parsers/Lexer.cpp` for the classified categories (`--`/`//`/`# `/`#!` line comments, nested
  * multiline comments, backslash escapes and doubled quotes in strings and quoted identifiers,
  * `$tag$...$tag$` heredocs, words of `[A-Za-z0-9_$]`). Keep this in sync with `fallbackTokenize` in
  * `programs/server/play.html`.
  *
  * The gtest regressions run every walk over both this tokenizer's output and the real `DB::Lexer`'s,
  * asserting that the classification agrees - so a divergence between the fallback path and the
  * lexer path fails loudly here instead of surfacing as a browser-only misclassification.
  */

namespace PlayFallbackTokenizer
{

/// The JS emits `TT_FALLBACK_OTHER` (-1) for everything it does not classify (multi-char operators,
/// a lone `?`, unicode, ...); no token walk treats it specially. `TokenType::Error` plays that role
/// here - it is likewise special for no walk.
constexpr DB::TokenType Other = DB::TokenType::Error;

struct Token
{
    DB::TokenType type;
    bool significant;
    std::string text;
};

inline std::vector<Token> tokenize(const std::string & text)
{
    std::vector<Token> tokens;
    size_t i = 0;
    const size_t n = text.size();

    const auto push = [&](DB::TokenType type, size_t end, bool significant = true)
    {
        end = std::min(end, n);
        tokens.push_back({type, significant, text.substr(i, end - i)});
        i = end;
    };
    const auto is_word_char = [](char c)
    {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '$';
    };
    const auto is_digit = [](char c) { return c >= '0' && c <= '9'; };
    const auto at = [&](size_t pos) -> char { return pos < n ? text[pos] : '\0'; };
    /// A quoted token (string literal or quoted identifier): `\` escapes the next character, a
    /// doubled quote continues the token.
    const auto quoted = [&](DB::TokenType type)
    {
        const char quote = text[i];
        size_t j = i + 1;
        while (j < n)
        {
            if (text[j] == '\\')
            {
                j += 2;
                continue;
            }
            if (text[j] == quote)
            {
                if (at(j + 1) == quote)
                {
                    j += 2;
                    continue;
                }
                ++j;
                break;
            }
            ++j;
        }
        push(type, j);
    };
    const auto line_comment = [&]()
    {
        const size_t j = text.find('\n', i);
        push(DB::TokenType::Comment, j == std::string::npos ? n : j, false);
    };

    while (i < n)
    {
        const char c = text[i];
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v')
        {
            size_t j = i + 1;
            while (j < n && (text[j] == ' ' || text[j] == '\t' || text[j] == '\n' || text[j] == '\r' || text[j] == '\f' || text[j] == '\v'))
                ++j;
            push(DB::TokenType::Whitespace, j, false);
        }
        else if (c == '-' && at(i + 1) == '-')
            line_comment();
        else if (c == '/' && at(i + 1) == '/')
            line_comment();
        else if (c == '#' && (at(i + 1) == ' ' || at(i + 1) == '!'))
            line_comment();
        else if (c == '/' && at(i + 1) == '*')
        {
            /// Nested multiline comments, as in the SQL standard.
            size_t level = 1;
            size_t j = i + 2;
            while (j + 1 < n && level > 0)
            {
                if (text[j] == '/' && text[j + 1] == '*')
                {
                    ++level;
                    j += 2;
                }
                else if (text[j] == '*' && text[j + 1] == '/')
                {
                    --level;
                    j += 2;
                }
                else
                    ++j;
            }
            push(DB::TokenType::Comment, level > 0 ? n : j, false);
        }
        else if (c == '\'')
            quoted(DB::TokenType::StringLiteral);
        else if (c == '`' || c == '"')
            quoted(DB::TokenType::QuotedIdentifier);
        else if (is_digit(c) || (c == '.' && is_digit(at(i + 1))))
        {
            /// Approximates the lexer's number token: word characters and dots, with a sign allowed
            /// right after an exponent marker. The walks only need the token's span and its text.
            size_t j = i + 1;
            while (j < n
                && ((is_word_char(text[j]) && text[j] != '$') || text[j] == '.'
                    || ((text[j] == '+' || text[j] == '-')
                        && (text[j - 1] == 'e' || text[j - 1] == 'E' || text[j - 1] == 'p' || text[j - 1] == 'P'))))
                ++j;
            push(DB::TokenType::Number, j);
        }
        else if (c == '(')
            push(DB::TokenType::OpeningRoundBracket, i + 1);
        else if (c == ')')
            push(DB::TokenType::ClosingRoundBracket, i + 1);
        else if (c == '[')
            push(DB::TokenType::OpeningSquareBracket, i + 1);
        else if (c == ']')
            push(DB::TokenType::ClosingSquareBracket, i + 1);
        else if (c == '{')
            push(DB::TokenType::OpeningCurlyBrace, i + 1);
        else if (c == '}')
            push(DB::TokenType::ClosingCurlyBrace, i + 1);
        else if (c == ',')
            push(DB::TokenType::Comma, i + 1);
        else if (c == ';')
            push(DB::TokenType::Semicolon, i + 1);
        else if (c == '*')
            push(DB::TokenType::Asterisk, i + 1);
        else if (c == '+')
            push(DB::TokenType::Plus, i + 1);
        else if (c == '=')
            push(DB::TokenType::Equals, at(i + 1) == '=' ? i + 2 : i + 1);
        else if (c == '-')
            push(at(i + 1) == '>' ? Other : DB::TokenType::Minus, at(i + 1) == '>' ? i + 2 : i + 1);
        else if ((c == '!' && at(i + 1) == '=') || (c == '<' && (at(i + 1) == '=' || at(i + 1) == '>'))
            || (c == '>' && at(i + 1) == '=') || (c == '|' && at(i + 1) == '|') || (c == ':' && at(i + 1) == ':'))
        {
            /// A two-character operator is consumed atomically, so its second character can never
            /// leak out as a spurious `=` in front of a settings walk.
            push(Other, i + 2);
        }
        else if (c == '$')
        {
            /// A `$tag$...$tag$` heredoc is one token, so nothing inside it can look like a clause.
            const size_t tag_end = text.find('$', i + 1);
            bool tag_ok = tag_end != std::string::npos;
            if (tag_ok)
                for (size_t k = i + 1; k < tag_end; ++k)
                    if (!is_word_char(text[k]))
                        tag_ok = false;
            const size_t closing = tag_ok ? text.find(text.substr(i, tag_end + 1 - i), tag_end + 1) : std::string::npos;
            if (closing != std::string::npos)
                push(DB::TokenType::HereDoc, closing + (tag_end + 1 - i));
            else if (!is_word_char(at(i + 1)))
                push(Other, i + 1);
            else
            {
                size_t j = i + 1;
                while (j < n && is_word_char(text[j]))
                    ++j;
                push(DB::TokenType::BareWord, j);
            }
        }
        else if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_')
        {
            size_t j = i + 1;
            while (j < n && is_word_char(text[j]))
                ++j;
            push(DB::TokenType::BareWord, j);
        }
        else
            push(Other, i + 1);
    }
    return tokens;
}

}
