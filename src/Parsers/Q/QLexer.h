#pragma once

#include <base/types.h>

#include <string_view>
#include <vector>


namespace DB
{

/// Tokens of the q language (the language of kdb+).
///
/// q is lexically very different from SQL: a space-separated run of numbers is one vector literal,
/// `/` starts a comment only after whitespace, `-1` is a negative literal only when the minus
/// does not follow a noun, and names may contain dots. The lexer below implements these rules.
enum class QTokenType : uint8_t
{
    /// A numeric, boolean, byte, or temporal literal, such as `42`, `1.5e3`, `101b`, `0x0aff`,
    /// `2024.01.15`, `2024.01.15D10:30:00`, `0D01:00`, `10:30:00.000`, `0N`, `0w`.
    Number,
    /// A symbol, such as `` `abc `` or the null symbol `` ` ``. Adjacent symbols without spaces between them form a symbol vector.
    Symbol,
    /// A string (a char vector) or a char in double quotes. The `value` holds the decoded text.
    String,
    /// A name, which may contain dots: `x`, `t.col`, `.z.p`.
    Name,
    /// A primitive verb: one of ``+ - * % ! & | < > = ~ , ^ # _ $ ? @ .``, the two-character
    /// comparisons `<=`, `>=`, `<>`, optionally followed by `:`, which makes it monadic (e.g. `-:`).
    Verb,
    /// An adverb: `'` (each), `':` (each prior), `/` (over), `/:` (each right), `\` (scan), `\:` (each left).
    Adverb,
    /// `:` as the assignment, or a lone `:` verb.
    Colon,
    /// `::` (the identity or a global assignment).
    DoubleColon,
    OpenParen,
    CloseParen,
    OpenBracket,
    CloseBracket,
    OpenBrace,
    CloseBrace,
    Semicolon,
    /// A line break that ends a statement: it is not inside brackets, and the next line starts in the first column.
    StatementBreak,
    End,
};

struct QToken
{
    QTokenType type = QTokenType::End;
    /// The source text of the token.
    std::string_view text;
    /// The decoded value of a `String` token, or the name of a `Symbol` without the backtick.
    String value;
    /// The offset of the token in the source text.
    size_t offset = 0;
    /// Whether whitespace (or the beginning of the text) precedes the token.
    bool space_before = false;
};

using QTokens = std::vector<QToken>;

/// Splits a q program into tokens. Throws `SYNTAX_ERROR` on malformed input.
QTokens tokenizeQ(std::string_view text);

}
