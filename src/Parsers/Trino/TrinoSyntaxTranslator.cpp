#include <Parsers/Trino/TrinoSyntaxTranslator.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>

#include <algorithm>
#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int SYNTAX_ERROR;
}

namespace
{

bool tokenIsKeyword(const Token & token, std::string_view keyword)
{
    if (token.type != TokenType::BareWord)
        return false;
    if (token.size() != keyword.size())
        return false;
    return strncasecmp(token.begin, keyword.data(), keyword.size()) == 0;
}

/// The kind of construct that precedes UNNEST and determines the translation.
enum class UnnestKind : uint8_t
{
    ArrayJoin,        /// CROSS JOIN UNNEST, INNER JOIN UNNEST, implicit comma join
    LeftArrayJoin,    /// LEFT [OUTER] JOIN UNNEST ... ON TRUE
    Standalone,       /// FROM UNNEST(...) - no table on the left
};

class Translator
{
public:
    Translator(const std::vector<Token> & tokens_, const char * source_begin_, const char * source_end_)
        : tokens(tokens_), source_begin(source_begin_), source_end(source_end_)
    {
    }

    std::optional<String> run()
    {
        translateRange(0, tokens.size(), /*type_context=*/ false);
        if (!changed)
            return std::nullopt;
        return std::move(out);
    }

private:
    const std::vector<Token> & tokens;
    [[maybe_unused]] const char * source_begin;
    [[maybe_unused]] const char * source_end;

    String out;
    bool changed = false;

    [[noreturn]] void throwNotSupported(const Token & token, std::string_view what, std::string_view hint) const
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "{} is not supported by the Trino dialect translation (near position {}){}{}",
            what,
            token.begin - source_begin + 1,
            hint.empty() ? "" : ". ",
            hint);
    }

    bool isKeywordAt(size_t idx, std::string_view keyword) const
    {
        return idx < tokens.size() && tokenIsKeyword(tokens[idx], keyword);
    }

    bool isTypeAt(size_t idx, TokenType type) const
    {
        return idx < tokens.size() && tokens[idx].type == type;
    }

    /// Returns the index of the round bracket that closes the one at open_idx, or tokens.size() if unbalanced.
    size_t findMatchingParen(size_t open_idx) const
    {
        chassert(tokens[open_idx].type == TokenType::OpeningRoundBracket);
        size_t depth = 0;
        for (size_t j = open_idx; j < tokens.size(); ++j)
        {
            if (tokens[j].type == TokenType::OpeningRoundBracket)
                ++depth;
            else if (tokens[j].type == TokenType::ClosingRoundBracket)
            {
                --depth;
                if (depth == 0)
                    return j;
            }
        }
        return tokens.size();
    }

    void emitText(std::string_view text)
    {
        out.append(text);
        out.push_back(' ');
    }

    void emitToken(const Token & token)
    {
        if (token.type == TokenType::StringLiteral)
        {
            /// In Trino a backslash inside a string literal is a regular character,
            /// while ClickHouse processes escape sequences. Double the backslashes
            /// to preserve Trino semantics.
            if (memchr(token.begin, '\\', token.size()) != nullptr)
            {
                String escaped;
                escaped.reserve(token.size() + 8);
                for (const char * c = token.begin; c < token.end; ++c)
                {
                    if (*c == '\\')
                        escaped += "\\\\";
                    else
                        escaped.push_back(*c);
                }
                changed = true;
                emitText(escaped);
                return;
            }
        }
        emitText(std::string_view(token.begin, token.size()));
    }

    /// Translates tokens of a sub-range into a separate string (used for expression
    /// fragments that are re-assembled by structural rewrites such as UNNEST).
    String translateSubRange(size_t begin_idx, size_t end_idx, bool type_context)
    {
        String saved = std::move(out);
        out.clear();
        translateRange(begin_idx, end_idx, type_context);
        /// Trim the trailing space for nicer re-assembly.
        while (!out.empty() && out.back() == ' ')
            out.pop_back();
        String result = std::move(out);
        out = std::move(saved);
        return result;
    }

    /// Splits the token range (typically the inside of parentheses) at top-level commas.
    std::vector<std::pair<size_t, size_t>> splitTopLevelCommas(size_t begin_idx, size_t end_idx) const
    {
        std::vector<std::pair<size_t, size_t>> parts;
        size_t depth = 0;
        size_t part_begin = begin_idx;
        for (size_t j = begin_idx; j < end_idx; ++j)
        {
            TokenType type = tokens[j].type;
            if (type == TokenType::OpeningRoundBracket || type == TokenType::OpeningSquareBracket)
                ++depth;
            else if (type == TokenType::ClosingRoundBracket || type == TokenType::ClosingSquareBracket)
            {
                if (depth > 0)
                    --depth;
            }
            else if (type == TokenType::Comma && depth == 0)
            {
                parts.emplace_back(part_begin, j);
                part_begin = j + 1;
            }
        }
        if (part_begin < end_idx)
            parts.emplace_back(part_begin, end_idx);
        return parts;
    }

    void translateRange(size_t begin_idx, size_t end_idx, bool type_context)
    {
        size_t i = begin_idx;
        while (i < end_idx)
            translateOne(i, end_idx, type_context);
    }

    /// Translates the construct starting at token `i` and advances `i` past it.
    void translateOne(size_t & i, size_t end_idx, bool type_context)
    {
        const Token & token = tokens[i];

        if (token.type == TokenType::StringLiteral)
        {
            emitToken(token);
            ++i;
            return;
        }

        /// Lambdas are parenthesized: the ClickHouse parser cannot parse a lambda
        /// that follows a literal argument (`f(1, x -> x)`), and in Trino lambdas
        /// are always the last argument. A parenthesized lambda always parses.
        if (!type_context && tryWrapLambda(i, end_idx))
            return;

        /// Comma-join with UNNEST behaves like CROSS JOIN UNNEST: ", UNNEST(...)" -> "ARRAY JOIN ...".
        if (token.type == TokenType::Comma && isKeywordAt(i + 1, "UNNEST") && isTypeAt(i + 2, TokenType::OpeningRoundBracket))
        {
            ++i;
            translateUnnest(i, end_idx, UnnestKind::ArrayJoin);
            return;
        }

        /// A parenthesized VALUES table: (VALUES (1, 'a'), (2, 'b')) becomes a
        /// subquery over SQLStandardValues, so that a following column alias
        /// list (AS t(x, y)) applies to it.
        if (token.type == TokenType::OpeningRoundBracket && isKeywordAt(i + 1, "VALUES"))
        {
            size_t close = findMatchingParen(i);
            if (close != tokens.size())
            {
                changed = true;
                out += "(SELECT * FROM SQLStandardValues(";
                emitValuesRows(i + 2, close);
                out += ")) ";
                i = close + 1;
                return;
            }
        }

        /// A fractional literal without the leading zero: BETWEEN .06 - 0.01 ...
        /// The Lexer assumes the tuple-access meaning of the dot after a bare word
        /// because it cannot distinguish identifiers from keywords.
        if (token.type == TokenType::Dot && isTypeAt(i + 1, TokenType::Number) && tokens[i + 1].begin == token.end
            && isLeadingDotNumberContext(i))
        {
            changed = true;
            emitText("0." + String(tokens[i + 1].begin, tokens[i + 1].size()));
            i += 2;
            return;
        }

        if (token.type != TokenType::BareWord)
        {
            emitToken(token);
            ++i;
            return;
        }

        /// ARRAY[1, 2, 3] -> [1, 2, 3]
        if (tokenIsKeyword(token, "ARRAY") && isTypeAt(i + 1, TokenType::OpeningSquareBracket))
        {
            changed = true;
            ++i;
            return;
        }

        if (type_context)
        {
            translateTypeWord(i, end_idx);
            return;
        }

        if ((tokenIsKeyword(token, "TRY_CAST") || tokenIsKeyword(token, "CAST")) && isTypeAt(i + 1, TokenType::OpeningRoundBracket))
        {
            translateCast(i, tokenIsKeyword(token, "TRY_CAST"));
            return;
        }

        /// ROW(1, 'a') constructor -> tuple(1, 'a')
        if (tokenIsKeyword(token, "ROW") && isTypeAt(i + 1, TokenType::OpeningRoundBracket))
        {
            changed = true;
            emitText("tuple");
            ++i;
            return;
        }

        if (tokenIsKeyword(token, "TRY") && isTypeAt(i + 1, TokenType::OpeningRoundBracket))
            throwNotSupported(token, "The TRY function", "Consider TRY_CAST or explicit checks with if().");

        if (tokenIsKeyword(token, "TABLESAMPLE"))
            throwNotSupported(token, "TABLESAMPLE", "Consider the SAMPLE clause of ClickHouse (dialect = 'clickhouse').");

        /// FROM UNNEST(...) - a standalone table.
        if (tokenIsKeyword(token, "FROM") && isKeywordAt(i + 1, "UNNEST") && isTypeAt(i + 2, TokenType::OpeningRoundBracket))
        {
            emitToken(token);
            ++i;
            translateUnnest(i, end_idx, UnnestKind::Standalone);
            return;
        }

        /// CROSS JOIN UNNEST(...) / INNER JOIN UNNEST(...) / JOIN UNNEST(...) -> ARRAY JOIN
        if ((tokenIsKeyword(token, "CROSS") || tokenIsKeyword(token, "INNER")) && isKeywordAt(i + 1, "JOIN")
            && isKeywordAt(i + 2, "UNNEST") && isTypeAt(i + 3, TokenType::OpeningRoundBracket))
        {
            i += 2;
            translateUnnest(i, end_idx, UnnestKind::ArrayJoin);
            return;
        }

        if (tokenIsKeyword(token, "JOIN") && isKeywordAt(i + 1, "UNNEST") && isTypeAt(i + 2, TokenType::OpeningRoundBracket))
        {
            ++i;
            translateUnnest(i, end_idx, UnnestKind::ArrayJoin);
            return;
        }

        /// LEFT [OUTER] JOIN UNNEST(...) ON TRUE -> LEFT ARRAY JOIN
        if (tokenIsKeyword(token, "LEFT"))
        {
            size_t j = i + 1;
            if (isKeywordAt(j, "OUTER"))
                ++j;
            if (isKeywordAt(j, "JOIN") && isKeywordAt(j + 1, "UNNEST") && isTypeAt(j + 2, TokenType::OpeningRoundBracket))
            {
                i = j + 1;
                translateUnnest(i, end_idx, UnnestKind::LeftArrayJoin);
                return;
            }
        }

        if (tokenIsKeyword(token, "UNNEST") && isTypeAt(i + 1, TokenType::OpeningRoundBracket))
            throwNotSupported(
                token,
                "This form of UNNEST",
                "Only FROM UNNEST(...), [CROSS/INNER/LEFT] JOIN UNNEST(...) and comma-separated UNNEST(...) are translated.");

        /// OFFSET n [ROW|ROWS] [LIMIT m | FETCH ...] -> LIMIT m OFFSET n
        if (tokenIsKeyword(token, "OFFSET"))
        {
            translateOffset(i, end_idx);
            return;
        }

        /// LIMIT ALL -> (nothing)
        if (tokenIsKeyword(token, "LIMIT") && isKeywordAt(i + 1, "ALL"))
        {
            changed = true;
            i += 2;
            return;
        }

        /// FETCH FIRST n ROWS ONLY -> LIMIT n
        if (tokenIsKeyword(token, "FETCH") && (isKeywordAt(i + 1, "FIRST") || isKeywordAt(i + 1, "NEXT")))
        {
            translateFetch(i, end_idx);
            return;
        }

        /// Statement-level VALUES (1, 'a'), (2, 'b') -> SELECT * FROM SQLStandardValues((1, 'a'), (2, 'b'))
        if (i == 0 && tokenIsKeyword(token, "VALUES"))
        {
            translateValuesStatement(i, end_idx);
            return;
        }

        /// TRIM('x' FROM s) -> TRIM(BOTH 'x' FROM s) and TRIM(LEADING FROM s) ->
        /// TRIM(LEADING ' ' FROM s): ClickHouse accepts only the full form.
        if (tokenIsKeyword(token, "TRIM") && isTypeAt(i + 1, TokenType::OpeningRoundBracket))
        {
            if (isTypeAt(i + 2, TokenType::StringLiteral) && isKeywordAt(i + 3, "FROM"))
            {
                changed = true;
                emitToken(token);
                emitToken(tokens[i + 1]);
                emitText("BOTH");
                i += 2;
                return;
            }
            if ((isKeywordAt(i + 2, "LEADING") || isKeywordAt(i + 2, "TRAILING") || isKeywordAt(i + 2, "BOTH"))
                && isKeywordAt(i + 3, "FROM"))
            {
                changed = true;
                emitToken(token);
                emitToken(tokens[i + 1]);
                emitToken(tokens[i + 2]);
                emitText("' '");
                i += 3;
                return;
            }
        }

        /// DECIMAL '123.45' -> CAST('123.45' AS Decimal(5, 2)).
        if (tokenIsKeyword(token, "DECIMAL") && isTypeAt(i + 1, TokenType::StringLiteral))
        {
            if (translateDecimalLiteral(i))
                return;
        }

        /// JSON '{"a": 1}' -> CAST('{"a": 1}' AS JSON). The ClickHouse JSON type
        /// stores objects, so non-object documents are rejected at execution.
        if (tokenIsKeyword(token, "JSON") && isTypeAt(i + 1, TokenType::StringLiteral))
        {
            changed = true;
            out += "CAST(";
            emitToken(tokens[i + 1]);
            out += "AS JSON) ";
            i += 2;
            return;
        }

        /// BETWEEN ASYMMETRIC is the default BETWEEN; BETWEEN SYMMETRIC orders
        /// the bounds first: x BETWEEN least(a, b) AND greatest(a, b).
        if (tokenIsKeyword(token, "BETWEEN") && isKeywordAt(i + 1, "ASYMMETRIC"))
        {
            changed = true;
            emitToken(token);
            i += 2;
            return;
        }

        if (tokenIsKeyword(token, "BETWEEN") && isKeywordAt(i + 1, "SYMMETRIC"))
        {
            translateBetweenSymmetric(i, end_idx);
            return;
        }

        /// TIMESTAMP '2022-11-01 09:08:07.321 [Asia/Tokyo]': the plain ClickHouse
        /// TIMESTAMP literal drops the fractional seconds (it produces DateTime)
        /// and does not understand region time zone names.
        if (tokenIsKeyword(token, "TIMESTAMP") && isTypeAt(i + 1, TokenType::StringLiteral))
        {
            if (translateTimestampLiteral(i))
                return;
        }

        /// nan() and infinity() -> the nan and inf literals (in ClickHouse these
        /// are literal keywords, so a function-call form does not even parse).
        if ((tokenIsKeyword(token, "NAN") || tokenIsKeyword(token, "INFINITY"))
            && isTypeAt(i + 1, TokenType::OpeningRoundBracket) && isTypeAt(i + 2, TokenType::ClosingRoundBracket))
        {
            changed = true;
            emitText(tokenIsKeyword(token, "NAN") ? "nan" : "inf");
            i += 3;
            return;
        }

        /// date_add('day', 5, x) -> date_add(DAY, 5, x): the ClickHouse parser
        /// accepts the unit of its special DATE_ADD form only as a bare keyword.
        if (tokenIsKeyword(token, "DATE_ADD") && isTypeAt(i + 1, TokenType::OpeningRoundBracket)
            && isTypeAt(i + 2, TokenType::StringLiteral) && isTypeAt(i + 3, TokenType::Comma) && tokens[i + 2].size() > 2)
        {
            std::string_view unit(tokens[i + 2].begin + 1, tokens[i + 2].size() - 2);
            bool alphabetic = !unit.empty();
            for (char c : unit)
                alphabetic &= isAlphaASCII(c);
            if (alphabetic)
            {
                changed = true;
                emitToken(token);
                emitToken(tokens[i + 1]);
                emitText(unit);
                emitToken(tokens[i + 3]);
                i += 4;
                return;
            }
        }

        /// SET SESSION name = value -> SET name = value
        if (i == 0 && tokenIsKeyword(token, "SET") && isKeywordAt(i + 1, "SESSION"))
        {
            changed = true;
            emitToken(token);
            i += 2;
            return;
        }

        emitToken(token);
        ++i;
    }

    /// DECIMAL '123.45' -> CAST('123.45' AS Decimal(5, 2)). Returns false when
    /// the literal is not a plain decimal number (then it is left as-is).
    bool translateDecimalLiteral(size_t & i)
    {
        const Token & literal = tokens[i + 1];
        if (literal.size() < 2)
            return false;
        std::string_view text(literal.begin + 1, literal.size() - 2);

        size_t digits = 0;
        size_t scale = 0;
        bool seen_dot = false;
        size_t pos_in_text = 0;
        if (pos_in_text < text.size() && (text[pos_in_text] == '+' || text[pos_in_text] == '-'))
            ++pos_in_text;
        for (; pos_in_text < text.size(); ++pos_in_text)
        {
            char c = text[pos_in_text];
            if (c == '.' && !seen_dot)
            {
                seen_dot = true;
            }
            else if (isNumericASCII(c))
            {
                ++digits;
                if (seen_dot)
                    ++scale;
            }
            else
                return false;
        }
        if (digits == 0 || digits > 76)
            return false;

        changed = true;
        out += "CAST(";
        emitToken(literal);
        out += "AS Decimal(" + std::to_string(digits) + ", " + std::to_string(scale) + ")) ";
        i += 2;
        return true;
    }

    /// TIMESTAMP '<date> <time>[.fraction][ <zone>]' with a fractional part or a
    /// named time zone -> toDateTime64 / parseDateTime64BestEffort. Returns false
    /// when the plain ClickHouse TIMESTAMP literal handles the value already.
    bool translateTimestampLiteral(size_t & i)
    {
        const Token & literal = tokens[i + 1];
        if (literal.size() < 2)
            return false;
        String text(literal.begin + 1, literal.size() - 2);

        /// Split into date, time and an optional zone part.
        std::vector<String> parts;
        size_t start = 0;
        while (start < text.size())
        {
            size_t space = text.find(' ', start);
            if (space == String::npos)
                space = text.size();
            if (space > start)
                parts.push_back(text.substr(start, space - start));
            start = space + 1;
        }
        if (parts.size() < 2 || parts.size() > 3 || text.find('\'') != String::npos || text.find('\\') != String::npos)
            return false;

        size_t scale = 0;
        if (size_t dot = parts[1].find('.'); dot != String::npos)
            scale = std::min<size_t>(9, parts[1].size() - dot - 1);

        bool has_named_zone = parts.size() == 3 && isAlphaASCII(parts[2][0]);
        bool has_offset = parts.size() == 3 && !has_named_zone;

        if (scale == 0 && !has_named_zone)
            return false;

        changed = true;
        if (has_named_zone)
            out += "toDateTime64('" + parts[0] + " " + parts[1] + "', " + std::to_string(scale) + ", '" + parts[2] + "') ";
        else if (has_offset)
            out += "parseDateTime64BestEffort('" + text + "', " + std::to_string(scale) + ") ";
        else
            out += "toDateTime64('" + text + "', " + std::to_string(scale) + ") ";
        i += 2;
        return true;
    }

    /// Finds where a BETWEEN bound expression ends: the next AND/OR, comma,
    /// unbalanced closing bracket or clause keyword at the top nesting level.
    /// CASE ... END counts as nesting (it may contain top-level AND).
    size_t findBetweenBoundEnd(size_t begin_idx, size_t end_idx) const
    {
        size_t depth = 0;
        for (size_t j = begin_idx; j < end_idx; ++j)
        {
            const Token & token = tokens[j];
            if (token.type == TokenType::OpeningRoundBracket || token.type == TokenType::OpeningSquareBracket
                || tokenIsKeyword(token, "CASE"))
            {
                ++depth;
                continue;
            }
            if (token.type == TokenType::ClosingRoundBracket || token.type == TokenType::ClosingSquareBracket
                || tokenIsKeyword(token, "END"))
            {
                if (depth == 0)
                    return j;
                --depth;
                continue;
            }
            if (depth > 0)
                continue;
            if (token.type == TokenType::Comma || token.type == TokenType::Semicolon)
                return j;
            if (token.type == TokenType::BareWord)
            {
                for (const auto * keyword :
                     {"AND",  "OR",     "IS",    "NOT",   "IN",     "LIKE",   "ILIKE", "BETWEEN", "AS",     "ASC",
                      "DESC", "WHERE",  "GROUP", "ORDER", "HAVING", "LIMIT",  "OFFSET", "FETCH",  "SETTINGS", "UNION",
                      "EXCEPT", "INTERSECT", "FROM", "THEN", "ELSE", "WHEN", "ON", "JOIN", "INNER", "LEFT",
                      "RIGHT", "FULL", "CROSS", "USING", "PREWHERE", "WINDOW", "FORMAT", "INTO"})
                    if (tokenIsKeyword(token, keyword))
                        return j;
            }
        }
        return end_idx;
    }

    /// x BETWEEN SYMMETRIC a AND b -> x BETWEEN least(a, b) AND greatest(a, b).
    /// `i` points at the BETWEEN token.
    void translateBetweenSymmetric(size_t & i, size_t end_idx)
    {
        size_t low_begin = i + 2;
        size_t and_idx = findBetweenBoundEnd(low_begin, end_idx);
        if (and_idx >= end_idx || !tokenIsKeyword(tokens[and_idx], "AND") || and_idx == low_begin)
            throwNotSupported(tokens[i], "This form of BETWEEN SYMMETRIC", "");
        size_t high_begin = and_idx + 1;
        size_t high_end = findBetweenBoundEnd(high_begin, end_idx);
        if (high_end == high_begin)
            throwNotSupported(tokens[i], "This form of BETWEEN SYMMETRIC", "");

        String low = translateSubRange(low_begin, and_idx, /*type_context=*/ false);
        String high = translateSubRange(high_begin, high_end, /*type_context=*/ false);

        changed = true;
        out += "BETWEEN least(" + low + ", " + high + ") AND greatest(" + low + ", " + high + ") ";
        i = high_end;
    }

    /// Whether a dot at position `idx` starts a numeric literal (`.06`) rather
    /// than a tuple element access (`t.1`), judged by the preceding token.
    bool isLeadingDotNumberContext(size_t idx) const
    {
        if (idx == 0)
            return true;
        const Token & prev = tokens[idx - 1];
        switch (prev.type)
        {
            case TokenType::Comma:
            case TokenType::OpeningRoundBracket:
            case TokenType::OpeningSquareBracket:
            case TokenType::Equals:
            case TokenType::NotEquals:
            case TokenType::Less:
            case TokenType::Greater:
            case TokenType::LessOrEquals:
            case TokenType::GreaterOrEquals:
            case TokenType::Plus:
            case TokenType::Minus:
            case TokenType::Asterisk:
            case TokenType::Slash:
            case TokenType::Percent:
            case TokenType::Concatenation:
            case TokenType::Arrow:
                return true;
            case TokenType::BareWord:
                /// Keywords that are followed by an expression. An identifier here
                /// would mean tuple element access instead.
                for (const auto * keyword : {"AND", "OR", "NOT", "BETWEEN", "IN", "LIKE", "ILIKE", "WHEN", "THEN", "ELSE",
                                             "SELECT", "WHERE", "HAVING", "ON", "BY", "LIMIT", "OFFSET", "AS", "CASE", "IS"})
                    if (tokenIsKeyword(prev, keyword))
                        return true;
                return false;
            default:
                return false;
        }
    }

    /// Recognizes `x -> expr` and `(x, y) -> expr` at the current position and
    /// emits the whole lambda wrapped in parentheses. The body extends to the
    /// next top-level comma or the end of the enclosing parentheses.
    bool tryWrapLambda(size_t & i, size_t end_idx)
    {
        size_t arrow = 0;
        if (tokens[i].type == TokenType::BareWord && isTypeAt(i + 1, TokenType::Arrow))
        {
            arrow = i + 1;
        }
        else if (tokens[i].type == TokenType::OpeningRoundBracket)
        {
            size_t close = findMatchingParen(i);
            if (close >= end_idx || !isTypeAt(close + 1, TokenType::Arrow))
                return false;
            for (size_t j = i + 1; j < close; ++j)
                if (tokens[j].type != TokenType::BareWord && tokens[j].type != TokenType::Comma)
                    return false;
            arrow = close + 1;
        }
        else
            return false;

        size_t body_end = end_idx;
        size_t depth = 0;
        for (size_t j = arrow + 1; j < end_idx; ++j)
        {
            TokenType type = tokens[j].type;
            if (type == TokenType::OpeningRoundBracket || type == TokenType::OpeningSquareBracket)
                ++depth;
            else if (type == TokenType::ClosingRoundBracket || type == TokenType::ClosingSquareBracket)
            {
                if (depth == 0)
                {
                    body_end = j;
                    break;
                }
                --depth;
            }
            else if (type == TokenType::Comma && depth == 0)
            {
                body_end = j;
                break;
            }
        }

        changed = true;
        out += "( ";
        for (size_t j = i; j <= arrow; ++j)
            emitToken(tokens[j]);
        translateRange(arrow + 1, body_end, /*type_context=*/ false);
        out += ") ";
        i = body_end;
        return true;
    }

    /// Type names inside CAST(... AS <type>). Most Trino type names (VARCHAR, BIGINT,
    /// DOUBLE, ...) are known to ClickHouse as case-insensitive aliases; the composite
    /// ones are spelled lowercase in Trino and need explicit mapping.
    void translateTypeWord(size_t & i, size_t end_idx)
    {
        const Token & token = tokens[i];

        if (isTypeAt(i + 1, TokenType::OpeningRoundBracket) || tokenIsKeyword(token, "TIMESTAMP") || tokenIsKeyword(token, "TIME"))
        {
            if (tokenIsKeyword(token, "ROW"))
            {
                changed = true;
                emitText("Tuple");
                ++i;
                return;
            }
            if (tokenIsKeyword(token, "ARRAY"))
            {
                changed = true;
                emitText("Array");
                ++i;
                return;
            }
            if (tokenIsKeyword(token, "MAP"))
            {
                changed = true;
                emitText("Map");
                ++i;
                return;
            }
            if (tokenIsKeyword(token, "TIMESTAMP") || tokenIsKeyword(token, "TIME"))
            {
                translateTimestampType(i, end_idx);
                return;
            }
        }

        emitToken(token);
        ++i;
    }

    /// TIMESTAMP [(p)] [WITH TIME ZONE] -> DateTime64(p), with the Trino default precision of 3.
    /// TIME is not supported (ClickHouse Time is experimental and has different semantics).
    void translateTimestampType(size_t & i, size_t end_idx)
    {
        const Token & token = tokens[i];
        if (tokenIsKeyword(token, "TIME") && !isKeywordAt(i + 1, "ZONE"))
            throwNotSupported(token, "The TIME type", "");

        ++i;
        String precision = "3";
        if (i < end_idx && tokens[i].type == TokenType::OpeningRoundBracket && isTypeAt(i + 1, TokenType::Number)
            && isTypeAt(i + 2, TokenType::ClosingRoundBracket))
        {
            precision = String(tokens[i + 1].begin, tokens[i + 1].size());
            i += 3;
        }
        if (isKeywordAt(i, "WITH") && isKeywordAt(i + 1, "TIME") && isKeywordAt(i + 2, "ZONE"))
            i += 3;

        changed = true;
        emitText("DateTime64(" + precision + ")");
    }

    /// CAST(x AS t): translates the type region; TRY_CAST(x AS t) -> accurateCastOrNull(x, 't').
    void translateCast(size_t & i, bool is_try)
    {
        size_t open = i + 1;
        size_t close = findMatchingParen(open);
        if (close == tokens.size())
        {
            /// Unbalanced parentheses: emit as-is and let the parser report the error.
            emitToken(tokens[i]);
            ++i;
            return;
        }

        /// Find the last top-level AS: the expression may contain AS only inside
        /// nested parentheses (subqueries), while the type never contains AS.
        size_t as_idx = tokens.size();
        size_t depth = 0;
        for (size_t j = open + 1; j < close; ++j)
        {
            TokenType type = tokens[j].type;
            if (type == TokenType::OpeningRoundBracket || type == TokenType::OpeningSquareBracket)
                ++depth;
            else if (type == TokenType::ClosingRoundBracket || type == TokenType::ClosingSquareBracket)
            {
                if (depth > 0)
                    --depth;
            }
            else if (depth == 0 && tokenIsKeyword(tokens[j], "AS"))
                as_idx = j;
        }

        if (as_idx == tokens.size())
        {
            /// No AS: not the standard form, pass through.
            emitToken(tokens[i]);
            ++i;
            return;
        }

        String expression = translateSubRange(open + 1, as_idx, /*type_context=*/ false);
        String type = translateSubRange(as_idx + 1, close, /*type_context=*/ true);

        if (is_try)
        {
            if (type.find('\'') != String::npos)
                throwNotSupported(tokens[i], "TRY_CAST to this type", "");
            changed = true;
            out += "accurateCastOrNull(" + expression + ", '" + type + "') ";
        }
        else
        {
            out += "CAST(" + expression + " AS " + type + ") ";
        }
        i = close + 1;
    }

    /// UNNEST(e1, e2, ...) [WITH ORDINALITY] [AS] [alias] [(c1, c2, ...)] [ON TRUE]
    /// `i` points at the UNNEST token.
    void translateUnnest(size_t & i, size_t end_idx, UnnestKind kind)
    {
        const Token & unnest_token = tokens[i];
        size_t open = i + 1;
        size_t close = findMatchingParen(open);
        if (close == tokens.size())
            throwNotSupported(unnest_token, "UNNEST with unbalanced parentheses", "");

        std::vector<String> args;
        for (const auto & [arg_begin, arg_end] : splitTopLevelCommas(open + 1, close))
            args.push_back(translateSubRange(arg_begin, arg_end, /*type_context=*/ false));

        if (args.empty())
            throwNotSupported(unnest_token, "UNNEST without arguments", "");

        size_t j = close + 1;

        bool with_ordinality = false;
        if (isKeywordAt(j, "WITH") && isKeywordAt(j + 1, "ORDINALITY"))
        {
            with_ordinality = true;
            j += 2;
        }

        String table_alias;
        std::vector<String> columns;
        {
            size_t k = j;
            bool has_as = isKeywordAt(k, "AS");
            if (has_as)
                ++k;
            /// Without the AS keyword, a bare word after UNNEST(...) is treated as
            /// a table alias only when it is followed by a column alias list; this
            /// avoids mistaking clause keywords (OFFSET, WINDOW, ...) for aliases.
            bool looks_like_alias = k < end_idx
                && (tokens[k].type == TokenType::BareWord || tokens[k].type == TokenType::QuotedIdentifier)
                && !tokenIsKeyword(tokens[k], "ON")
                && (has_as || isTypeAt(k + 1, TokenType::OpeningRoundBracket));
            if (looks_like_alias)
            {
                table_alias = String(tokens[k].begin, tokens[k].size());
                ++k;
                if (k < end_idx && tokens[k].type == TokenType::OpeningRoundBracket)
                {
                    size_t cols_close = findMatchingParen(k);
                    for (const auto & [col_begin, col_end] : splitTopLevelCommas(k + 1, cols_close))
                    {
                        if (col_end != col_begin + 1
                            || (tokens[col_begin].type != TokenType::BareWord && tokens[col_begin].type != TokenType::QuotedIdentifier))
                            throwNotSupported(tokens[col_begin], "This column alias of UNNEST", "");
                        columns.emplace_back(tokens[col_begin].begin, tokens[col_begin].size());
                    }
                    k = cols_close + 1;
                }
                j = k;
            }
        }

        /// LEFT JOIN UNNEST(...) ON TRUE and INNER JOIN UNNEST(...) ON TRUE: swallow the trivial condition.
        if (isKeywordAt(j, "ON"))
        {
            if (isKeywordAt(j + 1, "TRUE"))
                j += 2;
            else
                throwNotSupported(tokens[j], "JOIN UNNEST with a non-trivial ON condition", "Only ON TRUE is translated.");
        }

        if (columns.empty())
            throwNotSupported(
                unnest_token, "UNNEST without column aliases", "Specify them explicitly, e.g. UNNEST(expr) AS t (c).");

        changed = true;
        i = j;

        size_t n_args = args.size();
        size_t n_columns = columns.size();
        bool is_map = false;

        if (with_ordinality && n_columns == n_args + 1)
        {
            /// Arrays with ordinality.
        }
        else if (!with_ordinality && n_columns == n_args)
        {
            /// Arrays.
        }
        else if (n_args == 1 && (n_columns == (with_ordinality ? 3 : 2)))
        {
            /// A single map argument produces two columns (key, value).
            is_map = true;
        }
        else
            throwNotSupported(
                unnest_token,
                "UNNEST with this combination of arguments and column aliases",
                "The number of column aliases must match the number of array arguments "
                "(a single map argument produces two columns, WITH ORDINALITY adds one more).");

        if (kind == UnnestKind::Standalone)
        {
            emitStandaloneUnnest(args, columns, with_ordinality, is_map, table_alias);
            return;
        }

        /// ARRAY JOIN attaches to the table on the left.
        out += (kind == UnnestKind::LeftArrayJoin) ? "LEFT ARRAY JOIN " : "ARRAY JOIN ";

        std::vector<String> items;
        if (is_map)
        {
            items.push_back("mapKeys(" + args[0] + ") AS " + columns[0]);
            items.push_back("mapValues(" + args[0] + ") AS " + columns[1]);
            if (with_ordinality)
                items.push_back("arrayEnumerate(mapKeys(" + args[0] + ")) AS " + columns[2]);
        }
        else
        {
            for (size_t k = 0; k < n_args; ++k)
                items.push_back(args[k] + " AS " + columns[k]);
            if (with_ordinality)
                items.push_back("arrayEnumerate(" + args[0] + ") AS " + columns.back());
        }

        for (size_t k = 0; k < items.size(); ++k)
        {
            if (k > 0)
                out += ", ";
            out += items[k];
        }
        out.push_back(' ');
    }

    /// FROM UNNEST(...) with no table on the left: translated into a subquery.
    void emitStandaloneUnnest(
        const std::vector<String> & args,
        const std::vector<String> & columns,
        bool with_ordinality,
        bool is_map,
        const String & table_alias)
    {
        size_t n_args = args.size();

        if (n_args == 1 && !with_ordinality && !is_map)
        {
            out += "(SELECT arrayJoin(" + args[0] + ") AS " + columns[0] + ")";
        }
        else
        {
            String zip;
            std::vector<String> column_expressions;

            if (is_map)
            {
                zip = "arrayZip(mapKeys(" + args[0] + "), mapValues(" + args[0] + "))";
                column_expressions.push_back("__trino_unnest_elem.1 AS " + columns[0]);
                column_expressions.push_back("__trino_unnest_elem.2 AS " + columns[1]);
            }
            else if (n_args == 1)
            {
                zip = args[0];
                column_expressions.push_back("__trino_unnest_elem AS " + columns[0]);
            }
            else
            {
                zip = "arrayZipUnaligned(";
                for (size_t k = 0; k < n_args; ++k)
                {
                    if (k > 0)
                        zip += ", ";
                    zip += args[k];
                }
                zip += ")";
                for (size_t k = 0; k < n_args; ++k)
                    column_expressions.push_back("__trino_unnest_elem." + std::to_string(k + 1) + " AS " + columns[k]);
            }

            if (with_ordinality)
                column_expressions.push_back(columns.back());

            out += "(SELECT ";
            for (size_t k = 0; k < column_expressions.size(); ++k)
            {
                if (k > 0)
                    out += ", ";
                out += column_expressions[k];
            }
            out += " FROM (SELECT " + zip + " AS __trino_unnest) ARRAY JOIN __trino_unnest AS __trino_unnest_elem";
            if (with_ordinality)
                out += ", arrayEnumerate(__trino_unnest) AS " + columns.back();
            out += ")";
        }

        if (!table_alias.empty())
            out += " AS " + table_alias;
        out.push_back(' ');
    }

    /// OFFSET n [ROW|ROWS] followed by an optional LIMIT or FETCH clause.
    /// ClickHouse requires LIMIT before OFFSET; a standalone OFFSET n works as-is.
    void translateOffset(size_t & i, size_t end_idx)
    {
        size_t j = i + 1;
        if (j >= end_idx || tokens[j].type != TokenType::Number)
        {
            /// Not the Trino form; pass through.
            emitToken(tokens[i]);
            ++i;
            return;
        }
        String count(tokens[j].begin, tokens[j].size());
        ++j;
        bool had_rows = false;
        if (isKeywordAt(j, "ROW") || isKeywordAt(j, "ROWS"))
        {
            had_rows = true;
            ++j;
        }

        if (isKeywordAt(j, "LIMIT"))
        {
            if (isKeywordAt(j + 1, "ALL"))
            {
                changed = true;
                out += "OFFSET " + count + " ";
                i = j + 2;
                return;
            }
            if (j + 1 < end_idx && tokens[j + 1].type == TokenType::Number)
            {
                changed = true;
                String limit(tokens[j + 1].begin, tokens[j + 1].size());
                out += "LIMIT " + limit + " OFFSET " + count + " ";
                i = j + 2;
                return;
            }
        }

        if (isKeywordAt(j, "FETCH") && (isKeywordAt(j + 1, "FIRST") || isKeywordAt(j + 1, "NEXT")))
        {
            size_t fetch_idx = j;
            String fetch_limit;
            bool with_ties = false;
            if (parseFetch(fetch_idx, end_idx, fetch_limit, with_ties))
            {
                changed = true;
                out += "LIMIT " + fetch_limit + (with_ties ? " WITH TIES" : "") + " OFFSET " + count + " ";
                i = fetch_idx;
                return;
            }
        }

        /// Standalone OFFSET n: emit without the ROW/ROWS suffix.
        if (had_rows)
            changed = true;
        out += "OFFSET " + count + " ";
        i = j;
    }

    /// FETCH FIRST|NEXT [n] ROW|ROWS ONLY|WITH TIES. Returns false if the form is not recognized.
    bool parseFetch(size_t & i, size_t end_idx, String & limit, bool & with_ties)
    {
        size_t j = i + 1;
        if (!(isKeywordAt(j, "FIRST") || isKeywordAt(j, "NEXT")))
            return false;
        ++j;
        limit = "1";
        if (j < end_idx && tokens[j].type == TokenType::Number)
        {
            limit = String(tokens[j].begin, tokens[j].size());
            ++j;
        }
        if (!(isKeywordAt(j, "ROW") || isKeywordAt(j, "ROWS")))
            return false;
        ++j;
        if (isKeywordAt(j, "ONLY"))
        {
            with_ties = false;
            ++j;
        }
        else if (isKeywordAt(j, "WITH") && isKeywordAt(j + 1, "TIES"))
        {
            with_ties = true;
            j += 2;
        }
        else
            return false;
        i = j;
        return true;
    }

    void translateFetch(size_t & i, size_t end_idx)
    {
        String limit;
        bool with_ties = false;
        size_t j = i;
        if (!parseFetch(j, end_idx, limit, with_ties))
        {
            emitToken(tokens[i]);
            ++i;
            return;
        }
        changed = true;
        out += "LIMIT " + limit + (with_ties ? " WITH TIES" : "") + " ";
        i = j;
    }

    /// A statement-level VALUES query: VALUES 1, 2 or VALUES (1, 'a'), (2, 'b').
    void translateValuesStatement(size_t & i, size_t end_idx)
    {
        changed = true;
        out += "SELECT * FROM SQLStandardValues(";
        emitValuesRows(i + 1, end_idx);
        out += ") ";
        i = end_idx;
    }

    void emitValuesRows(size_t begin_idx, size_t end_idx)
    {
        auto rows = splitTopLevelCommas(begin_idx, end_idx);
        if (rows.empty())
            throwNotSupported(tokens[begin_idx > 0 ? begin_idx - 1 : 0], "VALUES without rows", "");
        for (size_t k = 0; k < rows.size(); ++k)
        {
            if (k > 0)
                out += ", ";
            String row = translateSubRange(rows[k].first, rows[k].second, /*type_context=*/ false);
            /// Wrap scalar rows: VALUES 1, 2 means two rows of one column.
            bool parenthesized = tokens[rows[k].first].type == TokenType::OpeningRoundBracket
                && findMatchingParen(rows[k].first) + 1 == rows[k].second;
            if (parenthesized)
                out += row;
            else
                out += "(" + row + ")";
        }
    }
};

}

std::optional<String> translateTrinoSyntax(const std::vector<Token> & tokens, const char * begin, const char * end)
{
    Translator translator(tokens, begin, end);
    return translator.run();
}

}
