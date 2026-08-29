#include <Parsers/Trino/TrinoSyntaxTranslator.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <base/defines.h>

#include <Poco/String.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <map>
#include <set>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
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

/// The target ClickHouse type for a Trino typed literal (TYPE 'value'), or
/// nullptr when the word is not a supported literal type prefix. DECIMAL,
/// TIMESTAMP, JSON, DATE and INTERVAL are handled elsewhere.
const char * getTypedLiteralTarget(const Token & token)
{
    static const std::pair<const char *, const char *> types[] =
    {
        {"BIGINT", "BIGINT"},
        {"INTEGER", "INTEGER"},
        {"INT", "INTEGER"},
        {"SMALLINT", "SMALLINT"},
        {"TINYINT", "TINYINT"},
        {"REAL", "REAL"},
        {"DOUBLE", "DOUBLE"},
        {"VARCHAR", "String"},
        {"CHAR", "String"},
        {"BOOLEAN", "BOOLEAN"},
        {"UUID", "UUID"},
        {"VARBINARY", "String"},
        {"IPADDRESS", "IPv6"},
    };
    for (const auto & [name, target] : types)
        if (tokenIsKeyword(token, name))
            return target;
    return nullptr;
}

/// The kind of construct that precedes UNNEST and determines the translation.
enum class UnnestKind : uint8_t
{
    ArrayJoin,        /// CROSS JOIN UNNEST, INNER JOIN UNNEST, implicit comma join
    LeftArrayJoin,    /// LEFT [OUTER] JOIN UNNEST ... ON TRUE
    Standalone,       /// FROM UNNEST(...) - no table on the left
};

/// The column aliases of a joined UNNEST, by the query scope that introduced
/// them and the table alias: `CROSS JOIN UNNEST(a) AS t (x)` binds `t.x`.
/// The scope is the token index of the opening parenthesis of the innermost
/// enclosing subquery (or SIZE_MAX at the top level), so that a nested
/// subquery or CTE reusing the same table alias is not affected.
using JoinedUnnestAliases = std::map<std::pair<size_t, String>, std::set<String>>;

class Translator
{
public:
    Translator(
        const std::vector<Token> & tokens_,
        const char * source_begin_,
        const char * source_end_,
        JoinedUnnestAliases known_unnest_aliases_ = {})
        : tokens(tokens_)
        , source_begin(source_begin_)
        , source_end(source_end_)
        , known_unnest_aliases(std::move(known_unnest_aliases_))
    {
        /// For every token, the opening parenthesis of the innermost enclosing
        /// subquery: a '(' immediately followed by SELECT, WITH or VALUES.
        enclosing_subquery_scope.resize(tokens.size(), TOP_LEVEL_SCOPE);
        std::vector<size_t> scope_stack{TOP_LEVEL_SCOPE};
        for (size_t j = 0; j < tokens.size(); ++j)
        {
            if (tokens[j].type == TokenType::ClosingRoundBracket && scope_stack.size() > 1)
                scope_stack.pop_back();
            enclosing_subquery_scope[j] = scope_stack.back();
            if (tokens[j].type == TokenType::OpeningRoundBracket)
            {
                bool is_subquery = j + 1 < tokens.size()
                    && (tokenIsKeyword(tokens[j + 1], "SELECT") || tokenIsKeyword(tokens[j + 1], "WITH")
                        || tokenIsKeyword(tokens[j + 1], "VALUES"));
                scope_stack.push_back(is_subquery ? j : scope_stack.back());
            }
        }
    }

    const JoinedUnnestAliases & getJoinedUnnestAliases() const { return joined_unnest_aliases; }

    std::optional<String> run()
    {
        size_t body_begin = 0;
        size_t clause_begin = 0;
        if (findTrailingClauseAfterSetOperation(body_begin, clause_begin))
        {
            /// In Trino a trailing ORDER BY/LIMIT/OFFSET/FETCH applies to the whole
            /// set operation, while in ClickHouse it binds to the last SELECT:
            /// wrap the set operation into a subquery.
            changed = true;
            translateRange(0, body_begin, /*type_context=*/ false);
            out += "SELECT * FROM ( ";
            translateRange(body_begin, clause_begin, /*type_context=*/ false);
            out += ") ";
            translateRange(clause_begin, tokens.size(), /*type_context=*/ false);
        }
        else
            translateRange(0, tokens.size(), /*type_context=*/ false);

        /// The settings that align query semantics with Trino (`join_use_nulls`,
        /// `use_variant_as_common_type`, `enable_analyzer`) are applied to the query
        /// context instead of the query text, so that they also hold for statements
        /// that carry their own `SETTINGS` clause and for wrappers such as
        /// `INSERT ... SELECT` or `EXPLAIN SELECT`. See `executeQuery.cpp`.

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

    /// Filled while translating; seeded from a previous pass so that the
    /// qualified references in the SELECT list (which precede the FROM clause)
    /// are rewritten too.
    JoinedUnnestAliases joined_unnest_aliases;
    JoinedUnnestAliases known_unnest_aliases;

    static constexpr size_t TOP_LEVEL_SCOPE = std::numeric_limits<size_t>::max();
    std::vector<size_t> enclosing_subquery_scope;

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

    /// Returns whether the expression starting at token `idx` (the first
    /// argument of a function call) is syntactically recognizable as VARBINARY:
    /// a call to a binary-producing function, a CAST/TRY_CAST to VARBINARY, or
    /// a byte-preserving string function over such an expression.
    bool isBinaryExpressionAt(size_t idx) const
    {
        while (idx < tokens.size() && tokens[idx].type == TokenType::OpeningRoundBracket)
            ++idx;
        if (idx >= tokens.size() || tokens[idx].type != TokenType::BareWord || !isTypeAt(idx + 1, TokenType::OpeningRoundBracket))
            return false;
        const Token & token = tokens[idx];

        static constexpr std::string_view binary_producers[] =
        {
            "to_utf8", "from_hex", "from_base64", "from_base64url", "from_big_endian_32", "from_big_endian_64",
            "to_ieee754_32", "to_ieee754_64", "md5", "sha1", "sha256", "sha512",
            "hmac_md5", "hmac_sha1", "hmac_sha256", "hmac_sha512",
            "spooky_hash_v2_32", "spooky_hash_v2_64", "xxhash64", "murmur3",
        };
        for (std::string_view name : binary_producers)
            if (tokenIsKeyword(token, name))
                return true;

        /// Byte-preserving functions: binary in, binary out.
        static constexpr std::string_view binary_preserving[] = {"substr", "substring", "lpad", "rpad", "reverse", "concat"};
        for (std::string_view name : binary_preserving)
            if (tokenIsKeyword(token, name))
                return isBinaryExpressionAt(idx + 2);

        if (tokenIsKeyword(token, "CAST") || tokenIsKeyword(token, "TRY_CAST"))
        {
            const size_t open = idx + 1;
            const size_t close = findMatchingParen(open);
            if (close == tokens.size())
                return false;
            /// The last top-level AS separates the expression from the type.
            size_t depth = 0;
            size_t as_idx = tokens.size();
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
            return as_idx != tokens.size() && isKeywordAt(as_idx + 1, "VARBINARY");
        }

        return false;
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

        /// Row field expansion of a parenthesized expression: (expr).* -> untuple(expr).
        /// (The primary must not be function-call arguments, so a preceding
        /// identifier-like token disables the rule.)
        if (token.type == TokenType::OpeningRoundBracket && isExpressionStartContext(i))
        {
            size_t close = findMatchingParen(i);
            if (close != tokens.size() && isTypeAt(close + 1, TokenType::Dot) && isTypeAt(close + 2, TokenType::Asterisk))
            {
                if (isKeywordAt(close + 3, "AS"))
                    throwNotSupported(tokens[close + 1], "Row field expansion .* with column aliases", "");
                changed = true;
                String inner = translateSubRange(i + 1, close, /*type_context=*/ false);
                /// (1, 2).* is a tuple denoted by the parentheses themselves.
                if (splitTopLevelCommas(i + 1, close).size() > 1)
                    inner = "tuple(" + inner + ")";
                out += "untuple(" + inner + ") ";
                i = close + 3;
                return;
            }
        }

        /// Row field expansion of other primaries is not detectable at the token
        /// level; a qualified asterisk (t.*) passes through to ClickHouse, and
        /// everything else must fail loudly rather than lose the expression.
        if (token.type == TokenType::Dot && isTypeAt(i + 1, TokenType::Asterisk))
        {
            if (i > 0 && (tokens[i - 1].type == TokenType::BareWord || tokens[i - 1].type == TokenType::QuotedIdentifier))
            {
                emitToken(token);
                ++i;
                return;
            }
            throwNotSupported(token, "Row field expansion with .*", "Parenthesize the expression: (expr).*");
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

        /// A qualified reference to a joined UNNEST alias: `t.x` -> `x`. The
        /// aliases of a ClickHouse ARRAY JOIN are unqualified, so the table
        /// qualifier would not resolve after the translation.
        if (!type_context && !known_unnest_aliases.empty()
            && (token.type == TokenType::BareWord || token.type == TokenType::QuotedIdentifier)
            && isTypeAt(i + 1, TokenType::Dot)
            && (isTypeAt(i + 2, TokenType::BareWord) || isTypeAt(i + 2, TokenType::QuotedIdentifier)))
        {
            auto it = known_unnest_aliases.find({enclosing_subquery_scope[i], String(token.begin, token.size())});
            if (it != known_unnest_aliases.end() && it->second.contains(String(tokens[i + 2].begin, tokens[i + 2].size())))
            {
                changed = true;
                emitToken(tokens[i + 2]);
                i += 3;
                return;
            }
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

        /// The VARBINARY overloads of the character functions operate on bytes,
        /// while the VARCHAR overloads count code points (and are mapped to the
        /// UTF8 variants by TrinoFunctionMapper). When the first argument is a
        /// syntactically recognizable VARBINARY expression, the byte-based
        /// ClickHouse function is emitted here instead; its name is left intact
        /// by the function mapper because it is not a Trino function name.
        if (isTypeAt(i + 1, TokenType::OpeningRoundBracket))
        {
            static constexpr std::pair<std::string_view, std::string_view> binary_variants[] =
            {
                {"length", "OCTET_LENGTH"},
                {"substr", "byteSlice"},
                {"substring", "byteSlice"},
                {"lpad", "leftPad"},
                {"rpad", "rightPad"},
            };
            for (const auto & [trino_name, byte_name] : binary_variants)
            {
                if (tokenIsKeyword(token, trino_name) && isBinaryExpressionAt(i + 2))
                {
                    changed = true;
                    emitText(byte_name);
                    ++i;
                    return;
                }
            }
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

        /// Statement-level VALUES (1, 'a'), (2, 'b') -> SELECT * FROM SQLStandardValues((1, 'a'), (2, 'b')).
        /// Also covers VALUES as an arm of a set operation.
        if (tokenIsKeyword(token, "VALUES")
            && (i == 0 || isKeywordAt(i - 1, "ALL") || isKeywordAt(i - 1, "DISTINCT") || isKeywordAt(i - 1, "UNION")
                || isKeywordAt(i - 1, "INTERSECT") || isKeywordAt(i - 1, "EXCEPT")))
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

        /// Trino set operations default to DISTINCT, while ClickHouse requires an
        /// explicit mode (or the union_default_mode setting) in compound set
        /// operations. `* EXCEPT (columns)` is column exclusion and is left alone.
        if ((tokenIsKeyword(token, "UNION") || tokenIsKeyword(token, "INTERSECT") || tokenIsKeyword(token, "EXCEPT"))
            && !isKeywordAt(i + 1, "ALL") && !isKeywordAt(i + 1, "DISTINCT")
            && !(i > 0 && tokens[i - 1].type == TokenType::Asterisk))
        {
            changed = true;
            emitToken(token);
            emitText("DISTINCT");
            ++i;
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

        /// LIKE patterns: Trino has no default escape character, so a backslash in
        /// the pattern is a literal character; ClickHouse treats it as the escape.
        /// (Doubling happens twice: string-literal level and LIKE level.)
        if ((tokenIsKeyword(token, "LIKE") || tokenIsKeyword(token, "ILIKE")) && isTypeAt(i + 1, TokenType::StringLiteral))
        {
            emitToken(token);
            const Token & pattern = tokens[i + 1];
            if (isKeywordAt(i + 2, "ESCAPE") || memchr(pattern.begin, '\\', pattern.size()) == nullptr)
                emitToken(pattern);
            else
            {
                String escaped;
                escaped.reserve(pattern.size() + 16);
                for (const char * c = pattern.begin; c < pattern.end; ++c)
                {
                    if (*c == '\\')
                        escaped += R"(\\\\)";
                    else
                        escaped.push_back(*c);
                }
                changed = true;
                emitText(escaped);
            }
            i += 2;
            return;
        }

        /// Typed literals: BIGINT '1' -> CAST('1' AS BIGINT), VARCHAR 'x' -> CAST('x' AS String), ...
        if (isTypeAt(i + 1, TokenType::StringLiteral))
        {
            if (const char * target = getTypedLiteralTarget(token))
            {
                changed = true;
                out += "CAST(";
                emitToken(tokens[i + 1]);
                out += String("AS ") + target + ") ";
                i += 2;
                return;
            }
        }

        /// GROUP BY AUTO -> GROUP BY ALL (group by all non-aggregated columns).
        if (tokenIsKeyword(token, "AUTO") && i >= 2 && tokenIsKeyword(tokens[i - 1], "BY") && tokenIsKeyword(tokens[i - 2], "GROUP"))
        {
            changed = true;
            emitText("ALL");
            ++i;
            return;
        }

        /// The TABLE t query shorthand -> SELECT * FROM t.
        if (tokenIsKeyword(token, "TABLE")
            && (isTypeAt(i + 1, TokenType::BareWord) || isTypeAt(i + 1, TokenType::QuotedIdentifier))
            && (i == 0 || tokens[i - 1].type == TokenType::OpeningRoundBracket || isKeywordAt(i - 1, "UNION")
                || isKeywordAt(i - 1, "INTERSECT") || isKeywordAt(i - 1, "EXCEPT") || isKeywordAt(i - 1, "ALL")
                || isKeywordAt(i - 1, "DISTINCT")))
        {
            changed = true;
            emitText("SELECT * FROM");
            ++i;
            return;
        }

        /// Aggregates with an inline ORDER BY or a WITHIN GROUP clause.
        if (isTypeAt(i + 1, TokenType::OpeningRoundBracket) && translateOrderedAggregate(i, end_idx))
            return;

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
        if (parts.size() < 2 || parts.size() > 3 || text.contains('\'') || text.contains('\\'))
            return false;

        size_t scale = 0;
        if (size_t dot = parts[1].find('.'); dot != String::npos)
            scale = std::min<size_t>(9, parts[1].size() - dot - 1);

        /// Minute-precision literals (TIMESTAMP '2012-08-08 01:00') need the seconds.
        bool added_seconds = false;
        if (std::count(parts[1].begin(), parts[1].end(), ':') == 1)
        {
            parts[1] += ":00";
            added_seconds = true;
        }

        bool has_named_zone = parts.size() == 3 && isAlphaASCII(parts[2][0]);
        bool has_offset = parts.size() == 3 && !has_named_zone;

        if (scale == 0 && !has_named_zone && !added_seconds)
            return false;

        changed = true;
        if (has_named_zone)
            out += "toDateTime64('" + parts[0] + " " + parts[1] + "', " + std::to_string(scale) + ", '" + parts[2] + "') ";
        else if (has_offset)
            out += "parseDateTime64BestEffort('" + parts[0] + " " + parts[1] + " " + parts[2] + "', " + std::to_string(scale) + ") ";
        else
            out += "toDateTime64('" + parts[0] + " " + parts[1] + "', " + std::to_string(scale) + ") ";
        i += 2;
        return true;
    }

    /// Detects a query of the form `<set operation> ORDER BY/LIMIT/OFFSET/FETCH ...`
    /// (optionally preceded by a WITH clause). Sets `body_begin` to the start of
    /// the set operation and `clause_begin` to the first trailing clause.
    bool findTrailingClauseAfterSetOperation(size_t & body_begin, size_t & clause_begin) const
    {
        if (tokens.empty())
            return false;

        size_t begin = 0;
        if (tokenIsKeyword(tokens[0], "WITH"))
        {
            /// The body is the first top-level SELECT or VALUES after the CTE list
            /// (the CTE definitions themselves are inside parentheses).
            size_t depth = 0;
            bool found = false;
            for (size_t j = 1; j < tokens.size(); ++j)
            {
                TokenType type = tokens[j].type;
                if (type == TokenType::OpeningRoundBracket)
                    ++depth;
                else if (type == TokenType::ClosingRoundBracket)
                {
                    if (depth > 0)
                        --depth;
                }
                else if (depth == 0 && (tokenIsKeyword(tokens[j], "SELECT") || tokenIsKeyword(tokens[j], "VALUES")))
                {
                    begin = j;
                    found = true;
                    break;
                }
            }
            if (!found)
                return false;
        }
        else if (!(tokenIsKeyword(tokens[0], "SELECT") || tokenIsKeyword(tokens[0], "VALUES")
                   || tokens[0].type == TokenType::OpeningRoundBracket))
            return false;

        size_t depth = 0;
        size_t last_set_operation = tokens.size();
        for (size_t j = begin; j < tokens.size(); ++j)
        {
            TokenType type = tokens[j].type;
            if (type == TokenType::OpeningRoundBracket || type == TokenType::OpeningSquareBracket)
                ++depth;
            else if (type == TokenType::ClosingRoundBracket || type == TokenType::ClosingSquareBracket)
            {
                if (depth > 0)
                    --depth;
            }
            else if (depth == 0
                && (tokenIsKeyword(tokens[j], "UNION") || tokenIsKeyword(tokens[j], "INTERSECT") || tokenIsKeyword(tokens[j], "EXCEPT"))
                && !(j > 0 && tokens[j - 1].type == TokenType::Asterisk))
                last_set_operation = j;
        }
        if (last_set_operation == tokens.size())
            return false;

        depth = 0;
        for (size_t j = last_set_operation + 1; j < tokens.size(); ++j)
        {
            TokenType type = tokens[j].type;
            if (type == TokenType::OpeningRoundBracket || type == TokenType::OpeningSquareBracket)
                ++depth;
            else if (type == TokenType::ClosingRoundBracket || type == TokenType::ClosingSquareBracket)
            {
                if (depth > 0)
                    --depth;
            }
            else if (depth == 0
                && ((tokenIsKeyword(tokens[j], "ORDER") && isKeywordAt(j + 1, "BY")) || tokenIsKeyword(tokens[j], "LIMIT")
                    || tokenIsKeyword(tokens[j], "OFFSET") || tokenIsKeyword(tokens[j], "FETCH")))
            {
                body_begin = begin;
                clause_begin = j;
                return true;
            }
        }
        return false;
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

    /// Whether the token at `idx` begins a primary expression (as opposed to
    /// being function-call arguments or a subscript), judged by the preceding token.
    bool isExpressionStartContext(size_t idx) const
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
                for (const auto * keyword : {"SELECT", "AS", "BY", "AND", "OR", "NOT", "WHEN", "THEN", "ELSE",
                                             "WHERE", "HAVING", "ON", "IN", "ALL", "DISTINCT", "UNION", "EXCEPT", "INTERSECT"})
                    if (tokenIsKeyword(prev, keyword))
                        return true;
                return false;
            default:
                return false;
        }
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
        out += "(";
        for (size_t j = i; j <= arrow; ++j)
            emitToken(tokens[j]);
        translateRange(arrow + 1, body_end, /*type_context=*/ false);
        out += ") ";
        i = body_end;
        return true;
    }

    /// agg(x ORDER BY k [DESC]) and listagg(x, sep) WITHIN GROUP (ORDER BY k):
    /// ClickHouse has no ordered aggregates. array_agg and listagg are rewritten
    /// through arraySort over (value, key) tuples; for order-insensitive
    /// aggregates the clause is simply dropped. `i` points at the function name.
    bool translateOrderedAggregate(size_t & i, size_t end_idx)
    {
        static constexpr std::string_view order_insensitive[] =
            {"sum", "count", "avg", "min", "max", "count_if", "bool_and", "bool_or", "every", "arbitrary",
             "any_value", "approx_distinct", "geometric_mean", "checksum", "stddev", "stddev_samp", "stddev_pop",
             "variance", "var_samp", "var_pop"};

        const Token & name = tokens[i];
        bool is_array_agg = tokenIsKeyword(name, "ARRAY_AGG");
        bool is_listagg = tokenIsKeyword(name, "LISTAGG");
        bool is_insensitive = false;
        for (const auto & candidate : order_insensitive)
            is_insensitive |= tokenIsKeyword(name, candidate);
        if (!is_array_agg && !is_listagg && !is_insensitive)
            return false;

        size_t open = i + 1;
        size_t close = findMatchingParen(open);
        if (close == tokens.size())
            return false;

        /// A top-level ORDER BY inside the call.
        size_t order_idx = close;
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
            else if (depth == 0 && tokenIsKeyword(tokens[j], "ORDER") && isKeywordAt(j + 1, "BY"))
            {
                order_idx = j;
                break;
            }
        }

        bool within_group = isKeywordAt(close + 1, "WITHIN") && isKeywordAt(close + 2, "GROUP")
            && isTypeAt(close + 3, TokenType::OpeningRoundBracket);

        if (order_idx == close && !within_group)
            return false;

        size_t args_end = order_idx;
        size_t order_begin = 0;
        size_t order_end = 0;
        size_t consumed_end = 0;
        if (within_group)
        {
            size_t wg_close = findMatchingParen(close + 3);
            if (order_idx != close || wg_close == tokens.size() || !isKeywordAt(close + 4, "ORDER") || !isKeywordAt(close + 5, "BY"))
                throwNotSupported(name, "This WITHIN GROUP clause", "");
            order_begin = close + 6;
            order_end = wg_close;
            consumed_end = wg_close + 1;
        }
        else
        {
            order_begin = order_idx + 2;
            order_end = close;
            consumed_end = close + 1;
        }

        if (isKeywordAt(open + 1, "DISTINCT"))
            throwNotSupported(name, "DISTINCT together with ORDER BY in an aggregate", "");

        /// A single ordering key: expr [ASC|DESC] [NULLS FIRST|LAST].
        if (splitTopLevelCommas(order_begin, order_end).size() != 1)
            throwNotSupported(name, "Multiple ORDER BY keys in an aggregate", "");
        size_t key_end = order_end;
        if (key_end >= order_begin + 2 && isKeywordAt(key_end - 2, "NULLS")
            && (isKeywordAt(key_end - 1, "FIRST") || isKeywordAt(key_end - 1, "LAST")))
            key_end -= 2;
        bool descending = false;
        if (key_end > order_begin && (isKeywordAt(key_end - 1, "ASC") || isKeywordAt(key_end - 1, "DESC")))
        {
            descending = tokenIsKeyword(tokens[key_end - 1], "DESC");
            --key_end;
        }
        if (key_end == order_begin)
            throwNotSupported(name, "This ORDER BY in an aggregate", "");
        String key = translateSubRange(order_begin, key_end, /*type_context=*/ false);

        /// Trailing FILTER (WHERE ...) and OVER clauses attach to the rewritten aggregate.
        String tail;
        if (isKeywordAt(consumed_end, "FILTER") && isTypeAt(consumed_end + 1, TokenType::OpeningRoundBracket))
        {
            size_t filter_close = findMatchingParen(consumed_end + 1);
            if (filter_close == tokens.size())
                throwNotSupported(name, "This FILTER clause", "");
            tail += " " + translateSubRange(consumed_end, filter_close + 1, /*type_context=*/ false);
            consumed_end = filter_close + 1;
        }
        if (isKeywordAt(consumed_end, "OVER"))
        {
            size_t over_end = 0;
            if (isTypeAt(consumed_end + 1, TokenType::OpeningRoundBracket))
                over_end = findMatchingParen(consumed_end + 1) + 1;
            else if (isTypeAt(consumed_end + 1, TokenType::BareWord) || isTypeAt(consumed_end + 1, TokenType::QuotedIdentifier))
                over_end = consumed_end + 2;
            else
                throwNotSupported(name, "This OVER clause", "");
            tail += " " + translateSubRange(consumed_end, over_end, /*type_context=*/ false);
            consumed_end = over_end;
        }

        changed = true;
        const char * sort_function = descending ? "arrayReverseSort" : "arraySort";

        if (is_insensitive)
        {
            out += String(name.begin, name.size()) + "(" + translateSubRange(open + 1, args_end, /*type_context=*/ false) + ")"
                + tail + " ";
            i = consumed_end;
            return true;
        }

        auto arguments = splitTopLevelCommas(open + 1, args_end);
        if (arguments.empty() || arguments.size() > (is_listagg ? 2 : 1))
            throwNotSupported(name, "This combination of arguments and ORDER BY in an aggregate", "");
        for (const auto & [arg_begin, arg_end_idx] : arguments)
            for (size_t j = arg_begin; j < arg_end_idx; ++j)
                if (tokenIsKeyword(tokens[j], "ON"))
                    throwNotSupported(name, "The ON OVERFLOW clause of listagg", "");

        String value = translateSubRange(arguments[0].first, arguments[0].second, /*type_context=*/ false);
        String sorted_values = "arrayMap(__trino_x -> (__trino_x).1, " + String(sort_function)
            + "(__trino_x -> (__trino_x).2, groupArray(tuple(" + value + ", " + key + "))" + tail + "))";

        if (is_listagg)
        {
            String separator = arguments.size() == 2
                ? translateSubRange(arguments[1].first, arguments[1].second, /*type_context=*/ false)
                : "''";
            out += "arrayStringConcat(" + sorted_values + ", " + separator + ") ";
        }
        else
            out += sorted_values + " ";

        i = consumed_end;
        UNUSED(end_idx);
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
            if (type.contains('\''))
                throwNotSupported(tokens[i], "TRY_CAST to this type", "");
            changed = true;
            out += "accurateCastOrNull(" + expression + ", '" + type + "') ";
        }
        else
        {
            /// All Trino types are nullable, so scalar cast targets are wrapped
            /// in Nullable (composite ClickHouse types cannot be Nullable).
            String first_word;
            for (char c : type)
            {
                if (!isWordCharASCII(c))
                    break;
                first_word += c;
            }
            String upper_word = Poco::toUpper(first_word);
            bool wrap_nullable = upper_word != "NULLABLE" && upper_word != "ARRAY" && upper_word != "TUPLE" && upper_word != "MAP"
                && upper_word != "ROW" && upper_word != "JSON" && !upper_word.empty();
            if (wrap_nullable)
            {
                changed = true;
                out += "CAST(" + expression + " AS Nullable(" + type + ")) ";
            }
            else
                out += "CAST(" + expression + " AS " + type + ") ";
        }
        i = close + 1;
    }

    /// UNNEST(e1, e2, ...) [WITH ORDINALITY] [AS] [alias] [(c1, c2, ...)] [ON TRUE]
    /// `i` points at the UNNEST token.
    void translateUnnest(size_t & i, size_t end_idx, UnnestKind kind)
    {
        const Token & unnest_token = tokens[i];
        const size_t unnest_scope = enclosing_subquery_scope[i];
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
        {
            /// In the standalone form the columns can be given synthesized names
            /// (each argument is assumed to be an array). In the join form the
            /// unnested columns would not be reachable (`SELECT *` does not
            /// include ARRAY JOIN aliases), so explicit aliases are required.
            if (kind != UnnestKind::Standalone)
                throwNotSupported(
                    unnest_token, "UNNEST without column aliases", "Specify them explicitly, e.g. UNNEST(expr) AS t (c).");
            for (size_t k = 0; k < args.size(); ++k)
                columns.push_back("__trino_unnest_col" + std::to_string(k + 1));
            if (with_ordinality)
                columns.push_back("__trino_unnest_ordinality");
        }

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

        /// The column aliases of a joined UNNEST are bound to its table alias in
        /// Trino (`t.x`), while an ARRAY JOIN alias is unqualified: remember the
        /// alias so that the qualified references are rewritten (see translateOne).
        if (!table_alias.empty())
            joined_unnest_aliases[{unnest_scope, table_alias}].insert(columns.begin(), columns.end());

        /// ARRAY JOIN attaches to the table on the left.
        const bool is_left = kind == UnnestKind::LeftArrayJoin;
        out += is_left ? "LEFT ARRAY JOIN " : "ARRAY JOIN ";

        /// `LEFT ARRAY JOIN` over an empty array emits the default value of the
        /// element type, while Trino emits NULL: making the elements Nullable
        /// turns that default into NULL. Element types that cannot be wrapped
        /// into `Nullable` (arrays, tuples, maps) are rejected by the analyzer -
        /// there is no way to represent the Trino result for them.
        auto nullable_if_left = [is_left](const String & array) -> String
        {
            if (!is_left)
                return array;
            return "arrayMap(__trino_e -> toNullable(__trino_e), " + array + ")";
        };

        /// One array per unnested column, all of the same length.
        std::vector<String> unnested;
        if (is_map)
        {
            unnested.push_back("mapKeys(" + args[0] + ")");
            unnested.push_back("mapValues(" + args[0] + ")");
        }
        else if (n_args == 1)
        {
            unnested.push_back(args[0]);
        }
        else
        {
            /// Trino zips several arrays of different lengths, padding the
            /// shorter ones with NULLs, so the raw arrays cannot be joined
            /// directly (that would require them to be aligned).
            String zip = "arrayZipUnaligned(";
            for (size_t k = 0; k < n_args; ++k)
            {
                if (k > 0)
                    zip += ", ";
                zip += args[k];
            }
            zip += ")";
            for (size_t k = 0; k < n_args; ++k)
                unnested.push_back("arrayMap(__trino_z -> (__trino_z)." + std::to_string(k + 1) + ", " + zip + ")");
        }

        std::vector<String> items;
        for (size_t k = 0; k < unnested.size(); ++k)
            items.push_back(nullable_if_left(unnested[k]) + " AS " + columns[k]);
        if (with_ordinality)
            items.push_back(nullable_if_left("arrayEnumerate(" + unnested[0] + ")") + " AS " + columns.back());

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
    /// The rows end at a set operation or a trailing clause, if any.
    void translateValuesStatement(size_t & i, size_t end_idx)
    {
        size_t rows_end = end_idx;
        size_t depth = 0;
        for (size_t j = i + 1; j < end_idx; ++j)
        {
            TokenType type = tokens[j].type;
            if (type == TokenType::OpeningRoundBracket || type == TokenType::OpeningSquareBracket)
                ++depth;
            else if (type == TokenType::ClosingRoundBracket || type == TokenType::ClosingSquareBracket)
            {
                if (depth > 0)
                    --depth;
            }
            else if (depth == 0 && type == TokenType::BareWord)
            {
                bool is_terminator = false;
                for (const auto * keyword : {"UNION", "INTERSECT", "EXCEPT", "ORDER", "LIMIT", "OFFSET", "FETCH"})
                    is_terminator |= tokenIsKeyword(tokens[j], keyword);
                if (is_terminator)
                {
                    rows_end = j;
                    break;
                }
            }
        }

        changed = true;
        out += "SELECT * FROM SQLStandardValues(";
        emitValuesRows(i + 1, rows_end);
        out += ") ";
        i = rows_end;
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
            size_t row_begin = rows[k].first;
            size_t row_end = rows[k].second;
            /// An explicit ROW constructor is the row itself: VALUES ROW(1, 'a')
            /// must not become a single-column row holding a tuple.
            if (tokenIsKeyword(tokens[row_begin], "ROW") && isTypeAt(row_begin + 1, TokenType::OpeningRoundBracket)
                && findMatchingParen(row_begin + 1) + 1 == row_end)
            {
                changed = true;
                out += "(" + translateSubRange(row_begin + 2, row_end - 1, /*type_context=*/ false) + ")";
                continue;
            }
            String row = translateSubRange(row_begin, row_end, /*type_context=*/ false);
            /// Wrap scalar rows: VALUES 1, 2 means two rows of one column.
            bool parenthesized = tokens[row_begin].type == TokenType::OpeningRoundBracket
                && findMatchingParen(row_begin) + 1 == row_end;
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
    std::optional<String> result = translator.run();

    /// A joined UNNEST binds its column aliases to a table alias (`AS t (x)`),
    /// but the ClickHouse `ARRAY JOIN` aliases are unqualified. The qualified
    /// references can appear before the FROM clause, so the aliases discovered
    /// by the first pass are fed into a second one.
    if (!translator.getJoinedUnnestAliases().empty())
    {
        Translator second_pass(tokens, begin, end, translator.getJoinedUnnestAliases());
        result = second_pass.run();
    }

    return result;
}

}
