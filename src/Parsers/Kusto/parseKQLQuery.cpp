#include <Parsers/Kusto/parseKQLQuery.h>

#include <Parsers/Kusto/KQLLexer.h>
#include <Parsers/Kusto/KQLParser.h>
#include <Parsers/Kusto/KQLTranslator.h>

#include <Parsers/ParserSetQuery.h>
#include <Parsers/TokenIterator.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>

#include <Poco/String.h>


namespace DB
{

namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
extern const int QUERY_IS_TOO_LARGE;
}

namespace
{

/// `SET` is not a KQL statement, but the dialect has to be switchable from inside it -
/// otherwise a session that turned KQL on could never turn it off. Recognized only in the
/// unambiguous `SET name =` shape, so a table called `set` still works.
bool looksLikeSetStatement(const std::vector<KQLToken> & tokens)
{
    return tokens.size() >= 3 && tokens[0].type == KQLTokenType::BareWord && Poco::toLower(String(tokens[0].text())) == "set"
        && tokens[1].type == KQLTokenType::BareWord && tokens[2].type == KQLTokenType::Equals;
}

/// Where the next statement starts, looking past the separator and everything insignificant after
/// it: whitespace, and comments in every spelling the lexer accepts. `end` when there is no next
/// statement - a trailing `// switch back` is not one.
///
/// Only ever used to *look*: the caller's `pos` must stay in front of a trailing comment, because
/// the client takes the text of the statement it just parsed as everything up to `pos`, and a
/// comment swallowed into it counts against `max_query_size`.
const char * afterStatementSeparator(const char * pos, const char * end)
{
    const std::vector<KQLToken> tokens = KQLLexer(pos, end).tokenize();
    size_t index = 0;
    while (index < tokens.size() && tokens[index].type == KQLTokenType::Semicolon)
        ++index;
    return index < tokens.size() ? tokens[index].begin : end;
}

/// `pos` stands on the `set` word.
ASTPtr parseSetStatement(const char *& pos, const char * end, size_t max_query_size, size_t max_parser_depth, size_t max_parser_backtracks)
{
    Tokens sql_tokens(pos, end, max_query_size, /*skip_insignificant=*/true);
    IParser::Pos token_iterator(sql_tokens, static_cast<uint32_t>(max_parser_depth), static_cast<uint32_t>(max_parser_backtracks));

    ParserSetQuery set_parser;
    ASTPtr node;
    Expected expected;
    if (set_parser.parse(token_iterator, node, expected))
    {
        pos = token_iterator->begin;
        /// Consume the statement separator so the caller resumes on the next statement. A comment
        /// after it is left alone: the main path treats a leading comment the same way.
        while (pos < end && (*pos == ';' || isWhitespaceASCII(*pos)))
            ++pos;
        return node;
    }
    throw Exception(ErrorCodes::SYNTAX_ERROR, "Cannot parse the SET statement");
}

}

ASTPtr parseKQLQuery(
    const char *& pos,
    const char * end,
    bool allow_multi_statements,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks)
{
    if (max_query_size && static_cast<size_t>(end - pos) > max_query_size)
        throw Exception(
            ErrorCodes::QUERY_IS_TOO_LARGE, "Query is too large ({} bytes), maximum is {} bytes", end - pos, max_query_size);

    /// `end` is the end of the whole script, not of this statement, so the token stream may
    /// run past the statement being parsed. A lexical error is therefore *not* raised here:
    /// it is left in the stream as an `Error` token for the parser to trip over only if it
    /// actually reads that far. Raising eagerly would fail statement 1 because statement 20
    /// contains a bad literal.
    KQLLexer lexer(pos, end);
    std::vector<KQLToken> tokens = lexer.tokenize();

    if (tokens.size() == 1 && tokens.front().isEnd())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Empty query");

    /// Move `pos` to the first real token before parsing anything. On failure the caller uses
    /// `pos` to find the line the statement is on - to skip it, and to pick up a trailing
    /// `-- { clientError ... }` test hint. Leaving `pos` on the whitespace or comment that
    /// preceded the statement would point that recovery at the wrong line.
    pos = tokens.front().begin;

    if (looksLikeSetStatement(tokens))
    {
        ASTPtr set_query = parseSetStatement(pos, end, max_query_size, max_parser_depth, max_parser_backtracks);
        /// `parseSetStatement` already consumed the statement separator, so anything left is
        /// a second statement, which this fast path must reject the same way the main path does.
        if (!allow_multi_statements && afterStatementSeparator(pos, end) != end)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Multi-statements are not allowed: unexpected text after the SET statement");
        return set_query;
    }

    KQLParser parser(pos, std::move(tokens), max_parser_depth);
    KQLTabularExpressionPtr query = parser.parseQuery();
    ASTPtr result = translateKQLQuery(*query);

    pos = parser.getEndPosition();

    if (!allow_multi_statements)
    {
        /// Anything left over would otherwise be dropped without a word.
        const char * rest = afterStatementSeparator(pos, end);
        if (rest != end)
            throw Exception(
                ErrorCodes::SYNTAX_ERROR,
                "Multi-statements are not allowed: unexpected text at position {}",
                rest - pos + 1);
    }

    return result;
}

ASTPtr tryParseKQLSetStatement(
    const char *& pos,
    const char * end,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks)
{
    KQLLexer lexer(pos, end);
    std::vector<KQLToken> tokens = lexer.tokenize();

    if (!looksLikeSetStatement(tokens))
        return nullptr;

    const char * probe = tokens.front().begin;
    ASTPtr node = parseSetStatement(probe, end, max_query_size, max_parser_depth, max_parser_backtracks);

    /// The escape hatch fires only when the whole query is that one SET statement; anything
    /// after it would be dropped silently. Leave such a query to the caller's disabled-KQL error.
    if (afterStatementSeparator(probe, end) != end)
        return nullptr;

    pos = probe;
    return node;
}

}
