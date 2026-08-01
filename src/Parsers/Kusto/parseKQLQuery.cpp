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
        Tokens sql_tokens(pos, end, max_query_size, /*skip_insignificant=*/true);
        IParser::Pos token_iterator(sql_tokens, static_cast<uint32_t>(max_parser_depth), static_cast<uint32_t>(max_parser_backtracks));

        ParserSetQuery set_parser;
        ASTPtr node;
        Expected expected;
        if (set_parser.parse(token_iterator, node, expected))
        {
            pos = token_iterator->begin;
            /// Consume the statement separator so the caller resumes on the next statement.
            while (pos < end && (*pos == ';' || isWhitespaceASCII(*pos)))
                ++pos;
            return node;
        }
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Cannot parse the SET statement");
    }

    KQLParser parser(pos, std::move(tokens), max_parser_depth);
    KQLTabularExpressionPtr query = parser.parseQuery();
    ASTPtr result = translateKQLQuery(*query);

    pos = parser.getEndPosition();

    if (!allow_multi_statements)
    {
        /// Anything left over would otherwise be dropped without a word.
        const char * rest = pos;
        while (rest < end && (*rest == ';' || isWhitespaceASCII(*rest)))
            ++rest;
        if (rest != end)
            throw Exception(
                ErrorCodes::SYNTAX_ERROR,
                "Multi-statements are not allowed: unexpected text at position {}",
                rest - pos + 1);
    }

    return result;
}

}
