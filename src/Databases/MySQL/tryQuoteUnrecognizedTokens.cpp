#include <Databases/MySQL/tryQuoteUnrecognizedTokens.h>
#include <Parsers/CommonParsers.h>
#include <Common/quoteString.h>

namespace DB
{

/// Checks if there are no any tokens (like whitespaces) between current and previous pos
static bool noWhitespaces(const char * to, const char * from)
{
    return static_cast<size_t>(from - to) == 0;
}

/// Checks if the token should be quoted too together with unrecognized
static bool isWordOrNumber(TokenType type)
{
    return type == TokenType::BareWord || type == TokenType::Number;
}

static void quoteLiteral(
    IParser::Pos & pos,
    IParser::Pos & pos_prev,
    const char *& pos_unrecognized,
    const char *& copy_from,
    String & rewritten_query)
{
    /// Copy also whitespaces if any
    const auto * end =
        isWordOrNumber(pos->type) && noWhitespaces(pos_prev->end, pos->begin)
        ? pos->end
        : pos_prev->end;
    String literal(pos_unrecognized, static_cast<size_t>(end - pos_unrecognized));
    rewritten_query.append(copy_from, pos_unrecognized - copy_from).append(backQuoteMySQL(literal));
    copy_from = end;
}

bool tryQuoteUnrecognizedTokens(String & query)
{
    Tokens tokens(query.data(), query.data() + query.size());
    IParser::Pos pos(tokens, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    Expected expected;
    String rewritten_query;
    const char * copy_from = query.data();
    auto pos_prev = pos;
    const char * pos_unrecognized = nullptr;
    for (;pos->type != TokenType::EndOfStream; ++pos)
    {
        /// Commit quotes if any whitespaces found or the token is not a word
        bool commit = !noWhitespaces(pos_prev->end, pos->begin) || (pos->type != TokenType::Error && !isWordOrNumber(pos->type));
        if (pos_unrecognized && commit)
        {
            quoteLiteral(
                pos,
                pos_prev,
                pos_unrecognized,
                copy_from,
                rewritten_query);
            pos_unrecognized = nullptr;
        }
        if (pos->type == TokenType::Error)
        {
            /// Find first appearance of the error token
            if (!pos_unrecognized)
            {
                pos_unrecognized =
                    isWordOrNumber(pos_prev->type) && noWhitespaces(pos_prev->end, pos->begin)
                    ? pos_prev->begin
                    : pos->begin;
            }
        }
        pos_prev = pos;
    }

    /// There was EndOfStream but not committed unrecognized token
    if (pos_unrecognized)
    {
        quoteLiteral(
            pos,
            pos_prev,
            pos_unrecognized,
            copy_from,
            rewritten_query);
        pos_unrecognized = nullptr;
    }

    /// If no Errors found
    if (copy_from == query.data())
        return false;

    auto size = static_cast<size_t>(pos->end - copy_from);
    rewritten_query.append(copy_from, size);
    query = rewritten_query;
    return true;
}

}
