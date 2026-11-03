#include <Parsers/LogsQL/parseLogsQLQuery.h>

#include <Parsers/Lexer.h>
#include <Parsers/TokenIterator.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

ASTPtr tryParseLogsQLQuery(
    IParser & parser,
    const char * & _out_query_end,
    const char * all_queries_end,
    String & out_error_message,
    int * out_error_code,
    bool allow_multi_statements,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks)
{
    if (out_error_code)
        *out_error_code = ErrorCodes::SYNTAX_ERROR;

    const char * query_begin = _out_query_end;

    Tokens tokens(query_begin, all_queries_end, max_query_size, /*skip_insignificant=*/ true);
    IParser::Pos token_iterator(tokens, static_cast<uint32_t>(max_parser_depth), static_cast<uint32_t>(max_parser_backtracks));

    if (token_iterator->isEnd() || token_iterator->type == TokenType::Semicolon)
    {
        out_error_message = "Empty query";
        _out_query_end = token_iterator->begin;
        return nullptr;
    }

    Expected expected;
    ASTPtr res;
    bool parse_res = false;
    try
    {
        parse_res = parser.parse(token_iterator, res, expected);
    }
    catch (const Exception & e)
    {
        out_error_message = e.message();
        if (out_error_code)
            *out_error_code = e.code();
        _out_query_end = token_iterator->begin;
        return nullptr;
    }

    const auto last_token = token_iterator.max();
    _out_query_end = last_token.end;

    if (!parse_res)
    {
        out_error_message = fmt::format("Syntax error: failed at position {}", last_token.begin - query_begin + 1);
        return nullptr;
    }

    /// The parsed query must end at the end of the input or at a semicolon.
    if (!token_iterator->isEnd() && token_iterator->type != TokenType::Semicolon)
    {
        out_error_message = fmt::format("Syntax error: unexpected input at position {} after the end of the query",
            token_iterator->begin - query_begin + 1);
        return nullptr;
    }

    while (token_iterator->type == TokenType::Semicolon)
        ++token_iterator;

    if (!allow_multi_statements && !token_iterator->isEnd())
    {
        out_error_message = "Syntax error: multi-statements are not allowed";
        return nullptr;
    }

    return res;
}

ASTPtr parseLogsQLQueryAndMovePosition(
    IParser & parser,
    const char * & pos,
    const char * end,
    bool allow_multi_statements,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks)
{
    String error_message;
    int error_code = ErrorCodes::SYNTAX_ERROR;

    ASTPtr res = tryParseLogsQLQuery(
        parser, pos, end, error_message, &error_code, allow_multi_statements, max_query_size, max_parser_depth, max_parser_backtracks);

    if (res)
        return res;

    throw Exception::createDeprecated(error_message, error_code);
}

ASTPtr parseLogsQLQuery(
    IParser & parser,
    const char * begin,
    const char * end,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks)
{
    return parseLogsQLQueryAndMovePosition(parser, begin, end, /*allow_multi_statements=*/ false, max_query_size, max_parser_depth, max_parser_backtracks);
}

}
