#include <Parsers/Mongo/parseMongoQuery.h>

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/IParser.h>
#include <Parsers/ParserSetQuery.h>

#include <Parsers/Mongo/ParserMongoQuery.h>
#include <Parsers/Mongo/Utils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

namespace Mongo
{

ASTPtr tryParseMongoQuery(
    IParser & parser,
    const char *& _out_query_end, // query start as input parameter, query end as output
    const char *& end,
    std::string & out_error_message,
    bool /*hilite*/,
    const std::string & /*description*/,
    bool /*allow_multi_statements*/,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks,
    bool /*skip_insignificant*/)
{
    Expected expected;
    ASTPtr res;
    Tokens token_subquery(_out_query_end, end, max_query_size, true);
    IParser::Pos token_iterator(token_subquery, static_cast<uint32_t>(max_parser_depth), static_cast<uint32_t>(max_parser_backtracks));
    std::shared_ptr<QueryMetadata> metadata;

    /** A `SET` is parsed by the ClickHouse parser, the way the other dialects do it as well. It is
      * how a session that switched to this dialect switches back out of it: the Mongo query
      * language has no statement of its own that could change a setting.
      */
    {
        Tokens set_tokens(_out_query_end, end, max_query_size, true);
        IParser::Pos set_iterator(set_tokens, static_cast<uint32_t>(max_parser_depth), static_cast<uint32_t>(max_parser_backtracks));
        Expected set_expected;
        ASTPtr set_query;
        if (ParserSetQuery().parse(set_iterator, set_query, set_expected))
        {
            /// The terminator belongs to the statement that was just parsed, so that the next query
            /// starts after it rather than at it.
            ParserToken(TokenType::Semicolon).ignore(set_iterator, set_expected);
            _out_query_end = set_iterator->begin;
            return set_query;
        }
    }

    /** The statement reaches to its terminator - a `;` outside a string literal - or, when there
      * is none, to the end of the input: a single query needs no trailing `;`, and a `;` inside a
      * value such as `{"name": "a;b"}` is data. Everything the statement consists of is read from
      * within this bound, so that the `.limit` of a later statement of a multi query cannot leak
      * into this one.
      */
    const char * statement_end = findStatementEnd(_out_query_end, end);

    auto [data_begin, data_end] = getSettingsSubstring(_out_query_end, statement_end);
    metadata = extractMetadataFromRequest(_out_query_end, statement_end);
    dynamic_cast<ParserMongoQuery &>(parser).setParsingData(parseData(data_begin, data_end, metadata->getAllocator()), metadata);

    /// The terminator belongs to the statement, so the next one starts after it.
    _out_query_end = statement_end == end ? end : statement_end + 1;
    const bool parse_res = parser.parse(token_iterator, res, expected);
    if (!parse_res)
    {
        /// The message is what the caller turns into the exception, so leaving it empty would
        /// report a syntax error that says nothing at all.
        out_error_message = "Cannot parse the Mongo query";
        return nullptr;
    }
    return res;
}


/// Parse query or throw an exception with error message.
ASTPtr parseMongoQueryAndMovePosition(
    IParser & parser,
    const char *& pos, /// Moved to end of parsed fragment.
    const char * end,
    const std::string & description,
    bool allow_multi_statements,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks)
{
    std::string error_message;
    ASTPtr res = tryParseMongoQuery(
        parser,
        pos,
        end,
        error_message,
        false,
        description,
        allow_multi_statements,
        max_query_size,
        max_parser_depth,
        max_parser_backtracks);

    if (res)
        return res;

    throw Exception::createDeprecated(error_message, ErrorCodes::SYNTAX_ERROR);
}


ASTPtr parseMongoQuery(
    IParser & parser,
    const char * begin,
    const char * end,
    const std::string & /*description*/,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks,
    const std::string & database)
{
    /// The wire protocol handlers format the returned AST unconditionally, so a failed
    /// parse must throw here - a returned nullptr would be dereferenced. The exception
    /// becomes a controlled error reply to the Mongo client.
    if (begin == end)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Empty Mongo query");

    /// A `SET` is parsed by the ClickHouse parser here as well: the client is not the only one that
    /// parses the text of a query, the server parses it again with the dialect of the session.
    {
        Tokens set_tokens(begin, end, max_query_size, true);
        IParser::Pos set_iterator(set_tokens, static_cast<uint32_t>(max_parser_depth), static_cast<uint32_t>(max_parser_backtracks));
        Expected set_expected;
        ASTPtr set_query;
        if (ParserSetQuery().parse(set_iterator, set_query, set_expected))
            return set_query;
    }

    Expected expected;
    ASTPtr res;
    Tokens token_subquery(begin, end, max_query_size, true);
    auto metadata = extractMetadataFromRequest(begin, end, database);
    metadata->add_data_to_query = false;
    auto [data_begin, data_end] = getSettingsSubstring(begin, end);

    dynamic_cast<ParserMongoQuery &>(parser).setParsingData(parseData(data_begin, data_end, metadata->getAllocator()), metadata);
    IParser::Pos token_iterator(token_subquery, static_cast<uint32_t>(max_parser_depth), static_cast<uint32_t>(max_parser_backtracks));
    const bool parse_res = parser.parse(token_iterator, res, expected);
    if (!parse_res || !res)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Cannot parse the Mongo query");
    return res;
}

}

}
