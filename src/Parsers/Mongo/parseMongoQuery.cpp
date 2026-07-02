#include "parseMongoQuery.h"

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/IParser.h>

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
    std::string & /*out_error_message*/,
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

    auto [data_begin, data_end] = getSettingsSubstring(_out_query_end, end);
    metadata = extractMetadataFromRequest(_out_query_end, end);
    dynamic_cast<ParserMongoQuery &>(parser).setParsingData(parseData(data_begin, data_end), metadata);

    _out_query_end = findKth<';'>(_out_query_end, end, 1) + 1;
    const bool parse_res = parser.parse(token_iterator, res, expected);
    if (!parse_res)
    {
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
    size_t max_parser_backtracks)
{
    if (begin == end)
    {
        return nullptr;
    }
    Expected expected;
    ASTPtr res;
    Tokens token_subquery(begin, end, max_query_size, true);
    auto metadata = extractMetadataFromRequest(begin, end);
    metadata->add_data_to_query = false;
    auto [data_begin, data_end] = getSettingsSubstring(begin, end);

    dynamic_cast<ParserMongoQuery &>(parser).setParsingData(parseData(data_begin, data_end), metadata);
    IParser::Pos token_iterator(token_subquery, static_cast<uint32_t>(max_parser_depth), static_cast<uint32_t>(max_parser_backtracks));
    const bool parse_res = parser.parse(token_iterator, res, expected);
    if (!parse_res)
    {
        return nullptr;
    }
    return res;
}

}

}
