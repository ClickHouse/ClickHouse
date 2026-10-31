#include <Parsers/Mongo/parseMongoQuery.h>

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/IParser.h>
#include <Parsers/ParserSetQuery.h>

#include <Parsers/Mongo/ParserMongoQuery.h>
#include <Parsers/Mongo/Utils.h>

#include <Common/StringUtils.h>
#include <Common/checkStackSize.h>

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

namespace Mongo
{

namespace
{

void normalizeIntegerField(Field & field)
{
    switch (field.getType())
    {
        case Field::Types::Int64:
            if (field.safeGet<Int64>() >= 0)
                field = Field(static_cast<UInt64>(field.safeGet<Int64>()));
            break;
        case Field::Types::Array:
            for (auto & element : field.safeGet<Array>())
                normalizeIntegerField(element);
            break;
        case Field::Types::Tuple:
            for (auto & element : field.safeGet<Tuple>())
                normalizeIntegerField(element);
            break;
        default:
            break;
    }
}

/** Stores every non-negative integer literal of the query as `UInt64`, the way the ClickHouse
  * parser reads it. JSON gives a number the type `Int64` whatever its sign, and the type of a
  * literal decides the name of the constant in the query tree: `2` stored as `Int64` becomes
  * `2_Int8`. A query that is sent to another server - a replica of parallel replicas, a shard of
  * a `Distributed` table - travels as text, where the same `2` is parsed back as `UInt8`, and the
  * header the remote side returns would not match the one the initiator expects.
  * The parsers of the dialect inspect the types of the literals they build, so this is done once,
  * on the finished query.
  */
void normalizeIntegerLiterals(IAST & ast)
{
    /// The AST is generated rather than parsed, so its depth is not bounded by `max_parser_depth`
    /// the way the depth of a parsed one is.
    checkStackSize();
    if (auto * literal = ast.as<ASTLiteral>())
        normalizeIntegerField(literal->value);
    for (const auto & child : ast.children)
        normalizeIntegerLiterals(*child);
}

}

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

    /// The statement is read as a whole, rather than token by token, so `max_query_size` is checked
    /// on the text of the statement itself: the translated query can be much smaller than it, e.g.
    /// when a filter carries a large `$comment`, which the translation drops.
    if (max_query_size && static_cast<size_t>(statement_end - _out_query_end) > max_query_size)
    {
        out_error_message = fmt::format("Max query size exceeded: the Mongo query is longer than {} bytes", max_query_size);
        return nullptr;
    }

    auto [data_begin, data_end] = getSettingsSubstring(_out_query_end, statement_end);
    metadata = extractMetadataFromRequest(_out_query_end, statement_end);
    metadata->max_parser_depth = max_parser_depth;
    dynamic_cast<ParserMongoQuery &>(parser).setParsingData(
        parseData(data_begin, data_end, metadata->getAllocator(), max_parser_depth), metadata);

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
    normalizeIntegerLiterals(*res);
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
    const std::string & database,
    const std::string & collection)
{
    /// The wire protocol handlers format the returned AST unconditionally, so a failed
    /// parse must throw here - a returned nullptr would be dereferenced. The exception
    /// becomes a controlled error reply to the Mongo client.
    if (begin == end)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Empty Mongo query");

    /// A `SET` is parsed by the ClickHouse parser here as well: the client is not the only one that
    /// parses the text of a query, the server parses it again with the dialect of the session.
    if (ASTPtr set_query = tryParseLeadingSetQuery(begin, end, max_query_size, max_parser_depth, max_parser_backtracks))
        return set_query;

    /// This path carries a single statement: a trailing `;` is tolerated, a second statement
    /// after it is not - it would be silently dropped rather than executed.
    {
        const char * statement_end = findStatementEnd(begin, end);
        for (const char * pos = statement_end == end ? end : statement_end + 1; pos != end; ++pos)
            if (!isWhitespaceASCII(*pos))
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Only a single Mongo statement is expected, but there is text after it");
    }

    /// See `tryParseMongoQuery`.
    if (max_query_size && static_cast<size_t>(end - begin) > max_query_size)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Max query size exceeded: the Mongo query is longer than {} bytes", max_query_size);

    Expected expected;
    ASTPtr res;
    Tokens token_subquery(begin, end, max_query_size, true);
    auto metadata = extractMetadataFromRequest(begin, end, database, collection);
    metadata->add_data_to_query = false;
    metadata->max_parser_depth = max_parser_depth;
    auto [data_begin, data_end] = getSettingsSubstring(begin, end);

    dynamic_cast<ParserMongoQuery &>(parser).setParsingData(
        parseData(data_begin, data_end, metadata->getAllocator(), max_parser_depth), metadata);
    IParser::Pos token_iterator(token_subquery, static_cast<uint32_t>(max_parser_depth), static_cast<uint32_t>(max_parser_backtracks));
    const bool parse_res = parser.parse(token_iterator, res, expected);
    if (!parse_res || !res)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Cannot parse the Mongo query");
    normalizeIntegerLiterals(*res);
    return res;
}

}

}
