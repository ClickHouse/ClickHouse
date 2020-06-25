#include <Parsers/MySQL/tryParseMySQLQuery.h>

#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/MySQL/ASTCreateQuery.h>

#include <Parsers/parseQuery.h>

namespace DB
{

namespace MySQLParser
{

bool ParserQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserDropQuery p_drop_query;
    ParserRenameQuery p_rename_query;
    ParserCreateQuery p_create_query;
    /// TODO: alter table

    return p_create_query.parse(pos, node, expected) || p_drop_query.parse(pos, node, expected)
        || p_rename_query.parse(pos, node, expected);
}

}

ASTPtr tryParseMySQLQuery(const std::string & query, size_t max_query_size, size_t max_parser_depth)
{
    std::string error_message;
    const char * pos = query.data();
    MySQLParser::ParserQuery p_query;
    return tryParseQuery(p_query, pos, query.data() + query.size(), error_message, false, "", false, max_query_size, max_parser_depth);
}

}

