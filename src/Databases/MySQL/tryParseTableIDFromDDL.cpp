#include <Databases/MySQL/tryParseTableIDFromDDL.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{

StorageID tryParseTableIDFromDDL(const String & query, const String & default_database_name)
{
    bool is_ddl = false;
    Tokens tokens(query.data(), query.data() + query.size());
    IParser::Pos pos(tokens, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    Expected expected;
    if (ParserKeyword(Keyword::CREATE_TEMPORARY_TABLE).ignore(pos, expected) || ParserKeyword(Keyword::CREATE_TABLE).ignore(pos, expected))
    {
        ParserKeyword(Keyword::IF_NOT_EXISTS).ignore(pos, expected);
        is_ddl = true;
    }
    else if (ParserKeyword(Keyword::ALTER_TABLE).ignore(pos, expected) || ParserKeyword(Keyword::RENAME_TABLE).ignore(pos, expected))
    {
        is_ddl = true;
    }
    else if (ParserKeyword(Keyword::DROP_TABLE).ignore(pos, expected) || ParserKeyword(Keyword::DROP_TEMPORARY_TABLE).ignore(pos, expected))
    {
        ParserKeyword(Keyword::IF_EXISTS).ignore(pos, expected);
        is_ddl = true;
    }
    else if (ParserKeyword(Keyword::TRUNCATE).ignore(pos, expected))
    {
        ParserKeyword(Keyword::TABLE).ignore(pos, expected);
        is_ddl = true;
    }

    ASTPtr table;
    if (!is_ddl || !ParserCompoundIdentifier(true).parse(pos, table, expected))
        return StorageID::createEmpty();
    auto table_id = table->as<ASTTableIdentifier>()->getTableId();
    if (table_id.database_name.empty())
        table_id.database_name = default_database_name;
    return table_id;
}

}
