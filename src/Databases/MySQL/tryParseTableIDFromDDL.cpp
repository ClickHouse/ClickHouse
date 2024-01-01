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
    IParser::Pos pos(tokens, 0);
    Expected expected;
    if (ParserKeyword("CREATE TEMPORARY TABLE").ignore(pos, expected) || ParserKeyword("CREATE TABLE").ignore(pos, expected))
    {
        ParserKeyword("IF NOT EXISTS").ignore(pos, expected);
        is_ddl = true;
    }
    else if (ParserKeyword("ALTER TABLE").ignore(pos, expected) || ParserKeyword("RENAME TABLE").ignore(pos, expected))
    {
        is_ddl = true;
    }
    else if (ParserKeyword("DROP TABLE").ignore(pos, expected) || ParserKeyword("DROP TEMPORARY TABLE").ignore(pos, expected))
    {
        ParserKeyword("IF EXISTS").ignore(pos, expected);
        is_ddl = true;
    }
    else if (ParserKeyword("TRUNCATE").ignore(pos, expected))
    {
        ParserKeyword("TABLE").ignore(pos, expected);
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
