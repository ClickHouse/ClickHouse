#include <Parsers/MySQL/ASTDDLTableIdentifier.h>

#include <Interpreters/StorageID.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{

namespace MySQLParser
{

ASTPtr ASTDDLTableIdentifier::clone() const
{
    auto res = std::make_shared<ASTDDLTableIdentifier>(*this);
    res->children.clear();
    return res;
}

bool ParseDDLTableIdentifier::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr table;
    if (ParserKeyword("CREATE TEMPORARY TABLE").ignore(pos, expected) || ParserKeyword("CREATE TABLE").ignore(pos, expected))
    {
        ParserKeyword("IF NOT EXISTS").ignore(pos, expected);
        if (!ParserCompoundIdentifier(true).parse(pos, table, expected))
            return false;
    }
    else if (ParserKeyword("ALTER TABLE").ignore(pos, expected))
    {
        if (!ParserCompoundIdentifier(true).parse(pos, table, expected))
            return false;
    }
    else if (ParserKeyword("DROP TABLE").ignore(pos, expected) || ParserKeyword("DROP TEMPORARY TABLE").ignore(pos, expected))
    {
        ParserKeyword("IF EXISTS").ignore(pos, expected);
        if (!ParserCompoundIdentifier(true).parse(pos, table, expected))
            return false;
    }
    else if (ParserKeyword("TRUNCATE").ignore(pos, expected))
    {
        ParserKeyword("TABLE").ignore(pos, expected);
        if (!ParserCompoundIdentifier(true).parse(pos, table, expected))
            return false;
    }
    else if (ParserKeyword("RENAME TABLE").ignore(pos, expected))
    {
        if (!ParserCompoundIdentifier(true).parse(pos, table, expected))
            return false;
    }
    else
    {
        while (!pos->isEnd())
            ++ pos;
        node = std::make_shared<ASTLiteral>(Field(UInt64(1)));
        return true;
    }
    //skip parse the rest of sql
    while (!pos->isEnd())
        ++ pos;

    auto table_id = table->as<ASTTableIdentifier>()->getTableId();
    auto table_identifier = std::make_shared<ASTDDLTableIdentifier>();
    table_identifier->table = table_id.table_name;
    table_identifier->database = table_id.database_name;
    node = table_identifier;
    return true;
}
}

}
