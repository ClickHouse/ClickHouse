#include <Parsers/MySQL/ASTAlterQuery.h>

#include <Interpreters/StorageID.h>
#include <Common/quoteString.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/MySQL/ASTAlterCommand.h>

namespace DB
{

namespace MySQLParser
{

ASTPtr ASTAlterQuery::clone() const
{
    auto res = std::make_shared<ASTAlterQuery>(*this);
    res->children.clear();

    if (command_list)
    {
        res->command_list = command_list->clone();
        res->children.emplace_back(res->command_list);
    }

    return res;
}

bool ParserAlterQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr table;
    ASTPtr command_list;

    if (!ParserKeyword("ALTER TABLE").ignore(pos, expected))
        return false;

    if (!ParserCompoundIdentifier(false).parse(pos, table, expected))
        return false;

    if (!ParserList(std::make_unique<ParserAlterCommand>(), std::make_unique<ParserToken>(TokenType::Comma)).parse(pos, command_list, expected))
        return false;

    auto alter_query = std::make_shared<ASTAlterQuery>();

    node = alter_query;
    alter_query->command_list = command_list;
    StorageID table_id = getTableIdentifier(table);
    alter_query->table = table_id.table_name;
    alter_query->database = table_id.database_name;

    if (alter_query->command_list)
        alter_query->children.emplace_back(alter_query->command_list);

    return true;
}

}

}
