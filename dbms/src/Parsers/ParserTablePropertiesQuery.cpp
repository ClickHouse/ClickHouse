#include <Parsers/ASTIdentifier.h>
#include <Parsers/TablePropertiesQueriesASTs.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserTablePropertiesQuery.h>

#include <Common/typeid_cast.h>


namespace DB
{


bool ParserTablePropertiesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_exists("EXISTS");
    ParserKeyword s_temporary("TEMPORARY");
    ParserKeyword s_describe("DESCRIBE");
    ParserKeyword s_desc("DESC");
    ParserKeyword s_show("SHOW");
    ParserKeyword s_create("CREATE");
    ParserKeyword s_table("TABLE");
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier name_p;

    ASTPtr database;
    ASTPtr table;
    std::shared_ptr<ASTQueryWithTableAndOutput> query;

    if (s_exists.ignore(pos, expected))
    {
        query = std::make_shared<ASTExistsQuery>();
    }
    else if (s_show.ignore(pos, expected))
    {
        if (!s_create.ignore(pos, expected))
            return false;

        query = std::make_shared<ASTShowCreateQuery>();
    }
    else
    {
        return false;
    }

    if (s_temporary.ignore(pos, expected))
        query->temporary = true;

    s_table.ignore(pos, expected);

    if (!name_p.parse(pos, table, expected))
        return false;

    if (s_dot.ignore(pos, expected))
    {
        database = table;
        if (!name_p.parse(pos, table, expected))
            return false;
    }

    if (database)
        query->database = typeid_cast<ASTIdentifier &>(*database).name;
    if (table)
        query->table = typeid_cast<ASTIdentifier &>(*table).name;

    node = query;

    return true;
}


}
