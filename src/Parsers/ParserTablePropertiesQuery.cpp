#include <Parsers/ASTIdentifier.h>
#include <Parsers/TablePropertiesQueriesASTs.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserTablePropertiesQuery.h>

#include <Common/typeid_cast.h>


namespace DB
{


bool ParserTablePropertiesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges)
{
    ParserKeyword s_exists("EXISTS");
    ParserKeyword s_temporary("TEMPORARY");
    ParserKeyword s_describe("DESCRIBE");
    ParserKeyword s_desc("DESC");
    ParserKeyword s_show("SHOW");
    ParserKeyword s_create("CREATE");
    ParserKeyword s_database("DATABASE");
    ParserKeyword s_table("TABLE");
    ParserKeyword s_dictionary("DICTIONARY");
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier name_p;

    ASTPtr database;
    ASTPtr table;
    std::shared_ptr<ASTQueryWithTableAndOutput> query;

    bool parse_only_database_name = false;

    bool temporary = false;
    if (s_exists.ignore(pos, expected, ranges))
    {
        if (s_temporary.ignore(pos, expected, ranges))
            temporary = true;

        if (s_table.checkWithoutMoving(pos, expected, ranges))
            query = std::make_shared<ASTExistsTableQuery>();
        else if (s_dictionary.checkWithoutMoving(pos, expected, ranges))
            query = std::make_shared<ASTExistsDictionaryQuery>();
        else
            query = std::make_shared<ASTExistsTableQuery>();
    }
    else if (s_show.ignore(pos, expected, ranges))
    {
        if (!s_create.ignore(pos, expected, ranges))
            return false;

        if (s_database.ignore(pos, expected, ranges))
        {
            parse_only_database_name = true;
            query = std::make_shared<ASTShowCreateDatabaseQuery>();
        }
        else if (s_dictionary.checkWithoutMoving(pos, expected, ranges))
            query = std::make_shared<ASTShowCreateDictionaryQuery>();
        else
            query = std::make_shared<ASTShowCreateTableQuery>();
    }
    else
    {
        return false;
    }

    if (parse_only_database_name)
    {
        if (!name_p.parse(pos, database, expected, ranges))
            return false;
    }
    else
    {
        if (temporary || s_temporary.ignore(pos, expected, ranges))
            query->temporary = true;

        if (!s_table.ignore(pos, expected, ranges))
            s_dictionary.ignore(pos, expected, ranges);

        if (!name_p.parse(pos, table, expected, ranges))
            return false;

        if (s_dot.ignore(pos, expected, ranges))
        {
            database = table;
            if (!name_p.parse(pos, table, expected, ranges))
                return false;
        }
    }

    tryGetIdentifierNameInto(database, query->database);
    tryGetIdentifierNameInto(table, query->table);

    node = query;

    return true;
}


}
