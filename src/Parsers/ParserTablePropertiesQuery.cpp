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
    if (s_exists.ignore(pos, expected))
    {
        if (s_temporary.ignore(pos, expected))
            temporary = true;

        if (s_table.checkWithoutMoving(pos, expected))
            query = std::make_shared<ASTExistsTableQuery>();
        else if (s_dictionary.checkWithoutMoving(pos, expected))
            query = std::make_shared<ASTExistsDictionaryQuery>();
        else
            query = std::make_shared<ASTExistsTableQuery>();
    }
    else if (s_show.ignore(pos, expected))
    {
        if (!s_create.ignore(pos, expected))
            return false;

        if (s_database.ignore(pos, expected))
        {
            parse_only_database_name = true;
            query = std::make_shared<ASTShowCreateDatabaseQuery>();
        }
        else if (s_dictionary.checkWithoutMoving(pos, expected))
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
        if (!name_p.parse(pos, database, expected))
            return false;
    }
    else
    {
        if (temporary || s_temporary.ignore(pos, expected))
            query->temporary = true;

        if (!s_table.ignore(pos, expected))
            s_dictionary.ignore(pos, expected);

        if (!name_p.parse(pos, table, expected))
            return false;

        if (s_dot.ignore(pos, expected))
        {
            database = table;
            if (!name_p.parse(pos, table, expected))
                return false;
        }
    }

    tryGetIdentifierNameInto(database, query->database);
    tryGetIdentifierNameInto(table, query->table);

    node = query;

    return true;
}


}
