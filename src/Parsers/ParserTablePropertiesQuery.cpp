#include <Parsers/TablePropertiesQueriesASTs.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserTablePropertiesQuery.h>

#include <Common/typeid_cast.h>


namespace DB
{


bool ParserTablePropertiesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_exists(Keyword::EXISTS);
    ParserKeyword s_temporary(Keyword::TEMPORARY);
    ParserKeyword s_show(Keyword::SHOW);
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_database(Keyword::DATABASE);
    ParserKeyword s_table(Keyword::TABLE);
    ParserKeyword s_view(Keyword::VIEW);
    ParserKeyword s_dictionary(Keyword::DICTIONARY);
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier name_p(true);

    ASTPtr database;
    ASTPtr table;
    std::shared_ptr<ASTQueryWithTableAndOutput> query;

    bool parse_only_database_name = false;
    bool parse_show_create_view = false;
    bool exists_view = false;

    bool temporary = false;
    if (s_exists.ignore(pos, expected))
    {
        if (s_database.ignore(pos, expected))
        {
            query = std::make_shared<ASTExistsDatabaseQuery>();
            parse_only_database_name = true;
        }
        else if (s_view.ignore(pos, expected))
        {
            query = std::make_shared<ASTExistsViewQuery>();
            exists_view = true;
        }
        else
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
    }
    else if (s_show.ignore(pos, expected))
    {
        bool has_create = false;

        if (s_create.checkWithoutMoving(pos, expected))
        {
            has_create = true;
            s_create.ignore(pos, expected);
        }

        if (s_database.ignore(pos, expected))
        {
            parse_only_database_name = true;
            query = std::make_shared<ASTShowCreateDatabaseQuery>();
        }
        else if (s_dictionary.checkWithoutMoving(pos, expected))
            query = std::make_shared<ASTShowCreateDictionaryQuery>();
        else if (s_view.ignore(pos, expected))
        {
            query = std::make_shared<ASTShowCreateViewQuery>();
            parse_show_create_view = true;
        }
        else
        {
            /// We support `SHOW CREATE tbl;` and `SHOW TABLE tbl`,
            /// but do not support `SHOW tbl`, which is ambiguous
            /// with other statement like `SHOW PRIVILEGES`.
            if (has_create || s_table.checkWithoutMoving(pos, expected))
                query = std::make_shared<ASTShowCreateTableQuery>();
            else
                return false;
        }
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
        if (!(exists_view || parse_show_create_view))
        {
            if (temporary || s_temporary.ignore(pos, expected))
                query->temporary = true;

            if (!s_table.ignore(pos, expected))
                s_dictionary.ignore(pos, expected);
        }
        if (!name_p.parse(pos, table, expected))
            return false;
        if (s_dot.ignore(pos, expected))
        {
            database = table;
            if (!name_p.parse(pos, table, expected))
                return false;
        }
    }

    query->database = database;
    query->table = table;

    if (database)
        query->children.push_back(database);

    if (table)
        query->children.push_back(table);

    node = query;

    return true;
}


}
