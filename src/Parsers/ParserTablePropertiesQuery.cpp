#include <Parsers/TablePropertiesQueriesASTs.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
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
    /// Use ParserCompoundIdentifier to support compound names like db.namespace.table
    ParserCompoundIdentifier name_p(/*table_name_with_optional_uuid*/ false, /*allow_query_parameter*/ true);

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
        else
        {
            if (s_temporary.ignore(pos, expected))
                temporary = true;

            if (s_view.ignore(pos, expected))
            {
                query = std::make_shared<ASTExistsViewQuery>();
                exists_view = true;
            }
            else if (s_table.checkWithoutMoving(pos, expected))
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

        // Check for TEMPORARY keyword after SHOW [CREATE]
        if (s_temporary.ignore(pos, expected))
            temporary = true;

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

        query->temporary = temporary;

        ASTPtr compound_name;
        if (!name_p.parse(pos, compound_name, expected))
            return false;

        /// Handle compound identifier (can be table, db.table, or db.namespace.table)
        /// Only one namespace level is supported (max 3 parts)
        if (auto * identifier = compound_name->as<ASTIdentifier>())
        {
            const auto & parts = identifier->name_parts;
            if (parts.size() == 1)
            {
                /// Just table name
                table = compound_name;
            }
            else if (parts.size() == 2)
        {
                /// database.table
                database = std::make_shared<ASTIdentifier>(parts[0]);
                table = std::make_shared<ASTIdentifier>(parts[1]);
            }
            else if (parts.size() == 3)
            {
                /// database.namespace.table -> database = parts[0], table = namespace.table
                database = std::make_shared<ASTIdentifier>(parts[0]);
                table = std::make_shared<ASTIdentifier>(parts[1] + "." + parts[2]);
            }
            else
            {
                /// More than 3 parts not supported - only one namespace level allowed
                return false;
            }
        }
        else
        {
            /// Non-identifier (shouldn't happen with ParserCompoundIdentifier)
            table = compound_name;
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
