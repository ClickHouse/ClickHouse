#include <Parsers/TablePropertiesQueriesASTs.h>

#include <Parsers/ASTIdentifier.h>
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
    boost::intrusive_ptr<ASTQueryWithTableAndOutput> query;

    bool parse_only_database_name = false;
    bool parse_show_create_view = false;
    bool exists_view = false;

    bool temporary = false;

    if (s_exists.ignore(pos, expected))
    {
        if (s_database.ignore(pos, expected))
        {
            query = make_intrusive<ASTExistsDatabaseQuery>();
            parse_only_database_name = true;
        }
        else
        {
            if (s_temporary.ignore(pos, expected))
                temporary = true;

            if (s_view.ignore(pos, expected))
            {
                query = make_intrusive<ASTExistsViewQuery>();
                exists_view = true;
            }
            else if (s_table.checkWithoutMoving(pos, expected))
                query = make_intrusive<ASTExistsTableQuery>();
            else if (s_dictionary.checkWithoutMoving(pos, expected))
                query = make_intrusive<ASTExistsDictionaryQuery>();
            else
                query = make_intrusive<ASTExistsTableQuery>();
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
            query = make_intrusive<ASTShowCreateDatabaseQuery>();
        }
        else if (s_dictionary.checkWithoutMoving(pos, expected))
            query = make_intrusive<ASTShowCreateDictionaryQuery>();
        else if (s_view.ignore(pos, expected))
        {
            query = make_intrusive<ASTShowCreateViewQuery>();
            parse_show_create_view = true;
        }
        else
        {
            /// We support `SHOW CREATE tbl;` and `SHOW TABLE tbl`,
            /// but do not support `SHOW tbl`, which is ambiguous
            /// with other statement like `SHOW PRIVILEGES`.
            if (has_create || s_table.checkWithoutMoving(pos, expected))
                query = make_intrusive<ASTShowCreateTableQuery>();
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
                query->setIsTemporary(true);

            if (!s_table.ignore(pos, expected))
                s_dictionary.ignore(pos, expected);
        }

        query->setIsTemporary(temporary);

        if (!name_p.parse(pos, table, expected))
            return false;
        if (s_dot.ignore(pos, expected))
        {
            database = table;
            if (!name_p.parse(pos, table, expected))
                return false;

            /// hierarchical table path (experimental): db.ns1.ns2.table -> (db, `ns1.ns2.table`).
            /// no query parameters inside a path, and no quoted components with a literal dot:
            /// a substituted or quoted dot would alias another path
            if (pos.allow_multipart_table_paths && s_dot.checkWithoutMoving(pos, expected))
            {
                String table_path = getIdentifierName(table);
                if (getIdentifierName(database).find('.') != String::npos
                    || table_path.empty() || table_path.find('.') != String::npos)
                    return false;
                while (s_dot.ignore(pos, expected))
                {
                    ASTPtr part;
                    if (!name_p.parse(pos, part, expected))
                        return false;
                    const String part_name = getIdentifierName(part);
                    if (part_name.empty() || part_name.find('.') != String::npos)
                        return false;
                    table_path += "." + part_name;
                }
                table = make_intrusive<ASTIdentifier>(table_path);
            }
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
