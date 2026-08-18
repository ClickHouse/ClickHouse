#include <Parsers/ASTDropQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/StatementFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

namespace
{

bool parseDropQuery(IParser::Pos & pos, ASTPtr & node, Expected & expected, const ASTDropQuery::Kind kind)
{
    ParserKeyword s_temporary(Keyword::TEMPORARY);
    ParserKeyword s_table(Keyword::TABLE);
    ParserKeyword s_dictionary(Keyword::DICTIONARY);
    ParserKeyword s_view(Keyword::VIEW);
    ParserKeyword s_database(Keyword::DATABASE);
    ParserKeyword s_from(Keyword::FROM);
    ParserKeyword s_all(Keyword::ALL);
    ParserKeyword s_tables(Keyword::TABLES);
    ParserKeyword s_not(Keyword::NOT);
    ParserKeyword s_like(Keyword::LIKE);
    ParserKeyword s_ilike(Keyword::ILIKE);
    ParserToken s_dot(TokenType::Dot);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_if_empty(Keyword::IF_EMPTY);
    ParserIdentifier name_p(true);
    ParserStringLiteral like_p(Highlight::string_like);
    ParserKeyword s_permanently(Keyword::PERMANENTLY);
    ParserKeyword s_no_delay(Keyword::NO_DELAY);
    ParserKeyword s_sync(Keyword::SYNC);
    ParserNameList tables_p;

    ASTPtr database;
    ASTPtr database_and_tables;
    String cluster_str;
    ASTPtr like;
    bool if_exists = false;
    bool if_empty = false;
    bool has_tables = false;
    bool is_like = false;
    bool is_not_like = false;
    bool is_case_insensitive_like = false;
    bool has_all = false;
    bool temporary = false;
    bool is_dictionary = false;
    bool is_view = false;
    bool sync = false;
    bool permanently = false;

    if (s_all.checkWithoutMoving(pos, expected))
        has_all = true;

    if (s_database.ignore(pos, expected))
    {
        if (s_if_exists.ignore(pos, expected))
            if_exists = true;

        if (s_if_empty.ignore(pos, expected))
            if_empty = true;

        if (!name_p.parse(pos, database, expected))
            return false;
    }
    else if ((s_tables.ignore(pos, expected) || (s_all.ignore(pos, expected) && s_tables.ignore(pos, expected))) && kind == ASTDropQuery::Kind::Truncate)
    {
        /// Either 'TRUNCATE TABLES FROM ..' or 'TRUNCATE ALL TABLES FROM ..'
        has_tables = true;
        if (!s_from.ignore(pos, expected))
            return false;

        if (s_if_exists.ignore(pos, expected))
            if_exists = true;

        if (!name_p.parse(pos, database, expected))
            return false;

        bool not_like = false;
        if (s_not.ignore(pos, expected))
            not_like = true;

        if (s_like.ignore(pos, expected))
        {
            if (not_like)
                is_not_like = true;
            if (!like_p.parse(pos, like, expected))
                return false;
            is_like = true;
        }

        if (s_ilike.ignore(pos, expected))
        {
            is_case_insensitive_like = true;
            if (not_like)
                is_not_like = true;
            if (!like_p.parse(pos, like, expected))
                return false;
            is_like = true;
        }
    }
    else
    {
        if (s_temporary.ignore(pos, expected))
            temporary = true;

        if (s_view.ignore(pos, expected))
            is_view = true;
        else if (s_dictionary.ignore(pos, expected))
            is_dictionary = true;

        /// for TRUNCATE queries TABLE keyword is assumed as default and can be skipped
        if (!is_view && !is_dictionary && (!s_table.ignore(pos, expected) && kind != ASTDropQuery::Kind::Truncate))
        {
            return false;
        }

        if (s_if_exists.ignore(pos, expected))
            if_exists = true;

        if (s_if_empty.ignore(pos, expected))
            if_empty = true;

        if (!tables_p.parse(pos, database_and_tables, expected))
            return false;

        if (database_and_tables->as<ASTExpressionList &>().children.size() > 1 && kind != ASTDropQuery::Kind::Drop)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Only Support DROP multiple tables currently");
    }

    /// common for tables / dictionaries / databases
    if (ParserKeyword{Keyword::ON}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    if (kind == ASTDropQuery::Kind::Detach && s_permanently.ignore(pos, expected))
        permanently = true;

    /// actually for TRUNCATE NO DELAY / SYNC means nothing
    if (s_no_delay.ignore(pos, expected) || s_sync.ignore(pos, expected))
        sync = true;

    auto query = make_intrusive<ASTDropQuery>();
    node = query;

    query->kind = kind;
    query->if_exists = if_exists;
    query->if_empty = if_empty;
    query->has_tables = has_tables;
    query->has_all = has_all;
    query->setIsTemporary(temporary);
    query->is_dictionary = is_dictionary;
    query->is_view = is_view;
    query->sync = sync;
    query->permanently = permanently;
    query->database = database;
    query->database_and_tables = database_and_tables;
    query->case_insensitive_like = is_case_insensitive_like;
    query->not_like = is_not_like;

    if (database)
        query->children.push_back(database);

    if (database_and_tables)
        query->children.push_back(database_and_tables);

    if (is_like)
        query->like = like->as<ASTLiteral &>().value.safeGet<String>();

    query->cluster = cluster_str;

    if (database_and_tables && database_and_tables->as<ASTExpressionList &>().children.size() == 1)
        node = query->getRewrittenASTsOfSingleTable(query)[0];

    return true;
}

}

bool ParserDropQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_detach(Keyword::DETACH);
    ParserKeyword s_truncate(Keyword::TRUNCATE);

    if (s_drop.ignore(pos, expected))
        return parseDropQuery(pos, node, expected, ASTDropQuery::Kind::Drop);
    if (s_detach.ignore(pos, expected))
        return parseDropQuery(pos, node, expected, ASTDropQuery::Kind::Detach);
    if (s_truncate.ignore(pos, expected))
        return parseDropQuery(pos, node, expected, ASTDropQuery::Kind::Truncate);
    return false;
}

}

namespace DB
{

REGISTER_STATEMENTS(Drop)
{
    factory.registerStatement("DROP", "",
    {
        .description = R"(
Deletes an existing entity. If the `IF EXISTS` clause is specified, the query does not return an error if the entity
does not exist. If the `SYNC` modifier is specified, the entity is dropped without delay.
)",
        .syntax = R"(
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster] [SYNC]
DROP [TEMPORARY] TABLE [IF EXISTS] [IF EMPTY] [db1.]name_1[, [db2.]name_2, ...] [ON CLUSTER cluster] [SYNC]
DROP DICTIONARY [IF EXISTS] [db.]name [SYNC]
DROP VIEW [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
DROP MASKING POLICY [IF EXISTS] name ON [database.]table [ON CLUSTER cluster_name] [FROM access_storage_type]
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
DROP FUNCTION [IF EXISTS] function_name [ON CLUSTER cluster]
DROP NAMED COLLECTION [IF EXISTS] name [ON CLUSTER cluster]
)",
        .examples = {
            {"Drop a table", "DROP TABLE IF EXISTS test SYNC;", ""},
            {"Drop a database", "DROP DATABASE IF EXISTS test;", ""},
        },
        .related = {"DETACH", "TRUNCATE", "UNDROP", "CREATE"},
    });

    factory.registerStatement("DETACH", "",
    {
        .description = R"(
Makes the server "forget" about the existence of a table, a materialized view, a dictionary, or a database.

Detaching does not delete the data or the metadata of the entity. If the entity was not detached `PERMANENTLY`, on the
next server launch the server reads the metadata and recalls the entity again. A permanently detached entity is not
recalled automatically, but it can be attached back with `ATTACH`.
)",
        .syntax = R"(
DETACH TABLE|VIEW|DICTIONARY|DATABASE [IF EXISTS] [db.]name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]
)",
        .examples = {
            {"Detach a table", "DETACH TABLE test;", ""},
            {"Detach a table permanently", "DETACH TABLE test PERMANENTLY;", ""},
        },
        .related = {"ATTACH", "DROP"},
    });

    factory.registerStatement("TRUNCATE", "",
    {
        .description = R"(
Quickly removes all data from a table or from all tables of a database, while preserving their structure.
)",
        .syntax = R"(
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
TRUNCATE [ALL] TABLES FROM [IF EXISTS] db [LIKE | ILIKE | NOT LIKE '<pattern>'] [ON CLUSTER cluster]
TRUNCATE DATABASE [IF EXISTS] db [ON CLUSTER cluster]
)",
        .examples = {
            {"Remove all rows of a table", "TRUNCATE TABLE test;", ""},
            {"Remove all rows of all tables of a database", "TRUNCATE ALL TABLES FROM test;", ""},
        },
        .related = {"DROP", "DELETE", "ALTER TABLE ... PARTITION"},
    });
}

}
