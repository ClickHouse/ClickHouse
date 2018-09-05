#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>


namespace DB
{

bool ParserNestedTable::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserToken open(TokenType::OpeningRoundBracket);
    ParserToken close(TokenType::ClosingRoundBracket);
    ParserIdentifier name_p;
    ParserNameTypePairList columns_p;

    ASTPtr name;
    ASTPtr columns;

    /// For now `name == 'Nested'`, probably alternative nested data structures will appear
    if (!name_p.parse(pos, name, expected))
        return false;

    if (!open.ignore(pos))
        return false;

    if (!columns_p.parse(pos, columns, expected))
        return false;

    if (!close.ignore(pos))
        return false;

    auto func = std::make_shared<ASTFunction>();
    func->name = typeid_cast<ASTIdentifier &>(*name).name;
    func->arguments = columns;
    func->children.push_back(columns);
    node = func;

    return true;
}


bool ParserIdentifierWithParameters::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserFunction function_or_array;
    if (function_or_array.parse(pos, node, expected))
        return true;

    ParserNestedTable nested;
    if (nested.parse(pos, node, expected))
        return true;

    return false;
}


bool ParserIdentifierWithOptionalParameters::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier non_parametric;
    ParserIdentifierWithParameters parametric;

    if (parametric.parse(pos, node, expected))
        return true;

    ASTPtr ident;
    if (non_parametric.parse(pos, ident, expected))
    {
        auto func = std::make_shared<ASTFunction>();
        func->name = typeid_cast<ASTIdentifier &>(*ident).name;
        node = func;
        return true;
    }

    return false;
}

bool ParserNameTypePairList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserNameTypePair>(), std::make_unique<ParserToken>(TokenType::Comma), false)
        .parse(pos, node, expected);
}

bool ParserColumnDeclarationList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserColumnDeclaration>(), std::make_unique<ParserToken>(TokenType::Comma), false)
        .parse(pos, node, expected);
}


bool ParserStorage::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_engine("ENGINE");
    ParserToken s_eq(TokenType::Equals);
    ParserKeyword s_partition_by("PARTITION BY");
    ParserKeyword s_order_by("ORDER BY");
    ParserKeyword s_sample_by("SAMPLE BY");
    ParserKeyword s_settings("SETTINGS");

    ParserIdentifierWithOptionalParameters ident_with_optional_params_p;
    ParserExpression expression_p;
    ParserSetQuery settings_p(/* parse_only_internals_ = */ true);

    ASTPtr engine;
    ASTPtr partition_by;
    ASTPtr order_by;
    ASTPtr sample_by;
    ASTPtr settings;

    if (!s_engine.ignore(pos, expected))
        return false;

    s_eq.ignore(pos, expected);

    if (!ident_with_optional_params_p.parse(pos, engine, expected))
        return false;

    while (true)
    {
        if (!partition_by && s_partition_by.ignore(pos, expected))
        {
            if (expression_p.parse(pos, partition_by, expected))
                continue;
            else
                return false;
        }

        if (!order_by && s_order_by.ignore(pos, expected))
        {
            if (expression_p.parse(pos, order_by, expected))
                continue;
            else
                return false;
        }

        if (!sample_by && s_sample_by.ignore(pos, expected))
        {
            if (expression_p.parse(pos, sample_by, expected))
                continue;
            else
                return false;
        }

        if (s_settings.ignore(pos, expected))
        {
            if (!settings_p.parse(pos, settings, expected))
                return false;
        }

        break;
    }

    auto storage = std::make_shared<ASTStorage>();
    storage->set(storage->engine, engine);
    storage->set(storage->partition_by, partition_by);
    storage->set(storage->order_by, order_by);
    storage->set(storage->sample_by, sample_by);
    storage->set(storage->settings, settings);

    node = storage;
    return true;
}


bool ParserCreateQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_temporary("TEMPORARY");
    ParserKeyword s_attach("ATTACH");
    ParserKeyword s_table("TABLE");
    ParserKeyword s_database("DATABASE");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserKeyword s_as("AS");
    ParserKeyword s_view("VIEW");
    ParserKeyword s_materialized("MATERIALIZED");
    ParserKeyword s_populate("POPULATE");
    ParserToken s_dot(TokenType::Dot);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserStorage storage_p;
    ParserIdentifier name_p;
    ParserColumnDeclarationList columns_p;
    ParserSelectWithUnionQuery select_p;

    ASTPtr database;
    ASTPtr table;
    ASTPtr columns;
    ASTPtr to_database;
    ASTPtr to_table;
    ASTPtr storage;
    ASTPtr as_database;
    ASTPtr as_table;
    ASTPtr select;
    String cluster_str;
    bool attach = false;
    bool if_not_exists = false;
    bool is_view = false;
    bool is_materialized_view = false;
    bool is_populate = false;
    bool is_temporary = false;

    if (!s_create.ignore(pos, expected))
    {
        if (s_attach.ignore(pos, expected))
            attach = true;
        else
            return false;
    }

    if (s_temporary.ignore(pos, expected))
    {
        is_temporary = true;
    }

    if (s_table.ignore(pos, expected))
    {
        if (s_if_not_exists.ignore(pos, expected))
            if_not_exists = true;

        if (!name_p.parse(pos, table, expected))
            return false;

        if (s_dot.ignore(pos, expected))
        {
            database = table;
            if (!name_p.parse(pos, table, expected))
                return false;
        }

        if (ParserKeyword{"ON"}.ignore(pos, expected))
        {
            if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                return false;
        }

        // Shortcut for ATTACH a previously detached table
        if (attach && (!pos.isValid() || pos.get().type == TokenType::Semicolon))
        {
            auto query = std::make_shared<ASTCreateQuery>();
            node = query;

            query->attach = attach;
            query->if_not_exists = if_not_exists;
            query->cluster = cluster_str;

            if (database)
                query->database = typeid_cast<ASTIdentifier &>(*database).name;
            if (table)
                query->table = typeid_cast<ASTIdentifier &>(*table).name;

            return true;
        }

        /// List of columns.
        if (s_lparen.ignore(pos, expected))
        {
            if (!columns_p.parse(pos, columns, expected))
                return false;

            if (!s_rparen.ignore(pos, expected))
                return false;

            if (!storage_p.parse(pos, storage, expected) && !is_temporary)
                return false;
        }
        else
        {
            storage_p.parse(pos, storage, expected);

            if (!s_as.ignore(pos, expected))
                return false;

            if (!select_p.parse(pos, select, expected)) /// AS SELECT ...
            {
                /// AS [db.]table
                if (!name_p.parse(pos, as_table, expected))
                    return false;

                if (s_dot.ignore(pos, expected))
                {
                    as_database = as_table;
                    if (!name_p.parse(pos, as_table, expected))
                        return false;
                }

                /// Optional - ENGINE can be specified.
                storage_p.parse(pos, storage, expected);
            }
        }
    }
    else if (is_temporary)
        return false;
    else if (s_database.ignore(pos, expected))
    {
        if (s_if_not_exists.ignore(pos, expected))
            if_not_exists = true;

        if (!name_p.parse(pos, database, expected))
            return false;

        if (ParserKeyword{"ON"}.ignore(pos, expected))
        {
            if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                return false;
        }

        storage_p.parse(pos, storage, expected);
    }
    else
    {
        /// VIEW or MATERIALIZED VIEW
        if (s_materialized.ignore(pos, expected))
        {
            is_materialized_view = true;
        }
        else
            is_view = true;

        if (!s_view.ignore(pos, expected))
            return false;

        if (s_if_not_exists.ignore(pos, expected))
            if_not_exists = true;

        if (!name_p.parse(pos, table, expected))
            return false;

        if (s_dot.ignore(pos, expected))
        {
            database = table;
            if (!name_p.parse(pos, table, expected))
                return false;
        }

        if (ParserKeyword{"ON"}.ignore(pos, expected))
        {
            if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                return false;
        }

        // TO [db.]table
        if (ParserKeyword{"TO"}.ignore(pos, expected))
        {
            if (!name_p.parse(pos, to_table, expected))
                return false;

            if (s_dot.ignore(pos, expected))
            {
                to_database = to_table;
                if (!name_p.parse(pos, to_table, expected))
                    return false;
            }
        }

        /// Optional - a list of columns can be specified. It must fully comply with SELECT.
        if (s_lparen.ignore(pos, expected))
        {
            if (!columns_p.parse(pos, columns, expected))
                return false;

            if (!s_rparen.ignore(pos, expected))
                return false;
        }

        if (is_materialized_view && !to_table)
        {
            /// Internal ENGINE for MATERIALIZED VIEW must be specified.
            if (!storage_p.parse(pos, storage, expected))
                return false;

            if (s_populate.ignore(pos, expected))
                is_populate = true;
        }

        /// AS SELECT ...
        if (!s_as.ignore(pos, expected))
            return false;

        if (!select_p.parse(pos, select, expected))
            return false;
    }

    auto query = std::make_shared<ASTCreateQuery>();
    node = query;

    query->attach = attach;
    query->if_not_exists = if_not_exists;
    query->is_view = is_view;
    query->is_materialized_view = is_materialized_view;
    query->is_populate = is_populate;
    query->is_temporary = is_temporary;

    if (database)
        query->database = typeid_cast<ASTIdentifier &>(*database).name;
    if (table)
        query->table = typeid_cast<ASTIdentifier &>(*table).name;
    query->cluster = cluster_str;

    if (to_database)
        query->to_database = typeid_cast<ASTIdentifier &>(*to_database).name;
    if (to_table)
        query->to_table = typeid_cast<ASTIdentifier &>(*to_table).name;

    query->set(query->columns, columns);
    query->set(query->storage, storage);
    if (as_database)
        query->as_database = typeid_cast<ASTIdentifier &>(*as_database).name;
    if (as_table)
        query->as_table = typeid_cast<ASTIdentifier &>(*as_table).name;
    query->set(query->select, select);

    return true;
}


}
