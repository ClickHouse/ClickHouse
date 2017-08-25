#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserSelectQuery.h>


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

    Pos begin = pos;

    /// For now `name == 'Nested'`, probably alternative nested data structures will appear
    if (!name_p.parse(pos, name, expected))
        return false;

    if (!open.ignore(pos))
        return false;

    if (!columns_p.parse(pos, columns, expected))
        return false;

    if (!close.ignore(pos))
        return false;

    auto func = std::make_shared<ASTFunction>(StringRange(begin, pos));
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

    Pos begin = pos;

    if (parametric.parse(pos, node, expected))
        return true;

    ASTPtr ident;
    if (non_parametric.parse(pos, ident, expected))
    {
        auto func = std::make_shared<ASTFunction>(StringRange(begin));
        func->name = typeid_cast<ASTIdentifier &>(*ident).name;
        node = func;
        return true;
    }

    return false;
}

bool ParserTypeInCastExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (ParserIdentifierWithOptionalParameters::parseImpl(pos, node, expected))
    {
        const auto & id_with_params = typeid_cast<const ASTFunction &>(*node);
        node = std::make_shared<ASTIdentifier>(id_with_params.range, String{ id_with_params.range.first, id_with_params.range.second });
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


bool ParserEngine::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_engine("ENGINE");
    ParserToken s_eq(TokenType::Equals);
    ParserIdentifierWithOptionalParameters storage_p;

    if (s_engine.ignore(pos, expected))
    {
        if (!s_eq.ignore(pos, expected))
            return false;

        if (!storage_p.parse(pos, node, expected))
            return false;
    }

    return true;
}


bool ParserCreateQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Pos begin = pos;

    ParserKeyword s_create("CREATE");
    ParserKeyword s_temporary("TEMPORARY");
    ParserKeyword s_attach("ATTACH");
    ParserKeyword s_table("TABLE");
    ParserKeyword s_database("DATABASE");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserKeyword s_as("AS");
    ParserKeyword s_select("SELECT");
    ParserKeyword s_view("VIEW");
    ParserKeyword s_materialized("MATERIALIZED");
    ParserKeyword s_populate("POPULATE");
    ParserToken s_dot(TokenType::Dot);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserEngine engine_p;
    ParserIdentifier name_p;
    ParserColumnDeclarationList columns_p;

    ASTPtr database;
    ASTPtr table;
    ASTPtr columns;
    ASTPtr storage;
    ASTPtr inner_storage;
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

    if (s_database.ignore(pos, expected))
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

        engine_p.parse(pos, storage, expected);
    }
    else if (s_table.ignore(pos, expected))
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

        /// List of columns.
        if (s_lparen.ignore(pos, expected))
        {
            if (!columns_p.parse(pos, columns, expected))
                return false;

            if (!s_rparen.ignore(pos, expected))
                return false;

            if (!engine_p.parse(pos, storage, expected))
                return false;

            /// For engine VIEW, you also need to read AS SELECT
            if (storage && (typeid_cast<ASTFunction &>(*storage).name == "View"
                        || typeid_cast<ASTFunction &>(*storage).name == "MaterializedView"))
            {
                if (!s_as.ignore(pos, expected))
                    return false;
                Pos before_select = pos;
                if (!s_select.ignore(pos, expected))
                    return false;
                pos = before_select;
                ParserSelectQuery select_p;
                select_p.parse(pos, select, expected);
            }
        }
        else
        {
            engine_p.parse(pos, storage, expected);

            if (!s_as.ignore(pos, expected))
                return false;

            /// AS SELECT ...
            Pos before_select = pos;
            if (s_select.ignore(pos, expected))
            {
                pos = before_select;
                ParserSelectQuery select_p;
                select_p.parse(pos, select, expected);
            }
            else
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
                engine_p.parse(pos, storage, expected);
            }
        }
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

        /// Optional - a list of columns can be specified. It must fully comply with SELECT.
        if (s_lparen.ignore(pos, expected))
        {
            if (!columns_p.parse(pos, columns, expected))
                return false;

            if (!s_rparen.ignore(pos, expected))
                return false;
        }

        /// Optional - internal ENGINE for MATERIALIZED VIEW can be specified
        engine_p.parse(pos, inner_storage, expected);

        if (s_populate.ignore(pos, expected))
            is_populate = true;

        /// AS SELECT ...
        if (!s_as.ignore(pos, expected))
            return false;

        ParserSelectQuery select_p;
        if (!select_p.parse(pos, select, expected))
            return false;
    }

    auto query = std::make_shared<ASTCreateQuery>(StringRange(begin, pos));
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
    if (inner_storage)
        query->inner_storage = inner_storage;

    query->columns = columns;
    query->storage = storage;
    if (as_database)
        query->as_database = typeid_cast<ASTIdentifier &>(*as_database).name;
    if (as_table)
        query->as_table = typeid_cast<ASTIdentifier &>(*as_table).name;
    query->select = select;

    if (columns)
        query->children.push_back(columns);
    if (storage)
        query->children.push_back(storage);
    if (select)
        query->children.push_back(select);
    if (inner_storage)
        query->children.push_back(inner_storage);

    return true;
}


}
