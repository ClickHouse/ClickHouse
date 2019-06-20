#include <Poco/String.h>

#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/AsteriskSemantic.h>

#include <Common/typeid_cast.h>
#include <Core/Names.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <iostream>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_IDENTIFIER;
    extern const int UNKNOWN_ELEMENT_IN_AST;
    extern const int LOGICAL_ERROR;
}

bool TranslateQualifiedNamesMatcher::needChildVisit(ASTPtr & node, const ASTPtr & child)
{
    /// Do not go to FROM, JOIN, subqueries.
    if (child->as<ASTTableExpression>() || child->as<ASTSelectWithUnionQuery>())
        return false;

    /// Processed nodes. Do not go into children.
    if (node->as<ASTQualifiedAsterisk>() || node->as<ASTTableJoin>())
        return false;

    /// ASTSelectQuery + others
    return true;
}

void TranslateQualifiedNamesMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * t = ast->as<ASTIdentifier>())
        visit(*t, ast, data);
    if (auto * t = ast->as<ASTTableJoin>())
        visit(*t, ast, data);
    if (auto * t = ast->as<ASTSelectQuery>())
        visit(*t, ast, data);
    if (auto * node = ast->as<ASTExpressionList>())
        visit(*node, ast, data);
    if (auto * node = ast->as<ASTFunction>())
        visit(*node, ast, data);
}

void TranslateQualifiedNamesMatcher::visit(ASTIdentifier & identifier, ASTPtr &, Data & data)
{
    if (IdentifierSemantic::getColumnName(identifier))
    {
        size_t best_table_pos = 0;
        size_t best_match = 0;
        for (size_t i = 0; i < data.tables.size(); ++i)
            if (size_t match = IdentifierSemantic::canReferColumnToTable(identifier, data.tables[i].first))
                if (match > best_match)
                {
                    best_match = match;
                    best_table_pos = i;
                }

        if (best_match)
            IdentifierSemantic::setMembership(identifier, best_table_pos + 1);

        /// In case if column from the joined table are in source columns, change it's name to qualified.
        if (best_table_pos && data.source_columns.count(identifier.shortName()))
            IdentifierSemantic::setNeedLongName(identifier, true);
        if (!data.tables.empty())
            IdentifierSemantic::setColumnNormalName(identifier, data.tables[best_table_pos].first);
    }
}

/// As special case, treat count(*) as count(), not as count(list of all columns).
void TranslateQualifiedNamesMatcher::visit(ASTFunction & node, const ASTPtr &, Data &)
{
    ASTPtr & func_arguments = node.arguments;

    String func_name_lowercase = Poco::toLower(node.name);
    if (func_name_lowercase == "count" &&
        func_arguments->children.size() == 1 &&
        func_arguments->children[0]->as<ASTAsterisk>())
        func_arguments->children.clear();
}

void TranslateQualifiedNamesMatcher::visit(const ASTQualifiedAsterisk & , const ASTPtr & ast, Data & data)
{
    if (ast->children.size() != 1)
        throw Exception("Logical error: qualified asterisk must have exactly one child", ErrorCodes::LOGICAL_ERROR);

    auto & ident = ast->children[0];

    /// @note it could contain table alias as table name.
    DatabaseAndTableWithAlias db_and_table(ident);

    for (const auto & known_table : data.tables)
        if (db_and_table.satisfies(known_table.first, true))
            return;

    throw Exception("Unknown qualified identifier: " + ident->getAliasOrColumnName(), ErrorCodes::UNKNOWN_IDENTIFIER);
}

void TranslateQualifiedNamesMatcher::visit(ASTTableJoin & join, const ASTPtr & , Data & data)
{
    if (join.using_expression_list)
        Visitor(data).visit(join.using_expression_list);
    else if (join.on_expression)
        Visitor(data).visit(join.on_expression);
}

void TranslateQualifiedNamesMatcher::visit(ASTSelectQuery & select, const ASTPtr & , Data & data)
{
    if (auto join = select.join())
        extractJoinUsingColumns(join->table_join, data);

#if 1 /// TODO: legacy?
    /// If the WHERE clause or HAVING consists of a single qualified column, the reference must be translated not only in children,
    /// but also in where_expression and having_expression.
    if (select.prewhere())
        Visitor(data).visit(select.refPrewhere());
    if (select.where())
        Visitor(data).visit(select.refWhere());
    if (select.having())
        Visitor(data).visit(select.refHaving());
#endif
}

static void addIdentifier(ASTs & nodes, const String & table_name, const String & column_name, AsteriskSemantic::RevertedAliasesPtr aliases)
{
    auto identifier = std::make_shared<ASTIdentifier>(std::vector<String>{table_name, column_name});

    bool added = false;
    if (aliases && aliases->count(identifier->name))
    {
        for (const String & alias : (*aliases)[identifier->name])
        {
            nodes.push_back(identifier->clone());
            nodes.back()->setAlias(alias);
            added = true;
        }
    }

    if (!added)
        nodes.emplace_back(identifier);
}

/// Replace *, alias.*, database.table.* with a list of columns.
void TranslateQualifiedNamesMatcher::visit(ASTExpressionList & node, const ASTPtr &, Data & data)
{
    const auto & tables_with_columns = data.tables;

    ASTs old_children;
    if (data.processAsterisks())
    {
        bool has_asterisk = false;
        for (const auto & child : node.children)
        {
            if (child->as<ASTAsterisk>())
            {
                if (tables_with_columns.empty())
                    throw Exception("An asterisk cannot be replaced with empty columns.", ErrorCodes::LOGICAL_ERROR);
                has_asterisk = true;
                break;
            }
            else if (const auto * qa = child->as<ASTQualifiedAsterisk>())
            {
                visit(*qa, child, data); /// check if it's OK before rewrite
                has_asterisk = true;
                break;
            }
        }

        if (has_asterisk)
        {
            old_children.swap(node.children);
            node.children.reserve(old_children.size());
        }
    }

    for (const auto & child : old_children)
    {
        if (const auto * asterisk = child->as<ASTAsterisk>())
        {
            bool first_table = true;
            for (const auto & [table, table_columns] : tables_with_columns)
            {
                for (const auto & column_name : table_columns)
                {
                    if (first_table || !data.join_using_columns.count(column_name))
                    {
                        String table_name = table.getQualifiedNamePrefix(false);
                        addIdentifier(node.children, table_name, column_name, AsteriskSemantic::getAliases(*asterisk));
                    }
                }

                first_table = false;
            }
        }
        else if (const auto * qualified_asterisk = child->as<ASTQualifiedAsterisk>())
        {
            DatabaseAndTableWithAlias ident_db_and_name(qualified_asterisk->children[0]);

            for (const auto & [table, table_columns] : tables_with_columns)
            {
                if (ident_db_and_name.satisfies(table, true))
                {
                    for (const auto & column_name : table_columns)
                    {
                        String table_name = table.getQualifiedNamePrefix(false);
                        addIdentifier(node.children, table_name, column_name, AsteriskSemantic::getAliases(*qualified_asterisk));
                    }
                    break;
                }
            }
        }
        else
            node.children.emplace_back(child);
    }
}

/// 'select * from a join b using id' should result one 'id' column
void TranslateQualifiedNamesMatcher::extractJoinUsingColumns(const ASTPtr ast, Data & data)
{
    const auto & table_join = ast->as<ASTTableJoin &>();

    if (table_join.using_expression_list)
    {
        const auto & keys = table_join.using_expression_list->as<ASTExpressionList &>();
        for (const auto & key : keys.children)
            if (auto opt_column = getIdentifierName(key))
                data.join_using_columns.insert(*opt_column);
            else if (key->as<ASTLiteral>())
                data.join_using_columns.insert(key->getColumnName());
            else
            {
                String alias = key->tryGetAlias();
                if (alias.empty())
                    throw Exception("Logical error: expected identifier or alias, got: " + key->getID(), ErrorCodes::LOGICAL_ERROR);
                data.join_using_columns.insert(alias);
            }
    }
}

}
