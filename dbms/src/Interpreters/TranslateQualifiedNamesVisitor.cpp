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
    if (typeid_cast<ASTTableExpression *>(child.get()) ||
        typeid_cast<ASTSelectWithUnionQuery *>(child.get()))
        return false;

    /// Processed nodes. Do not go into children.
    if (typeid_cast<ASTQualifiedAsterisk *>(node.get()) ||
        typeid_cast<ASTTableJoin *>(node.get()))
        return false;

    /// ASTSelectQuery + others
    return true;
}

void TranslateQualifiedNamesMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * t = typeid_cast<ASTIdentifier *>(ast.get()))
        visit(*t, ast, data);
    if (auto * t = typeid_cast<ASTTableJoin *>(ast.get()))
        visit(*t, ast, data);
    if (auto * t = typeid_cast<ASTSelectQuery *>(ast.get()))
        visit(*t, ast, data);
    if (auto * node = typeid_cast<ASTExpressionList *>(ast.get()))
        visit(*node, ast, data);
    if (auto * node = typeid_cast<ASTFunction *>(ast.get()))
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
        typeid_cast<const ASTAsterisk *>(func_arguments->children[0].get()))
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
    std::vector<ASTPtr *> out;
    if (join.using_expression_list)
        out.push_back(&join.using_expression_list);
    else if (join.on_expression)
        out.push_back(&join.on_expression);

    for (ASTPtr * add_node : out)
        Visitor(data).visit(*add_node);
}

void TranslateQualifiedNamesMatcher::visit(ASTSelectQuery & select, const ASTPtr & , Data & data)
{
    if (auto join = select.join())
        extractJoinUsingColumns(join->table_join, data);

    /// If the WHERE clause or HAVING consists of a single qualified column, the reference must be translated not only in children,
    /// but also in where_expression and having_expression.
    std::vector<ASTPtr *> out;
    if (select.prewhere_expression)
        out.push_back(&select.prewhere_expression);
    if (select.where_expression)
        out.push_back(&select.where_expression);
    if (select.having_expression)
        out.push_back(&select.having_expression);

    for (ASTPtr * add_node : out)
        Visitor(data).visit(*add_node);
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
            if (typeid_cast<const ASTAsterisk *>(child.get()))
            {
                if (tables_with_columns.empty())
                    throw Exception("An asterisk cannot be replaced with empty columns.", ErrorCodes::LOGICAL_ERROR);
                has_asterisk = true;
                break;
            }
            else if (auto qa = typeid_cast<const ASTQualifiedAsterisk *>(child.get()))
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
        if (const auto * asterisk = typeid_cast<const ASTAsterisk *>(child.get()))
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
        else if (const auto * qualified_asterisk = typeid_cast<const ASTQualifiedAsterisk *>(child.get()))
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
    const auto & table_join = typeid_cast<const ASTTableJoin &>(*ast);

    if (table_join.using_expression_list)
    {
        auto & keys = typeid_cast<ASTExpressionList &>(*table_join.using_expression_list);
        for (const auto & key : keys.children)
            if (auto opt_column = getIdentifierName(key))
                data.join_using_columns.insert(*opt_column);
            else if (typeid_cast<const ASTLiteral *>(key.get()))
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
