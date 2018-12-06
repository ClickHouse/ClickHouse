#include <Interpreters/TranslateQualifiedNamesVisitor.h>

#include <Common/typeid_cast.h>
#include <Core/Names.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>


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
    if (typeid_cast<ASTIdentifier *>(node.get()) ||
        typeid_cast<ASTQualifiedAsterisk *>(node.get()) ||
        typeid_cast<ASTTableJoin *>(node.get()))
        return false;

    /// ASTSelectQuery + others
    return true;
}

std::vector<ASTPtr> TranslateQualifiedNamesMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * t = typeid_cast<ASTIdentifier *>(ast.get()))
        return visit(*t, ast, data);
    if (auto * t = typeid_cast<ASTQualifiedAsterisk *>(ast.get()))
        return visit(*t, ast, data);
    if (auto * t = typeid_cast<ASTTableJoin *>(ast.get()))
        return visit(*t, ast, data);
    if (auto * t = typeid_cast<ASTSelectQuery *>(ast.get()))
        return visit(*t, ast, data);
    return {};
}

std::vector<ASTPtr> TranslateQualifiedNamesMatcher::visit(const ASTIdentifier & identifier, ASTPtr & ast, Data & data)
{
    const NameSet & source_columns = data.source_columns;
    const std::vector<DatabaseAndTableWithAlias> & tables = data.tables;

    if (identifier.general())
    {
        /// Select first table name with max number of qualifiers which can be stripped.
        size_t max_num_qualifiers_to_strip = 0;
        size_t best_table_pos = 0;

        for (size_t table_pos = 0; table_pos < tables.size(); ++table_pos)
        {
            const auto & table = tables[table_pos];
            auto num_qualifiers_to_strip = getNumComponentsToStripInOrderToTranslateQualifiedName(identifier, table);

            if (num_qualifiers_to_strip > max_num_qualifiers_to_strip)
            {
                max_num_qualifiers_to_strip = num_qualifiers_to_strip;
                best_table_pos = table_pos;
            }
        }

        if (max_num_qualifiers_to_strip)
            stripIdentifier(ast, max_num_qualifiers_to_strip);

        /// In case if column from the joined table are in source columns, change it's name to qualified.
        if (best_table_pos && source_columns.count(ast->getColumnName()))
        {
            const DatabaseAndTableWithAlias & table = tables[best_table_pos];
            table.makeQualifiedName(ast);
        }
    }

    return {};
}

std::vector<ASTPtr> TranslateQualifiedNamesMatcher::visit(const ASTQualifiedAsterisk & , const ASTPtr & ast, Data & data)
{
    const std::vector<DatabaseAndTableWithAlias> & tables = data.tables;

    if (ast->children.size() != 1)
        throw Exception("Logical error: qualified asterisk must have exactly one child", ErrorCodes::LOGICAL_ERROR);

    ASTIdentifier * ident = typeid_cast<ASTIdentifier *>(ast->children[0].get());
    if (!ident)
        throw Exception("Logical error: qualified asterisk must have identifier as its child", ErrorCodes::LOGICAL_ERROR);

    size_t num_components = ident->children.size();
    if (num_components > 2)
        throw Exception("Qualified asterisk cannot have more than two qualifiers", ErrorCodes::UNKNOWN_ELEMENT_IN_AST);

    DatabaseAndTableWithAlias db_and_table(*ident);

    for (const auto & table_names : tables)
    {
        /// database.table.*, table.* or alias.*
        if (num_components == 2)
        {
            if (!table_names.database.empty() &&
                db_and_table.database == table_names.database &&
                db_and_table.table == table_names.table)
                return {};
        }
        else if (num_components == 0)
        {
            if ((!table_names.table.empty() && db_and_table.table == table_names.table) ||
                (!table_names.alias.empty() && db_and_table.table == table_names.alias))
                return {};
        }
    }

    throw Exception("Unknown qualified identifier: " + ident->getAliasOrColumnName(), ErrorCodes::UNKNOWN_IDENTIFIER);
}

std::vector<ASTPtr> TranslateQualifiedNamesMatcher::visit(const ASTTableJoin & join, const ASTPtr & , Data &)
{
    /// Don't translate on_expression here in order to resolve equation parts later.
    std::vector<ASTPtr> out;
    if (join.using_expression_list)
        out.push_back(join.using_expression_list);
    return out;
}

std::vector<ASTPtr> TranslateQualifiedNamesMatcher::visit(const ASTSelectQuery & select, const ASTPtr & , Data &)
{
    /// If the WHERE clause or HAVING consists of a single quailified column, the reference must be translated not only in children,
    /// but also in where_expression and having_expression.
    std::vector<ASTPtr> out;
    if (select.prewhere_expression)
        out.push_back(select.prewhere_expression);
    if (select.where_expression)
        out.push_back(select.where_expression);
    if (select.having_expression)
        out.push_back(select.having_expression);
    return out;
}

}
