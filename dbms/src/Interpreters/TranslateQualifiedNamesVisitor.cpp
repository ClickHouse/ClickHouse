#include <Interpreters/TranslateQualifiedNamesVisitor.h>

#include <Core/NamesAndTypes.h>

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
}

void TranslateQualifiedNamesVisitor::visit(ASTIdentifier * identifier, ASTPtr & ast, const DumpASTNode & dump) const
{
    if (identifier->general())
    {
        /// Select first table name with max number of qualifiers which can be stripped.
        size_t max_num_qualifiers_to_strip = 0;
        size_t best_table_pos = 0;

        for (size_t table_pos = 0; table_pos < tables.size(); ++table_pos)
        {
            const auto & table = tables[table_pos];
            auto num_qualifiers_to_strip = getNumComponentsToStripInOrderToTranslateQualifiedName(*identifier, table);

            if (num_qualifiers_to_strip > max_num_qualifiers_to_strip)
            {
                max_num_qualifiers_to_strip = num_qualifiers_to_strip;
                best_table_pos = table_pos;
            }
        }

        if (max_num_qualifiers_to_strip)
        {
            dump.print(String("stripIdentifier ") + identifier->name, max_num_qualifiers_to_strip);
            stripIdentifier(ast, max_num_qualifiers_to_strip);
        }

        /// In case if column from the joined table are in source columns, change it's name to qualified.
        if (best_table_pos && source_columns.contains(ast->getColumnName()))
        {
            const DatabaseAndTableWithAlias & table = tables[best_table_pos];
            table.makeQualifiedName(ast);
            dump.print("makeQualifiedName", table.database + '.' + table.table + ' ' + ast->getColumnName());
        }
    }
}

void TranslateQualifiedNamesVisitor::visit(ASTQualifiedAsterisk *, ASTPtr & ast, const DumpASTNode &) const
{
    if (ast->children.size() != 1)
        throw Exception("Logical error: qualified asterisk must have exactly one child", ErrorCodes::LOGICAL_ERROR);

    ASTIdentifier * ident = typeid_cast<ASTIdentifier *>(ast->children[0].get());
    if (!ident)
        throw Exception("Logical error: qualified asterisk must have identifier as its child", ErrorCodes::LOGICAL_ERROR);

    size_t num_components = ident->children.size();
    if (num_components > 2)
        throw Exception("Qualified asterisk cannot have more than two qualifiers", ErrorCodes::UNKNOWN_ELEMENT_IN_AST);

    std::pair<String, String> db_and_table = getDatabaseAndTableNameFromIdentifier(*ident);

    for (const auto & table_names : tables)
    {
        /// database.table.*, table.* or alias.*
        if (num_components == 2)
        {
            if (!table_names.database.empty() &&
                db_and_table.first == table_names.database &&
                db_and_table.second == table_names.table)
                return;
        }
        else if (num_components == 0)
        {
            if ((!table_names.table.empty() && db_and_table.second == table_names.table) ||
                (!table_names.alias.empty() && db_and_table.second == table_names.alias))
                return;
        }
    }

    throw Exception("Unknown qualified identifier: " + ident->getAliasOrColumnName(), ErrorCodes::UNKNOWN_IDENTIFIER);
}

void TranslateQualifiedNamesVisitor::visit(ASTTableJoin * join, ASTPtr &, const DumpASTNode &) const
{
    /// Don't translate on_expression here in order to resolve equation parts later.
    if (join->using_expression_list)
        visit(join->using_expression_list);
}

void TranslateQualifiedNamesVisitor::visit(ASTSelectQuery * select, ASTPtr & ast, const DumpASTNode &) const
{
    /// If the WHERE clause or HAVING consists of a single quailified column, the reference must be translated not only in children,
    /// but also in where_expression and having_expression.
    if (select->prewhere_expression)
        visit(select->prewhere_expression);
    if (select->where_expression)
        visit(select->where_expression);
    if (select->having_expression)
        visit(select->having_expression);

    visitChildren(ast);
}

void TranslateQualifiedNamesVisitor::visitChildren(ASTPtr & ast) const
{
    for (auto & child : ast->children)
    {
        /// Do not go to FROM, JOIN, subqueries.
        if (!typeid_cast<const ASTTableExpression *>(child.get())
            && !typeid_cast<const ASTSelectWithUnionQuery *>(child.get()))
        {
            visit(child);
        }
    }
}

}
