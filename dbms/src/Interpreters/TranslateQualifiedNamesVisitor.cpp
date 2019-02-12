#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/IdentifierSemantic.h>

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
    if (typeid_cast<ASTQualifiedAsterisk *>(node.get()) ||
        typeid_cast<ASTTableJoin *>(node.get()))
        return false;

    /// ASTSelectQuery + others
    return true;
}

std::vector<ASTPtr *> TranslateQualifiedNamesMatcher::visit(ASTPtr & ast, Data & data)
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

std::vector<ASTPtr *> TranslateQualifiedNamesMatcher::visit(ASTIdentifier & identifier, ASTPtr &, Data & data)
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

        /// In case if column from the joined table are in source columns, change it's name to qualified.
        if (best_table_pos && data.source_columns.count(identifier.shortName()))
            IdentifierSemantic::setNeedLongName(identifier, true);
        IdentifierSemantic::setColumnNormalName(identifier, data.tables[best_table_pos].first);
    }

    return {};
}

std::vector<ASTPtr *> TranslateQualifiedNamesMatcher::visit(const ASTQualifiedAsterisk & , const ASTPtr & ast, Data & data)
{
    if (ast->children.size() != 1)
        throw Exception("Logical error: qualified asterisk must have exactly one child", ErrorCodes::LOGICAL_ERROR);

    auto & ident = ast->children[0];

    /// @note it could contain table alias as table name.
    DatabaseAndTableWithAlias db_and_table(ident);

    for (const auto & known_table : data.tables)
        if (db_and_table.satisfies(known_table.first, true))
            return {};

    throw Exception("Unknown qualified identifier: " + ident->getAliasOrColumnName(), ErrorCodes::UNKNOWN_IDENTIFIER);
}

std::vector<ASTPtr *> TranslateQualifiedNamesMatcher::visit(ASTTableJoin & join, const ASTPtr & , Data &)
{
    std::vector<ASTPtr *> out;
    if (join.using_expression_list)
        out.push_back(&join.using_expression_list);
    else if (join.on_expression)
        out.push_back(&join.on_expression);
    return out;
}

std::vector<ASTPtr *> TranslateQualifiedNamesMatcher::visit(ASTSelectQuery & select, const ASTPtr & , Data &)
{
    /// If the WHERE clause or HAVING consists of a single qualified column, the reference must be translated not only in children,
    /// but also in where_expression and having_expression.
    std::vector<ASTPtr *> out;
    if (select.prewhere_expression)
        out.push_back(&select.prewhere_expression);
    if (select.where_expression)
        out.push_back(&select.where_expression);
    if (select.having_expression)
        out.push_back(&select.having_expression);
    return out;
}

}
