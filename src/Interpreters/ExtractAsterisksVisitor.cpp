#include <Interpreters/ExtractAsterisksVisitor.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Core/Defines.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_IDENTIFIER;
    extern const int LOGICAL_ERROR;
}

void ExtractAsterisksMatcher::Data::addTableColumns(
    const String & table_name,
    ASTs & columns,
    ShouldAddColumnPredicate should_add_column_predicate)
{
    auto it = table_columns.find(table_name);
    if (it == table_columns.end())
    {
        auto table_name_it = table_name_alias.find(table_name);
        if (table_name_it != table_name_alias.end())
        {
            it = table_columns.find(table_name_it->second);
            if (it == table_columns.end())
                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unknown qualified identifier: {}", table_name);
        }
        else
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unknown qualified identifier: {}", table_name);
    }
    for (const auto & column : it->second)
    {
        if (should_add_column_predicate(column.name))
        {
            ASTPtr identifier;
            if (it->first.empty())
                /// We want tables from JOIN to have aliases.
                /// But it is possible to set joined_subquery_requires_alias = 0,
                /// and write a query like `select * FROM (SELECT 1), (SELECT 1), (SELECT 1)`.
                /// If so, table name will be empty here.
                ///
                /// We cannot create compound identifier with empty part (there is an assert).
                /// So, try our luck and use only column name.
                /// (Rewriting AST for JOIN is not an efficient design).
                identifier = std::make_shared<ASTIdentifier>(column.name);
            else
                identifier = std::make_shared<ASTIdentifier>(std::vector<String>{it->first, column.name});
            columns.emplace_back(std::move(identifier));
        }
    }
}

void ExtractAsterisksMatcher::visit(const ASTPtr & ast, Data & data)
{
    if (auto * t = ast->as<ASTExpressionList>())
        visit(*t, ast, data);
}


void ExtractAsterisksMatcher::visit(const ASTExpressionList & node, const ASTPtr &, Data & data)
{
    bool has_asterisks = false;
    data.new_select_expression_list = std::make_shared<ASTExpressionList>();
    data.new_select_expression_list->children.reserve(node.children.size());
    for (const auto & child : node.children)
    {
        ASTs columns;
        if (const auto * asterisk = child->as<ASTAsterisk>())
        {
            has_asterisks = true;
            for (auto & table_name : data.tables_order)
                data.addTableColumns(table_name, columns);
            if (asterisk->transformers)
            {
                for (const auto & transformer : asterisk->transformers->children)
                    IASTColumnsTransformer::transform(transformer, columns);
            }
        }
        else if (const auto * qualified_asterisk = child->as<ASTQualifiedAsterisk>())
        {
            has_asterisks = true;
            if (!qualified_asterisk->qualifier)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: qualified asterisk must have a qualifier");
            auto & identifier = qualified_asterisk->qualifier->as<ASTIdentifier &>();
            data.addTableColumns(identifier.name(), columns);
            if (qualified_asterisk->transformers)
            {
                for (const auto & transformer : qualified_asterisk->transformers->children)
                {
                    if (transformer->as<ASTColumnsApplyTransformer>() ||
                        transformer->as<ASTColumnsExceptTransformer>() ||
                        transformer->as<ASTColumnsReplaceTransformer>())
                        IASTColumnsTransformer::transform(transformer, columns);
                    else
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: qualified asterisk must only have children of IASTColumnsTransformer type");
                }
            }
        }
        else if (const auto * columns_list_matcher = child->as<ASTColumnsListMatcher>())
        {
            has_asterisks = true;
            for (const auto & ident : columns_list_matcher->column_list->children)
                columns.emplace_back(ident->clone());
            if (columns_list_matcher->transformers)
            {
                for (const auto & transformer : columns_list_matcher->transformers->children)
                    IASTColumnsTransformer::transform(transformer, columns);
            }
        }
        else if (const auto * columns_regexp_matcher = child->as<ASTColumnsRegexpMatcher>())
        {
            has_asterisks = true;
            for (auto & table_name : data.tables_order)
                data.addTableColumns(
                    table_name,
                    columns,
                    [&](const String & column_name) { return columns_regexp_matcher->isColumnMatching(column_name); });
            if (columns_regexp_matcher->transformers)
            {
                for (const auto & transformer : columns_regexp_matcher->transformers->children)
                    IASTColumnsTransformer::transform(transformer, columns);
            }
        }
        else
            data.new_select_expression_list->children.push_back(child);
        data.new_select_expression_list->children.insert(
            data.new_select_expression_list->children.end(),
            std::make_move_iterator(columns.begin()),
            std::make_move_iterator(columns.end()));
    }
    if (!has_asterisks)
        data.new_select_expression_list.reset();
}

}
