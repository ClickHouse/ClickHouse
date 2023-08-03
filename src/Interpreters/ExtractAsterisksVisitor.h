#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/Aliases.h>
#include <Parsers/ASTExpressionList.h>

namespace DB
{

class ASTSelectQuery;
class Context;

/// Replace asterisks in select_expression_list with column identifiers
class ExtractAsterisksMatcher
{
public:
    struct Data
    {
        std::unordered_map<String, NamesAndTypesList> table_columns;
        std::unordered_map<String, String> table_name_alias;
        std::vector<String> tables_order;
        std::shared_ptr<ASTExpressionList> new_select_expression_list;

        explicit Data(const std::vector<TableWithColumnNamesAndTypes> & tables)
        {
            tables_order.reserve(tables.size());
            for (const auto & table : tables)
            {
                String table_name = table.table.getQualifiedNamePrefix(false);
                NamesAndTypesList columns = table.columns;
                tables_order.push_back(table_name);
                table_name_alias.emplace(table.table.table /* table_name */, table_name /* alias_name */);
                table_columns.emplace(std::move(table_name), std::move(columns));
            }
        }

        using ShouldAddColumnPredicate = std::function<bool (const String&)>;

        /// Add columns from table with table_name into select expression list
        /// Use should_add_column_predicate for check if column name should be added
        /// By default should_add_column_predicate returns true for any column name
        void addTableColumns(
            const String & table_name,
            ASTs & columns,
            ShouldAddColumnPredicate should_add_column_predicate = [](const String &) { return true; });
    };

    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return false; }
    static void visit(const ASTPtr & ast, Data & data);

private:
    static void visit(const ASTExpressionList & node, const ASTPtr &, Data & data);
};

using ExtractAsterisksVisitor = ConstInDepthNodeVisitor<ExtractAsterisksMatcher, true>;

}
