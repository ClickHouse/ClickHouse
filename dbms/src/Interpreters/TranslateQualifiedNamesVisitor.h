#pragma once

#include <vector>

#include <Core/Names.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTIdentifier;
class ASTQualifiedAsterisk;
struct ASTTableJoin;
class ASTSelectQuery;
class ASTExpressionList;
class ASTFunction;

/// Visit one node for names qualification. @sa InDepthNodeVisitor.
class TranslateQualifiedNamesMatcher
{
public:
    using Visitor = InDepthNodeVisitor<TranslateQualifiedNamesMatcher, true>;

    struct Data
    {
        NameSet source_columns;
        const std::vector<TableWithColumnNames> & tables;
        std::unordered_set<String> join_using_columns;
        bool has_columns;

        Data(const NameSet & source_columns_, const std::vector<TableWithColumnNames> & tables_, bool has_columns_ = true)
            : source_columns(source_columns_)
            , tables(tables_)
            , has_columns(has_columns_)
        {}

        static std::vector<TableWithColumnNames> tablesOnly(const std::vector<DatabaseAndTableWithAlias> & tables)
        {
            std::vector<TableWithColumnNames> tables_with_columns;
            tables_with_columns.reserve(tables.size());

            for (const auto & table : tables)
                tables_with_columns.emplace_back(TableWithColumnNames{table, {}});
            return tables_with_columns;
        }

        bool processAsterisks() const { return !tables.empty() && has_columns; }
    };

    static void visit(ASTPtr & ast, Data & data);
    static bool needChildVisit(ASTPtr & node, const ASTPtr & child);

private:
    static void visit(ASTIdentifier & node, ASTPtr & ast, Data &);
    static void visit(const ASTQualifiedAsterisk & node, const ASTPtr & ast, Data &);
    static void visit(ASTTableJoin & node, const ASTPtr & ast, Data &);
    static void visit(ASTSelectQuery & node, const ASTPtr & ast, Data &);
    static void visit(ASTExpressionList &, const ASTPtr &, Data &);
    static void visit(ASTFunction &, const ASTPtr &, Data &);

    static void extractJoinUsingColumns(const ASTPtr ast, Data & data);
};

/// Visits AST for names qualification.
/// It finds columns and translate their names to the normal form. Expand asterisks and qualified asterisks with column names.
using TranslateQualifiedNamesVisitor = TranslateQualifiedNamesMatcher::Visitor;

}
