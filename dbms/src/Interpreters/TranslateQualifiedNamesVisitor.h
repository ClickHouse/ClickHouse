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

/// Visit one node for names qualification. @sa InDepthNodeVisitor.
class TranslateQualifiedNamesMatcher
{
public:
    struct Data
    {
        const NameSet & source_columns;
        const std::vector<TableWithColumnNames> & tables;

        static void setTablesOnly(const std::vector<DatabaseAndTableWithAlias> & tables,
                                  std::vector<TableWithColumnNames> & tables_with_columns)
        {
            tables_with_columns.clear();
            tables_with_columns.reserve(tables.size());
            for (const auto & table : tables)
                tables_with_columns.emplace_back(TableWithColumnNames{table, {}});
        }
    };

    static constexpr const char * label = "TranslateQualifiedNames";

    static std::vector<ASTPtr *> visit(ASTPtr & ast, Data & data);
    static bool needChildVisit(ASTPtr & node, const ASTPtr & child);

private:
    static std::vector<ASTPtr *> visit(ASTIdentifier & node, ASTPtr & ast, Data &);
    static std::vector<ASTPtr *> visit(const ASTQualifiedAsterisk & node, const ASTPtr & ast, Data &);
    static std::vector<ASTPtr *> visit(ASTTableJoin & node, const ASTPtr & ast, Data &);
    static std::vector<ASTPtr *> visit(ASTSelectQuery & node, const ASTPtr & ast, Data &);
};

/// Visits AST for names qualification.
/// It finds columns (general identifiers and asterisks) and translate their names according to tables' names.
using TranslateQualifiedNamesVisitor = InDepthNodeVisitor<TranslateQualifiedNamesMatcher, true>;

}
