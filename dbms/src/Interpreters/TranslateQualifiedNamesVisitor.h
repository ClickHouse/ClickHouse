#pragma once

#include <vector>

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
        const std::vector<DatabaseAndTableWithAlias> & tables;
    };

    static constexpr const char * label = __FILE__;

    static std::vector<ASTPtr> visit(ASTPtr & ast, Data & data);
    static bool needChildVisit(ASTPtr & node, const ASTPtr & child);

private:
    static std::vector<ASTPtr> visit(const ASTIdentifier & node, ASTPtr & ast, Data &);
    static std::vector<ASTPtr> visit(const ASTQualifiedAsterisk & node, const ASTPtr & ast, Data &);
    static std::vector<ASTPtr> visit(const ASTTableJoin & node, const ASTPtr & ast, Data &);
    static std::vector<ASTPtr> visit(const ASTSelectQuery & node, const ASTPtr & ast, Data &);
};

/// Visits AST for names qualification.
/// It finds columns (general identifiers and asterisks) and translate their names according to tables' names.
using TranslateQualifiedNamesVisitor = InDepthNodeVisitor<TranslateQualifiedNamesMatcher, true>;

}
