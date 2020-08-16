#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTSelectQuery;
struct SettingChange;
class SettingsChanges;

/// Pushdown SETTINGS clause that goes after FORMAT to the SELECT query:
/// (since settings after FORMAT parsed separatelly not in the ParserSelectQuery but in ParserQueryWithOutput)
///
///     SELECT 1                             FORMAT Null SETTINGS max_block_size = 1 ->
///     SELECT 1 SETTINGS max_block_size = 1 FORMAT Null SETTINGS max_block_size = 1
///
/// Otherwise settings after FORMAT will not be applied.
class QueryWithOutputSettingsPushDownMatcher
{
public:
    using Visitor = InDepthNodeVisitor<QueryWithOutputSettingsPushDownMatcher, true>;

    struct Data
    {
        const ASTPtr & settings_ast;
    };

    static bool needChildVisit(ASTPtr & node, const ASTPtr & child);
    static void visit(ASTPtr & ast, Data & data);

private:
    static void visit(ASTSelectQuery &, ASTPtr &, Data &);
};

using QueryWithOutputSettingsPushDownVisitor = QueryWithOutputSettingsPushDownMatcher::Visitor;

}
