#pragma once

#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTSelectQuery;
class Context;

/// AST transformer. It replaces multiple joins to (subselect + join) track.
/// 'select * from t1 join t2 on ... join t3 on ... join t4 on ...' would be rewriten with
/// 'select * from (select * from t1 join t2 on ...) join t3 on ...) join t4 on ...'
class JoinToSubqueryTransformMatcher
{
public:
    struct Data
    {
        const Context & context;
        bool done = false;
    };

    static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }
    static void visit(ASTPtr & ast, Data & data);

private:
    /// - combines two source TablesInSelectQueryElement into resulting one (Subquery)
    /// - adds table hidings to ASTSelectQuery.with_expression_list
    ///
    /// TablesInSelectQueryElement [result]
    ///  TableExpression
    ///   Subquery (alias __join1)
    ///    SelectWithUnionQuery
    ///      ExpressionList
    ///       SelectQuery
    ///        ExpressionList
    ///         Asterisk
    ///        TablesInSelectQuery
    ///         TablesInSelectQueryElement [source1]
    ///         TablesInSelectQueryElement [source2]
    ///
    static void visit(ASTSelectQuery & select, ASTPtr & ast, Data & data);

    /// @return combined TablesInSelectQueryElement or nullptr if cannot rewrite
    static ASTPtr replaceJoin(ASTPtr left, ASTPtr right);
};

using JoinToSubqueryTransformVisitor = InDepthNodeVisitor<JoinToSubqueryTransformMatcher, true>;

}
