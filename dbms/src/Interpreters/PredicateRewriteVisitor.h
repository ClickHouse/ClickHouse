#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class PredicateRewriteVisitorData
{
public:
    using TypeToVisit = ASTSelectWithUnionQuery;

    bool isRewrite() const { return is_rewrite; }

    void visit(ASTSelectWithUnionQuery & union_select_query, ASTPtr &);

    PredicateRewriteVisitorData(const Context & context_, const ASTs & predicates_, const Names & colunm_names_, bool optimize_final_);

private:
    const Context & context;
    const ASTs & predicates;
    const Names & column_names;

    bool optimize_final;
    bool is_rewrite = false;

    void visitFirstInternalSelect(ASTSelectQuery & select_query, ASTPtr &);

    void visitOtherInternalSelect(ASTSelectQuery & select_query, ASTPtr &);

    bool allowPushDown(const ASTSelectQuery & subquery, NameSet & aggregate_column);

    bool rewriteSubquery(ASTSelectQuery & subquery, const Names & outer_columns, const Names & inner_columns);
};

using PredicateRewriteMatcher = OneTypeMatcher<PredicateRewriteVisitorData, false>;
using PredicateRewriteVisitor = InDepthNodeVisitor<PredicateRewriteMatcher, true>;
}
