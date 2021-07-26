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
    bool is_rewrite = false;
    using TypeToVisit = ASTSelectWithUnionQuery;

    void visit(ASTSelectWithUnionQuery & union_select_query, ASTPtr &);

    static bool needChild(const ASTPtr & node, const ASTPtr &)
    {
        if (node && node->as<TypeToVisit>())
            return false;

        return true;
    }

    PredicateRewriteVisitorData(const Context & context_, const ASTs & predicates_, Names && column_names_, bool optimize_final_, bool optimize_with_);

private:
    const Context & context;
    const ASTs & predicates;
    const Names column_names;
    bool optimize_final;
    bool optimize_with;

    void visitFirstInternalSelect(ASTSelectQuery & select_query, ASTPtr &);

    void visitOtherInternalSelect(ASTSelectQuery & select_query, ASTPtr &);

    bool rewriteSubquery(ASTSelectQuery & subquery, const Names & outer_columns, const Names & inner_columns);
};

using PredicateRewriteMatcher = OneTypeMatcher<PredicateRewriteVisitorData, PredicateRewriteVisitorData::needChild>;
using PredicateRewriteVisitor = InDepthNodeVisitor<PredicateRewriteMatcher, true>;
}
