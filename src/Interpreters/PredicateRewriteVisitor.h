#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTSelectIntersectExceptQuery;
class ASTSelectQuery;
class ASTSelectWithUnionQuery;

class PredicateRewriteVisitorData : WithContext
{
public:
    bool is_rewrite = false;
    using TypeToVisit = ASTSelectWithUnionQuery;

    void visit(ASTSelectWithUnionQuery & union_select_query, ASTPtr &);

    static bool needChild(const ASTPtr & node, const ASTPtr &)
    {
        return !(node && node->as<TypeToVisit>());
    }

    PredicateRewriteVisitorData(
        ContextPtr context_,
        const ASTList & predicates_,
        const TableWithColumnNamesAndTypes & table_columns_,
        bool optimize_final_,
        bool optimize_with_);

private:
    const ASTList & predicates;
    const TableWithColumnNamesAndTypes & table_columns;
    bool optimize_final;
    bool optimize_with;

    void visitFirstInternalSelect(ASTSelectQuery & select_query, ASTPtr &);

    void visitOtherInternalSelect(ASTSelectQuery & select_query, ASTPtr &);

    void visit(ASTSelectIntersectExceptQuery & intersect_except_query, ASTPtr &);

    bool rewriteSubquery(ASTSelectQuery & subquery, const Names & inner_columns);

    void visitInternalSelect(bool first, ASTSelectQuery & select_node, ASTPtr & node);
};

using PredicateRewriteMatcher = OneTypeMatcher<PredicateRewriteVisitorData, PredicateRewriteVisitorData::needChild>;
using PredicateRewriteVisitor = InDepthNodeVisitor<PredicateRewriteMatcher, true>;

}
