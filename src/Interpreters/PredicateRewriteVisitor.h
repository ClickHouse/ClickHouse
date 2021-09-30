#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/IAST.h>

namespace DB
{

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
        const ASTs & predicates_,
        const TableWithColumnNamesAndTypes & table_columns_,
        bool optimize_final_,
        bool optimize_with_);

private:
    const ASTs & predicates;
    const TableWithColumnNamesAndTypes & table_columns;
    bool optimize_final;
    bool optimize_with;

    void visitFirstInternalSelect(ASTSelectQuery & select_query, ASTPtr &);

    void visitOtherInternalSelect(ASTSelectQuery & select_query, ASTPtr &);

    bool rewriteSubquery(ASTSelectQuery & subquery, const Names & inner_columns);
};

using PredicateRewriteMatcher = OneTypeMatcher<PredicateRewriteVisitorData, PredicateRewriteVisitorData::needChild>;
using PredicateRewriteVisitor = InDepthNodeVisitor<PredicateRewriteMatcher, true>;

}
