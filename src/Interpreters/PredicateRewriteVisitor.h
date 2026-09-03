#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTSelectIntersectExceptQuery;
class ASTSelectQuery;
class ASTSelectWithUnionQuery;

/// Whether `PredicateRewriteVisitorData::rewriteSubquery` could add any predicate to this subquery.
/// Depends only on the subquery - a `FINAL`, a `LIMIT`, or a `SELECT` list carrying a window function
/// or an `untuple` bars every predicate alike - so it can be asked before there is a predicate to add.
bool canRewriteSubquery(const ASTSelectQuery & subquery, bool optimize_final, bool optimize_with, ContextPtr context);

class PredicateRewriteVisitorData : WithContext
{
public:
    bool is_rewrite = false;
    using TypeToVisit = ASTSelectWithUnionQuery;

    void visit(ASTSelectWithUnionQuery & union_select_query, ASTPtr &);

    static bool needChild(const ASTPtr & node, const ASTPtr & child)
    {
        /// Do not descend into the JOIN condition (ON/USING). A subquery there is unrelated
        /// to the joined table, so rewriteSubquery's positional inner/outer column mapping
        /// would be invalid (and could read out of bounds).
        if (child && child->as<ASTTableJoin>())
            return false;
        return !(node && node->as<TypeToVisit>());
    }

    PredicateRewriteVisitorData(
        ContextPtr context_,
        const ASTs & predicates_,
        const TableWithColumnNamesAndTypes & table_columns_,
        bool optimize_final_,
        bool optimize_with_);

    bool rewriteSubquery(ASTSelectQuery & subquery, const Names & inner_columns);

private:
    const ASTs & predicates;
    const TableWithColumnNamesAndTypes & table_columns;
    bool optimize_final;
    bool optimize_with;

    void visitFirstInternalSelect(ASTSelectQuery & select_query, ASTPtr &);

    void visitOtherInternalSelect(ASTSelectQuery & select_query, ASTPtr &);

    void visit(ASTSelectIntersectExceptQuery & intersect_except_query, ASTPtr &);

    void visitInternalSelect(size_t index, ASTSelectQuery & select_node, ASTPtr & node);
};

using PredicateRewriteMatcher = OneTypeMatcher<PredicateRewriteVisitorData, PredicateRewriteVisitorData::needChild>;
using PredicateRewriteVisitor = InDepthNodeVisitor<PredicateRewriteMatcher, true>;

}
