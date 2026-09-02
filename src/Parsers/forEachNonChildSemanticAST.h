#pragma once

#include <Parsers/IAST_fwd.h>
#include <functional>

namespace DB
{

class IAST;

/// Visits each AST subtree that `node` keeps OUTSIDE `IAST::children` but that still carries query
/// semantics (and that the node class's `updateTreeHashImpl` folds into the tree hash) — currently
/// the `SHOW ... WHERE` / `LIMIT` expressions, the `BACKUP` settings / `ON CLUSTER` host ids /
/// per-element partitions, the `CREATE ROW POLICY` filter expressions, the
/// `CREATE MASKING POLICY` `UPDATE` assignments / `WHERE` condition, the `CREATE USER` /
/// `ALTER USER` target names, and the AST-valued `SETTINGS` values of `ASTSetQuery`. Generic
/// walks over `children` — the rewrite-rule matcher and its size/depth limits and placeholder
/// screening, and the query-parameter discovery and substitution — miss these members, so they
/// must be told about them explicitly, and all of them must stay behind the SAME list: a carrier
/// the substitution walk missed would let `SHOW TABLES LIMIT {n:UInt64}` reach the rule matcher
/// with the placeholder unsubstituted and silently bypass a `REJECT` rule pinning `LIMIT 42`.
/// Only non-null members are visited; the callback is not recursed automatically (the caller
/// decides how to descend). Keep this in sync with the `updateTreeHashImpl` overrides of the
/// listed classes.
void forEachNonChildSemanticAST(const IAST & node, const std::function<void(const ASTPtr &)> & visit);

/// The mutable overload, for walks that replace nodes (query-parameter substitution). It does NOT
/// visit the `SETTINGS` values of `ASTSetQuery`: they are `ASTPtr`s inside immutable shared
/// `Field` payloads (`FieldFromASTImpl`), so replacement must go through rebuilding the `Field`
/// (`ReplaceQueryParameterVisitor::visitSettingsChanges`), not through mutating the AST in place.
void forEachMutableNonChildSemanticAST(IAST & node, const std::function<void(ASTPtr &)> & visit);

}
