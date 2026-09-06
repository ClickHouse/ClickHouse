#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB
{

/// Rewrite function names to their canonical forms.
/// For example, rewrite (1) to (2)
/// (1) SELECT suM(1), AVG(2);
/// (2) SELECT sum(1), avg(2);
///
/// It's used to help projection query analysis matching function nodes by their canonical names.
/// See the comment of ActionsDAG::foldActionsByProjection for details.
struct FunctionNameNormalizer
{
    static void visit(IAST *);

    /// Same, and additionally canonicalizes the function name of a projection's
    /// `COLUMNS(...) APPLY` transformer (including the function names inside its parameters and
    /// lambda), so `APPLY SUM` and `APPLY sum` compare as the same definition.
    ///
    /// Comparison-only. `visit` deliberately keeps the transformer as written: it runs on every
    /// `CREATE`/`ALTER` before the definition is persisted (table metadata on disk and in
    /// ZooKeeper), older replicas compare the serialized `projections` field byte-for-byte, and
    /// the canonical form of an older version is the definition as written. Rewriting it on the
    /// write path would make such a replica fail with `METADATA_MISMATCH`.
    static void visitForComparison(IAST *);

private:
    static void visitImpl(IAST *, bool normalize_apply_transformer);
};

}
