#pragma once

#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{

/// Hybrid-segment pruner, modeled after PartitionPruner / Iceberg::ManifestFilesPruner /
/// Paimon::PartitionPruner.
///
/// Build one KeyCondition over the user filter (PREWHERE+WHERE represented as an
/// ActionsDAG) using all comparable Hybrid columns as the key. For each segment, build
/// a second KeyCondition from its (already watermark-substituted) predicate AST and
/// use `KeyCondition::extractPlainRangesForColumn` to obtain a Hyperrectangle (fail-open
/// to whole-universe per column when extraction is ambiguous). Then ask
/// `KeyCondition::checkInHyperrectangle(rect, types).can_be_true`. The segment can be
/// pruned iff the answer is false.
///
/// canBePruned() returns true only when (user_filter AND segment_predicate) is provably
/// empty. It returns false in all other cases — unsupported segment shapes, missing user
/// filter, exceptions — so the caller falls back to scanning the segment normally.
class HybridSegmentPruner
{
public:
    HybridSegmentPruner(
        const ActionsDAGWithInversionPushDown & filter_dag,
        const NamesAndTypesList & hybrid_columns,
        ContextPtr context);

    bool canBePruned(const ASTPtr & substituted_segment_predicate) const;

    /// True if the user filter is unrecognizable / always-true on the Hybrid key columns:
    /// no segment can ever be pruned, so callers can short-circuit.
    bool isUseless() const { return useless; }

private:
    KeyDescription identity_key;
    KeyCondition user_condition;
    ContextPtr context;
    bool useless = false;
};

}
