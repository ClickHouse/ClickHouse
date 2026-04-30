#pragma once

#include <Analyzer/IQueryTreePass.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

/// Adds CASTs for Hybrid segments when physical types differ from the Hybrid schema
///
/// It normalizes headers coming from different segments when table structure in some segments
/// differs from the Hybrid table definition. For example column X is UInt32 in the Hybrid table,
/// but Int64 in an additional segment.
///
/// Without these casts ConvertingActions may fail to reconcile mismatched headers when casts are impossible
/// (e.g. AggregateFunction states carry hashed data tied to their argument type and cannot be recast), for example:
/// "Conversion from AggregateFunction(uniq, Decimal(38, 0)) to AggregateFunction(uniq, UInt64) is not supported"
/// (CANNOT_CONVERT_TYPE).
///
/// Per-segment casts are not reliable because WithMergeState strips aliases, so merged pipelines
/// from different segments would return different headers (with or without CAST), leading to errors
/// like "Cannot find column `max(value)` in source stream, there are only columns: [max(_CAST(value, 'UInt64'))]"
/// (THERE_IS_NO_COLUMN).
class HybridCastsPass : public IQueryTreePass
{
public:
    String getName() override { return "HybridCastsPass"; }
    String getDescription() override { return "Inject casts for Hybrid columns to match schema types"; }
    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
