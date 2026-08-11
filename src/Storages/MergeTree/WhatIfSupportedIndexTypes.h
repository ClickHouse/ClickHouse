#pragma once

#include <base/types.h>

namespace DB
{

/// Fail-closed allowlist of skip-index types that hypothetical indexes support.
///
/// The empirical estimator feeds granules to `IMergeTreeIndexAggregator` and asks
/// `IMergeTreeIndexCondition::mayBeTrueOnGranule`, so only index types with those plain
/// semantics can be modelled. Types that need a different layout (a tokenized block for
/// `text`, a vector store for `vector_similarity`) or that are not implemented at all
/// would otherwise reach that pipeline and produce a meaningless estimate.
///
/// The check is by type name rather than by an `IMergeTreeIndex` capability predicate on
/// purpose: a predicate can only reject the bad types we already know about, while a newly
/// registered type must be rejected until someone has checked it against the pipeline.
bool isIndexTypeSupportedByWhatIf(const String & index_type);

/// The allowlist as a comma-separated list, for error messages.
String getIndexTypesSupportedByWhatIf();

}
