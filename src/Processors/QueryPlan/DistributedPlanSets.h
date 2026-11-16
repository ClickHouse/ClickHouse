#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <memory>
#include <vector>

namespace DB
{

class FutureSetFromSubquery;
using FutureSetFromSubqueryPtr = std::shared_ptr<FutureSetFromSubquery>;

/// Handling of `IN`-subquery sets for a distributed query plan (`make_distributed_plan`).
/// The sets are built once on the initiator and their values ship with the worker tasks;
/// see `QueryPlan::convertToDistributed` for how these functions are used around the cut.

/// Rejects sets that worker tasks cannot receive (currently: sets backed by a `GLOBAL IN` /
/// `GLOBAL JOIN` external table; a task has no way to carry a temporary table). Runs before
/// the optimization passes, so no such set is built for a query that is rejected.
void validateSetsForDistributedPlan(QueryPlan::Node & root);

/// Detaches the IN-subquery sets from the delayed set steps and removes those steps from the
/// plan, so the fragments never carry them; the caller re-adds the sets to the initiator plan.
std::vector<FutureSetFromSubqueryPtr> extractSetsForDistributedPlan(QueryPlan::Node *& root);

/// Makes a set subquery source run as a distributed plan: the values are deduplicated on the
/// workers (bounded by `max_rows_to_transfer`, so an over-limit set fails during the build)
/// while the set itself is still filled on the initiator. A source that collapses to a single
/// stage, or that has a step that cannot be serialized, keeps running locally.
void convertSetSourceForDistributedPlan(QueryPlan & source_plan, const ContextPtr & context);

}
