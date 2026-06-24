#pragma once
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

/// Returns true if the query plan can be universalized and cached.
///
/// The plan is considered serializable if:
///   - All leaf nodes are ReadFromMergeTree (replaceable with ReadFromTableStep) or
///     already serializable (isSerializable() == true).
///   - All non-leaf nodes are serializable (isSerializable() == true).
///   - All sub-plans inside DelayedCreatingSetsStep nodes satisfy the same conditions.
///
/// When false is returned, the plan must not be stored in the query plan cache.
bool isSerializablePlan(const QueryPlan & plan);

/// Replaces all ReadFromMergeTree leaf nodes with ReadFromTableStep.
///
/// ReadFromTableStep stores only the table identifier (database.table) and
/// table expression modifiers (FINAL, SAMPLE). It is serializable via the existing
/// QueryPlan binary serialization infrastructure.
///
/// On cache retrieval, QueryPlan::resolveStorages() re-instantiates the read step by
/// calling storage->read() against the current storage snapshot, ensuring correct
/// results even if data has changed since the plan was cached.
///
/// Maintenance note: if new leaf step types are added that represent table reads,
/// they must be handled here analogously to ReadFromMergeTree.
///
/// Must only be called after isSerializablePlan() returns true, and before
/// QueryPlan::optimize() — the optimizer may replace ReadFromMergeTree with
/// LazilyReadFromMergeTree, which is not handled here. Calling universalizePlan()
/// on a post-optimization plan with LazilyReadFromMergeTree nodes will leave those
/// nodes in place; the subsequent ensureSerialized() will then fail or produce
/// an incorrect plan. Phase 4 integration must enforce this ordering.
void universalizePlan(QueryPlan & plan);

}

