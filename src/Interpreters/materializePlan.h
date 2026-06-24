#pragma once
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/Context_fwd.h>

#include <string_view>

namespace DB
{

/// Deserializes and materializes a cached query plan for execution.
///
/// The process is:
///   1. QueryPlan::deserialize() — reconstructs the plan skeleton from binary bytes.
///      Leaf nodes are `ReadFromTableStep` (storage-agnostic).
///   2. QueryPlan::makeSets() — builds `PreparedSet` objects for IN (...) subquery expressions.
///   3. QueryPlan::resolveStorages() — replaces each `ReadFromTableStep` with fresh storage-specific
///      read steps by calling `storage->read` against the current data snapshot.
///
/// The materialized plan is in pre-optimization state. Expression analysis and aggregation
/// planning are not re-run — those are encoded in the cached plan structure.
///
/// `QueryPlan::optimize` IS re-executed on every cache hit. The call chain is:
///   `buildQueryPipeline` → `QueryPlan::buildQueryPipeline(settings, ..., do_optimize=true)` → `optimize`
/// This is intentional per RFC #93812: the optimizer must re-run to adapt to current storage
/// snapshots (part sets), table statistics, and server configuration (e.g. changed settings).
///
/// Throws if deserialization fails (e.g. format version mismatch, unknown step type).
/// Callers should check the entry's format_version before calling.
QueryPlan materializePlan(std::string_view serialized_bytes, const ContextPtr & context);

}

