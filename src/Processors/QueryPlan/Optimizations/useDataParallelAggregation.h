#pragma once

#include <Core/Names.h>

namespace DB
{

class ActionsDAG;
struct KeyDescription;

namespace QueryPlanOptimizations
{

/// Returns true if the storage's `partition_key` is a deterministic function of the operator's key
/// columns `key_names` (whose computation from the storage's columns is given by `key_actions`).
///
/// When that holds, two rows with equal key values always have an equal partition key value, so they
/// always end up in the same partition. In other words, no key value is ever split across partitions.
/// Therefore, if the storage keeps each partition within a single stream, each key value appears in
/// exactly one stream, and an operator keyed on `key_names` (DISTINCT, GROUP BY, LIMIT BY) can process
/// each stream independently and skip the cross-stream merge.
///
/// For example, for `SELECT DISTINCT a` (key `a`):
///   - allowed (the partition key is a function of `a`): `PARTITION BY a % 8`, `PARTITION BY toYYYYMM(a)`,
///     `PARTITION BY sipHash64(a) % 16`. Also `SELECT DISTINCT toString(a)` with `PARTITION BY a % 8`,
///     because `toString` is injective so `a` is recoverable from the key, hence `a % 8` is determined.
///   - not allowed: `PARTITION BY b % 8` (depends on `b`, which `a` does not determine), or key `a % 4`
///     with `PARTITION BY a % 8` (`a % 4` does not determine `a % 8`: e.g. `a = 0` and `a = 4` share
///     `a % 4 = 0` but land in partitions `0` and `4`).
///
/// Shared by the per-partition request passes (`optimize*PerPartition`) and `applyStreamDisjointness`.
bool isPartitionKeyFunctionOfKeys(const KeyDescription & partition_key, const ActionsDAG & key_actions, const Names & key_names);

}

}
