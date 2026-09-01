#pragma once

#include <Core/Names.h>

namespace DB
{

class ActionsDAG;
class ArrayJoinStep;
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

/// Generalized form: the partitioning expression is given directly as a DAG and the names of its result
/// columns. Besides table partition keys, this covers other stream-partitioning schemes, e.g. the hash
/// scatter by the window `PARTITION BY` columns (where the partitioning expression is the identity over
/// those columns).
bool isPartitionKeyFunctionOfKeys(
    const ActionsDAG & partition_actions, const Names & partition_key_columns, const ActionsDAG & key_actions, const Names & key_names);

/// Returns the transformation applied by an `ArrayJoinStep` as an `ActionsDAG`: every column of the
/// step's input passes through unchanged, and each array-joined column becomes an `ARRAY_JOIN` node
/// over its source array.
///
/// The passes above compose this DAG into `key_actions` when they look through an ARRAY JOIN. An
/// exploded column can carry the very name of its source array (`ARRAY JOIN arr`, or any alias under
/// the old analyzer), so without the node a key referencing it would be mistaken for the source column
/// and matched against the partition key. With the node the explosion is part of the key's lineage,
/// and `isPartitionKeyFunctionOfKeys` rejects the key.
ActionsDAG buildArrayJoinDAG(const ArrayJoinStep & array_join);

}

}
