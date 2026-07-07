#pragma once

#include <Core/ColumnNumbers.h>
#include <Core/SortDescription.h>
#include <DataTypes/IDataType_fwd.h>

namespace DB
{

class QueryPipelineBuilder;

/// Repartitions the data by the hash of the key columns: the pipeline ends up with
/// num_partitions streams where stream i carries exactly the rows of partition i.
/// `hash_cast_types` (one entry per key, optional) selects a type to cast each key to before hashing.
void scatterByPartition(QueryPipelineBuilder & pipeline, size_t num_partitions, const ColumnNumbers & key_columns, const DataTypes & hash_cast_types = {});

/// Same as `scatterByPartition`, but the input streams are already sorted by `sort_description` and each
/// output partition stream stays sorted: the per-partition pieces of every input stream are combined with a
/// `MergingSortedTransform` (order-preserving) instead of a `ResizeProcessor` (which interleaves arbitrarily).
/// Used by `parallel_full_sorting_merge` to shard a read-in-order (already sorted) merge-join side without a
/// full re-sort - each shard only needs to finish the sort, not redo it.
void scatterByPartitionPreservingOrder(
    QueryPipelineBuilder & pipeline,
    size_t num_partitions,
    const ColumnNumbers & key_columns,
    const SortDescription & sort_description,
    size_t max_block_size);

}
