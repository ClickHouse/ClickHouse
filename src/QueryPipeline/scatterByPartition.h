#pragma once

#include <Core/ColumnNumbers.h>
#include <DataTypes/IDataType_fwd.h>

namespace DB
{

class QueryPipelineBuilder;

/// Repartitions the data by the hash of the key columns: the pipeline ends up with
/// num_partitions streams where stream i carries exactly the rows of partition i.
/// `hash_cast_types` (one entry per key, optional) selects a type to cast each key to before hashing.
void scatterByPartition(QueryPipelineBuilder & pipeline, size_t num_partitions, const ColumnNumbers & key_columns, const DataTypes & hash_cast_types = {});

/// A hash scatter into `num_partitions` followed by per-partition merges of the `num_streams` inputs wires up
/// (num_partitions * num_streams) connections in the pipeline. Bound this by a sane value so that a large
/// `max_threads` cannot explode the port/processor count.
inline constexpr size_t scatter_connection_count_limit = 1000000;
void checkScatterConnectionLimit(size_t num_partitions, size_t num_streams);

}
