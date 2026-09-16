#pragma once

#include <Core/ColumnNumbers.h>
#include <DataTypes/IDataType_fwd.h>
#include <QueryPipeline/Pipe.h>

namespace DB
{

class QueryPipelineBuilder;

/// Repartitions the data by the hash of the key columns: the pipeline ends up with
/// num_partitions streams where stream i carries exactly the rows of partition i.
/// `hash_cast_types` (one entry per key, optional) selects a type to cast each key to before hashing.
/// `scattered_stream_transform` works like the getter of `addSimpleTransform`: it is applied to every
/// scattered stream before the streams of a partition are merged into one, so its work runs on all
/// streams in parallel. It may return nullptr for no transform.
void scatterByPartition(
    QueryPipelineBuilder & pipeline,
    size_t num_partitions,
    const ColumnNumbers & key_columns,
    const DataTypes & hash_cast_types = {},
    const Pipe::ProcessorGetterSharedHeader & scattered_stream_transform = {});

/// Spreads whole chunks round-robin over num_partitions streams: stream i carries the chunks of
/// partition i. The scatter of input stream s starts at partition start_bucket + s.
/// `scattered_stream_transform` works as in `scatterByPartition`.
void scatterRoundRobin(
    QueryPipelineBuilder & pipeline,
    size_t num_partitions,
    size_t start_bucket,
    const Pipe::ProcessorGetterSharedHeader & scattered_stream_transform = {});

/// A hash scatter into `num_partitions` followed by per-partition merges of the `num_streams` inputs wires up
/// (num_partitions * num_streams) connections in the pipeline. Both functions bound this by a sane value so that
/// a large `max_threads` cannot explode the port/processor count.
void checkScatterConnectionLimit(size_t num_partitions, size_t num_streams);

/// The partition count reduced (down to one) so that a scatter of `num_streams` streams stays within the limit,
/// for steps whose partition count is a free choice.
size_t clampScatterPartitions(size_t num_partitions, size_t num_streams);

}
