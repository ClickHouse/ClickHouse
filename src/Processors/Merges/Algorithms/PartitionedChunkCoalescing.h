#pragma once

#include <Columns/IColumn.h>

namespace DB
{

/// Visits nonempty source slices in partition-major, then source order, reading offsets directly
/// from the caller's partition index and supplying each slice's destination position.
template <typename Offsets, typename Visitor>
void forEachPartitionedChunkRange(size_t num_partitions, size_t num_sources, Offsets && offsets, Visitor && visitor)
{
    size_t destination = 0;
    for (size_t partition = 0; partition < num_partitions; ++partition)
        for (size_t source = 0; source < num_sources; ++source)
        {
            const size_t begin = offsets(source, partition);
            const size_t length = offsets(source, partition + 1) - begin;
            if (!length)
                continue;
            visitor(source, begin, length, destination);
            destination += length;
        }
}

/// Coalesces compatible columns whose rows are already grouped by the same partition mapping.
/// The output keeps that grouping and the source order within each partition.
template <typename Offsets>
MutableColumnPtr coalescePartitionedColumn(
    const VectorWithMemoryTracking<ColumnPtr> & sources, size_t num_partitions, Offsets && offsets)
{
    auto destination = sources.front()->cloneEmpty();
    destination->prepareForSquashing(sources, /* factor */ 1);
    forEachPartitionedChunkRange(
        num_partitions, sources.size(), offsets,
        [&](size_t source, size_t begin, size_t length, size_t)
        {
            destination->insertRangeFrom(*sources[source], begin, length);
        });
    return destination;
}

}
