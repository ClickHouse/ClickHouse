#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUColumnCache.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{

/// Source step put in place of the `ReadFromMergeTree` under a keyless `GPUAggregatingStep` by
/// `optimizeAggregationFromGPUResidentColumns`.
///
/// Emits one row per part, holding that part's own sum of every column the aggregation reads, and
/// leaves the step above it exactly as it was to add those few rows up - the trick
/// `ReadFromTextIndexCount` plays with `count()`, where the source answers from somewhere other
/// than the data and the aggregation above combines the per-part answers.
///
/// That only works where a partial sum has the same type as the column it came from, which is why
/// the pass accepts `UInt64`, `Int64` and `Float64` and nothing else: for those three, and only
/// those three, `sum` returns the column's own type, so a column of per-part sums is a column of
/// the type that was summed - and this step's output header is the read step's header, unchanged,
/// with nothing above it to adjust. A `UInt32` column, whose `sum` is a `UInt64`, would need a
/// different header and a different step above it, which is a different design rather than a wider
/// `switch` here.
///
/// Where a part's column is in the `GPUColumnCache` it is summed in device memory and nothing is
/// read on the host at all: no disk, no decompression, no transfer. Where it is not, it is read
/// once, copied to the device, cached and summed. That miss is not a fallback path - it is the
/// cache being filled, which is what the first query over a part does.
class ReadFromGPUResidentColumns : public ISourceStep
{
public:
    /// One column to sum: what it is called and how the device sees it.
    struct ColumnToSum
    {
        String name;

        /// `ClickHouseGPUElementType` and `ClickHouseGPUSumType` values, kept as `int` so that
        /// this header does not have to carry the boundary's enumerators - as in `DB::GPU`.
        int element_type;
        int sum_type;

        /// The width of one value, and so of `rows_count` of them: the size of the device buffer
        /// this column needs.
        size_t element_size;
    };

    /// `output_header_` has to be the header of the `ReadFromMergeTree` this replaces, and
    /// `columns_` its columns in the same order - the pass checks both. `parts_` must hold no part
    /// of zero rows: such a part contributes no row on the ordinary path either, and emitting one
    /// for it would turn an empty result into a row of zeroes under
    /// `empty_result_for_aggregation_by_empty_set`.
    ReadFromGPUResidentColumns(
        SharedHeader output_header_,
        std::vector<ColumnToSum> columns_,
        DataPartsVector parts_,
        const MergeTreeData & data_,
        StorageSnapshotPtr storage_snapshot_,
        GPUColumnCachePtr cache_,
        ContextPtr context_,
        size_t num_streams_);

    String getName() const override { return "ReadFromGPUResidentColumns"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(FormatSettings & format_settings) const override;
    void describeActions(JSONBuilder::JSONMap & map) const override;

private:
    std::vector<ColumnToSum> columns;
    DataPartsVector parts;

    /// A reference, as in `ReadFromMergeTree`: what keeps the table alive for the length of the
    /// query is the storage snapshot below - which holds the storage - and the query's table lock.
    const MergeTreeData & data;

    StorageSnapshotPtr storage_snapshot;
    GPUColumnCachePtr cache;

    /// The query's context, for the process list element, progress callback, quota and query hash
    /// that the reads filling the cache are tied to - so that such a read is cancellable, counted
    /// and charged exactly as the same read under `ReadFromMergeTree` would have been.
    ///
    /// The query's read limits (`max_rows_to_read` and friends) are deliberately not carried into
    /// those reads, and the pass refuses the optimization while any of them is set: a limit on how
    /// much may be read would fire on a miss and not on a hit, which would make the same query
    /// throw or not depending on what the cache happens to hold.
    ContextPtr context;

    /// The table's UUID, taken once: it is half of a cache key and `getStorageID` takes a lock.
    UUID table_uuid;

    size_t num_streams;
};

}

#endif
