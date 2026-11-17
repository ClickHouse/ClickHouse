#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/CudfGroupBy.cuh>
#include <GPU/CudfReduction.cuh>
#include <GPU/GPUResources.cuh>
#include <GPU/GPUTypeMapping.h>
#include <GPU/GPUTypes.cuh>
#include <GPU/GPUUploadPipe.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

#include <memory>
#include <optional>
#include <vector>

namespace DB::GPU
{

/// An aggregate without `GROUP BY` on the device: the values of a column go to the device in batches of `batch_bytes`,
/// and each batch is reduced there.
class GPUAccumulator
{
public:
    GPUAccumulator(const IDataType & argument_type, const IDataType & result_type, GPUAggregationKind aggregation_, size_t batch_bytes_);

    void add(const IColumn & column);

    Field finalize();

    static constexpr size_t max_stage_bytes = 256UL * 1024 * 1024;

private:
    static constexpr size_t max_batch_rows = (1UL << 31) - 1;

    void reduceBatchOnDevice();

    const GPUElementType element_type;
    const GPUElementType result_type;
    const DataTypePtr result_data_type;
    const GPUAggregationKind aggregation;
    const size_t batch_bytes;

    std::unique_ptr<CudfReduction> reduction;
    ColumnUploadPipe pipe;
};


/// A `GROUP BY` on the device: the keys and the values go to the device in batches of about `batch_bytes`, and the
/// `CudfGroupBy` groups each batch into the groups so far. Without keys, the aggregates are of every row, as one group,
/// which there is none of where no row is grouped.
class GroupByGPUAccumulator
{
public:
    GroupByGPUAccumulator(
        const DataTypes & key_types,
        const DataTypes & value_types,
        std::vector<GPUGroupByAggregate> aggregates_,
        size_t batch_bytes);

    void add(const Columns & key_columns, const Columns & value_columns);

    /// Returns the number of groups.
    size_t finalize();

    /// After `finalize`: into a column of the keys each, and a column of the result of each aggregate.
    void copyGroupsTo(MutableColumns & key_columns, MutableColumns & aggregate_columns);

private:
    static constexpr size_t max_batch_rows = (1UL << 31) - 1;

    size_t stagedRows() const { return value_pipes.front().stagedRows(); }
    size_t stagedVariableBytes() const;

    void sendBatchToDevice();

    const std::vector<GPUElementType> group_keys;
    /// The columns aggregated, sent once each however many aggregates are of them.
    const std::vector<GPUElementType> values;
    const std::vector<GPUGroupByAggregate> aggregates;
    const size_t batch_rows;

    std::vector<ColumnUploadPipe> key_pipes;
    std::vector<ColumnUploadPipe> value_pipes;

    StreamPtr group_stream;
    std::unique_ptr<CudfGroupBy> group_by;

    std::optional<size_t> num_groups;
};

}

#endif
