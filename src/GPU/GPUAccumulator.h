#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUTypes.h>
#include <GPU/GPUColumns.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Common/PODArray.h>

#include <optional>
#include <string_view>
#include <vector>

namespace DB::GPU
{
const String & deviceProbeError();

std::optional<GPUElementType> elementTypeOf(const IDataType & type);

std::optional<GPUCodec> codecOf(UInt8 method_byte);

std::optional<GPUAggregationKind> aggregationOf(const String & aggregate_function_name);

size_t elementSizeOf(GPUElementType element_type);


std::string_view rawValuesOf(const IColumn & column, size_t num_rows, size_t element_size);

void * resizeForElementType(IColumn & column, size_t num_rows, GPUElementType element_type);

bool canReduceOnDevice(const IDataType & argument_type, const IDataType & result_type, GPUAggregationKind aggregation);



class GPUAccumulator
{
public:
    GPUAccumulator(
        const IDataType & argument_type,
        const IDataType & result_type,
        GPUAggregationKind aggregation_,
        size_t batch_bytes_,
        std::optional<GPUCodec> codec_ = {});

    void add(const IColumn & column);

    void addBlock(const char * payload, size_t compressed_bytes, size_t decompressed_bytes);

    Field finalize();

private:
    static constexpr size_t max_batch_rows = (1UL << 31) - 1;

    void reduceBatchOnDevice();

    void combine(UInt64 batch_result);

    const GPUElementType element_type;
    const GPUResultType result_type;
    const GPUAggregationKind aggregation;
    const size_t element_size;
    const size_t batch_bytes;
    const std::optional<GPUCodec> codec;

    UploadPipe pipe;

    UInt64 integer_result = 0;
    Float64 float_result = 0;
    bool has_result = false;
};


bool canGroupByReduceOnDevice(
    const DataTypes & key_types,
    const DataTypes & argument_types,
    const DataTypes & result_types,
    const std::vector<GPUAggregationKind> & aggregations);

class GroupByGPUAccumulator
{
public:
    GroupByGPUAccumulator(
        const DataTypes & key_types,
        const DataTypes & argument_types,
        const DataTypes & result_types,
        const std::vector<GPUAggregationKind> & aggregations,
        size_t batch_bytes);

    ~GroupByGPUAccumulator();

    GroupByGPUAccumulator(const GroupByGPUAccumulator &) = delete;
    GroupByGPUAccumulator & operator=(const GroupByGPUAccumulator &) = delete;

    void add(const Columns & key_columns, const Columns & value_columns);

    size_t finalize();

    void copyGroupsTo(MutableColumns & key_columns, MutableColumns & value_columns);

private:
    static constexpr size_t max_batch_rows = (1UL << 31) - 1;

    void sendBatchToDevice();

    std::vector<GPUElementType> key_element_types;
    std::vector<GPUElementType> value_element_types;
    std::vector<GPUResultType> value_result_types;
    std::vector<GPUAggregationKind> value_aggregations;

    std::vector<size_t> key_element_sizes;
    std::vector<size_t> value_element_sizes;

    const size_t batch_rows;

    std::vector<PinnedBuffer> staged_keys;
    std::vector<PinnedBuffer> staged_values;
    size_t staged_rows = 0;

    GPUGroupBy * handle = nullptr;

    std::optional<size_t> num_groups;
};
}

#endif
