#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUAggregationCudf.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Common/PODArray.h>

#include <optional>
#include <vector>

namespace DB::GPU
{
const String & deviceProbeError();

std::optional<int> elementTypeOf(const IDataType & type);

std::optional<int> codecOf(UInt8 method_byte);

std::optional<int> aggregationOf(const String & aggregate_function_name);

size_t elementSizeOf(int element_type);


void * resizeForElementType(IColumn & column, size_t num_rows, int element_type);

bool canReduceOnDevice(const IDataType & argument_type, const IDataType & result_type, int aggregation);

class PinnedBuffer
{
public:
    PinnedBuffer() = default;
    explicit PinnedBuffer(size_t capacity_) { reserve(capacity_); }
    ~PinnedBuffer();

    PinnedBuffer(PinnedBuffer && other) noexcept;
    PinnedBuffer & operator=(PinnedBuffer && other) noexcept;

    PinnedBuffer(const PinnedBuffer &) = delete;
    PinnedBuffer & operator=(const PinnedBuffer &) = delete;

    void reserve(size_t bytes);

    void append(const char * data, size_t bytes);

    void clear() { used = 0; }

    const char * data() const { return buffer; }
    size_t size() const { return used; }
    bool empty() const { return used == 0; }

private:
    char * buffer = nullptr;
    size_t capacity = 0;
    size_t used = 0;
};


class GPUAccumulator
{
public:
    GPUAccumulator(
        const IDataType & argument_type,
        const IDataType & result_type,
        int aggregation_,
        size_t batch_bytes_,
        std::optional<int> codec_ = {});

    void add(const IColumn & column);

    void addBlock(const char * payload, size_t compressed_bytes, size_t decompressed_bytes);

    Field finalize();

private:
    static constexpr size_t max_batch_rows = (1UL << 31) - 1;

    void flushIfBatchWouldOverflow(size_t incoming_rows, size_t incoming_bytes);

    void reduceBatchOnDevice();

    void combine(UInt64 batch_result);

    const int element_type;
    const int result_type;
    const int aggregation;
    const size_t element_size;
    const size_t batch_bytes;
    const std::optional<int> codec;

    PinnedBuffer staged;

    std::vector<size_t> block_offsets;
    std::vector<size_t> block_compressed_sizes;
    std::vector<size_t> block_decompressed_sizes;

    size_t staged_values_bytes = 0;

    UInt64 integer_result = 0;
    Float64 float_result = 0;
    bool has_result = false;
};


bool canGroupByReduceOnDevice(
    const DataTypes & key_types,
    const DataTypes & argument_types,
    const DataTypes & result_types,
    const std::vector<int> & aggregations);

class GroupByGPUAccumulator
{
public:
    GroupByGPUAccumulator(
        const DataTypes & key_types,
        const DataTypes & argument_types,
        const DataTypes & result_types,
        const std::vector<int> & aggregations,
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

    std::vector<int> key_element_types;
    std::vector<int> value_element_types;
    std::vector<int> value_result_types;
    std::vector<int> value_aggregations;

    std::vector<size_t> key_element_sizes;
    std::vector<size_t> value_element_sizes;

    const size_t batch_rows;

    std::vector<PinnedBuffer> staged_keys;
    std::vector<PinnedBuffer> staged_values;
    size_t staged_rows = 0;

    void * handle = nullptr;

    std::optional<size_t> num_groups;
};
}

#endif
