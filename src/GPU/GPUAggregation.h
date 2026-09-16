#pragma once

#include "config.h"

#if USE_GPU

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

std::optional<int> sumTypeOf(const IDataType & type);

size_t elementSizeOf(int element_type);


void * resizeForElementType(IColumn & column, size_t num_rows, int element_type);

bool canSumOnDevice(const IDataType & argument_type, const IDataType & result_type);

class SumAccumulator
{
public:
    SumAccumulator(const IDataType & argument_type, const IDataType & result_type, size_t batch_bytes_);

    void add(const IColumn & column);

    Field finalize();

private:
    static constexpr size_t max_batch_rows = (1UL << 31) - 1;

    void sumBatchOnDevice();

    const int element_type;
    const int sum_type;
    const size_t element_size;
    const size_t batch_bytes;

    PODArray<char> staged;

    UInt64 integer_sum = 0;
    Float64 float_sum = 0;
};



bool canGroupBySumOnDevice(const DataTypes & key_types, const DataTypes & argument_types, const DataTypes & result_types);

class GroupBySumAccumulator
{
public:
    GroupBySumAccumulator(
        const DataTypes & key_types, const DataTypes & argument_types, const DataTypes & result_types, size_t batch_bytes);

    ~GroupBySumAccumulator();

    GroupBySumAccumulator(const GroupBySumAccumulator &) = delete;
    GroupBySumAccumulator & operator=(const GroupBySumAccumulator &) = delete;

    void add(const Columns & key_columns, const Columns & value_columns);

    size_t finalize();

    void copyGroupsTo(MutableColumns & key_columns, MutableColumns & value_columns);

private:
    static constexpr size_t max_batch_rows = (1UL << 31) - 1;

    void sendBatchToDevice();

    std::vector<int> key_element_types;
    std::vector<int> value_element_types;
    std::vector<int> value_sum_types;

    std::vector<size_t> key_element_sizes;
    std::vector<size_t> value_element_sizes;

    const size_t batch_rows;

    std::vector<PODArray<char>> staged_keys;
    std::vector<PODArray<char>> staged_values;
    size_t staged_rows = 0;

    void * handle = nullptr;

    std::optional<size_t> num_groups;
};

}

#endif
