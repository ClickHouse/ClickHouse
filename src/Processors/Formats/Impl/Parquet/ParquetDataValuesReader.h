#pragma once

#include <functional>
#include <concepts>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <IO/ReadBuffer.h>
#include "ParquetDataBuffer.h"

namespace DB
{

class RleValuesReader
{
public:
    RleValuesReader(std::unique_ptr<arrow::bit_util::BitReader> bit_reader_, Int32 bit_width_);

    /**
     * @brief Used when the bit_width is 0, so all elements have same value.
     */
    explicit RleValuesReader(UInt32 total_size, Int32 val = 0)
        : bit_reader(nullptr), bit_width(0), cur_group_size(total_size), cur_value(val), cur_group_is_packed(false)
        {}

    void nextGroup();

    void nextGroupIfNecessary() { if (cur_group_cursor >= cur_group_size) nextGroup(); }

    UInt32 curGroupLeft() const { return cur_group_size - cur_group_cursor; }

    /**
     * @brief Visit num_values elements.
     * For RLE encoding, for same group, the value is same, so they can be visited repeatedly.
     * For BitPacked encoding, the values may be different with each other, so they must be visited individual.
     *
     * @tparam IndividualVisitor A callback with signature: void(Int32 val)
     * @tparam RepeatedVisitor A callback with signature: void(UInt32 count, Int32 val)
     */
    template <typename IndividualVisitor, typename RepeatedVisitor>
    void visitValues(UInt32 num_values, IndividualVisitor && individual_visitor, RepeatedVisitor && repeated_visitor);

    /**
     * @brief Visit num_values elements by parsed nullability.
     * If the parsed value is same as max_def_level, then it is processed as null value.
     *
     * @tparam IndividualVisitor A callback with signature: void(size_t cursor)
     * @tparam RepeatedVisitor A callback with signature: void(size_t cursor, UInt32 count)
     *
     * Because the null map is processed, so only the callbacks only need to process the valid data.
     */
    template <typename IndividualVisitor, typename RepeatedVisitor>
    void visitNullableValues(
        size_t cursor,
        UInt32 num_values,
        Int32 max_def_level,
        LazyNullMap & null_map,
        IndividualVisitor && individual_visitor,
        RepeatedVisitor && repeated_visitor);

    /**
     * @brief Visit num_values elements by parsed nullability.
     * It may be inefficient to process the valid data individually like in visitNullableValues,
     * so a valid_index_steps index array is generated first, in order to process valid data continuously.
     *
     * @tparam IndividualNullVisitor A callback with signature: void(size_t cursor), used to process null value
     * @tparam SteppedValidVisitor  A callback with signature:
     *  void(size_t cursor, const std::vector<UInt8> & valid_index_steps)
     *  valid_index_steps records the gap size between two valid elements,
     *  i-th item in valid_index_steps describes how many elements there are
     *  from i-th valid element (include) to (i+1)-th valid element (exclude).
     *
     *  take following BitPacked group values for example, and assuming max_def_level is 1:
     *      [1,   0,    1,   1,   0,    1    ]
     *       null valid null null valid null
     *  the second line shows the corresponding validation state,
     *  then the valid_index_steps has values [1, 3, 2].
     *  Please note that the the sum of valid_index_steps is same as elements number in this group.
     *  TODO the definition of valid_index_steps should be updated when supporting nested types
     *
     * @tparam RepeatedVisitor  A callback with signature: void(bool is_valid, UInt32 cursor, UInt32 count)
     */
    template <typename IndividualNullVisitor, typename SteppedValidVisitor, typename RepeatedVisitor>
    void visitNullableBySteps(
        size_t cursor,
        UInt32 num_values,
        Int32 max_def_level,
        IndividualNullVisitor && null_visitor,
        SteppedValidVisitor && stepped_valid_visitor,
        RepeatedVisitor && repeated_visitor);

    /**
     * @brief Set the Values to column_data directly
     *
     * @tparam TValue The type of column data.
     * @tparam ValueGetter A callback with signature: TValue(Int32 val)
     */
    template <typename TValue, typename ValueGetter>
    void setValues(TValue * res_values, UInt32 num_values, ValueGetter && val_getter);

    /**
     * @brief Set the value by valid_index_steps generated in visitNullableBySteps.
     *  According to visitNullableBySteps, the elements number is valid_index_steps.size()-1,
     *  so valid_index_steps.size()-1 elements are read, and set to column_data with steps in valid_index_steps
     */
    template <typename TValue, typename ValueGetter>
    void setValueBySteps(
        TValue * res_values,
        const std::vector<UInt8> & col_data_steps,
        ValueGetter && val_getter);

private:
    std::unique_ptr<arrow::bit_util::BitReader> bit_reader;

    std::vector<Int32> cur_packed_bit_values;
    std::vector<UInt8> valid_index_steps;

    const Int32 bit_width;

    UInt32 cur_group_size = 0;
    UInt32 cur_group_cursor = 0;
    Int32 cur_value;
    bool cur_group_is_packed;
};

using RleValuesReaderPtr = std::unique_ptr<RleValuesReader>;


class ParquetDataValuesReader
{
public:
    virtual void readBatch(MutableColumnPtr & column, LazyNullMap & null_map, UInt32 num_values) = 0;

    virtual ~ParquetDataValuesReader() = default;
};

using ParquetDataValuesReaderPtr = std::unique_ptr<ParquetDataValuesReader>;


enum class ParquetReaderTypes
{
    Normal,
    TimestampInt96,
};

/**
 * The definition level is RLE or BitPacked encoding, while data is read directly
 */
template <typename TColumn, ParquetReaderTypes reader_type = ParquetReaderTypes::Normal>
class ParquetPlainValuesReader : public ParquetDataValuesReader
{
public:

    ParquetPlainValuesReader(
        Int32 max_def_level_,
        std::unique_ptr<RleValuesReader> def_level_reader_,
        ParquetDataBuffer data_buffer_)
        : max_def_level(max_def_level_)
        , def_level_reader(std::move(def_level_reader_))
        , plain_data_buffer(std::move(data_buffer_))
    {}

    void readBatch(MutableColumnPtr & col_ptr, LazyNullMap & null_map, UInt32 num_values) override;

private:
    Int32 max_def_level;
    std::unique_ptr<RleValuesReader> def_level_reader;
    ParquetDataBuffer plain_data_buffer;
};

template <typename TColumn>
class ParquetBitPlainReader : public ParquetDataValuesReader
{
public:
    ParquetBitPlainReader(
        Int32 max_def_level_,
        std::unique_ptr<RleValuesReader> def_level_reader_,
        std::unique_ptr<arrow::bit_util::BitReader> bit_reader_)
        : max_def_level(max_def_level_)
        , def_level_reader(std::move(def_level_reader_))
        , bit_reader(std::move(bit_reader_))
    {}

    void readBatch(MutableColumnPtr & col_ptr, LazyNullMap & null_map, UInt32 num_values) override;

private:
    Int32 max_def_level;
    std::unique_ptr<RleValuesReader> def_level_reader;
    std::unique_ptr<arrow::bit_util::BitReader> bit_reader;
};

/**
 * The data and definition level encoding are same as ParquetPlainValuesReader.
 * But the element size is const and bigger than primitive data type.
 */
template <typename TColumn>
class ParquetFixedLenPlainReader : public ParquetDataValuesReader
{
public:

    ParquetFixedLenPlainReader(
        Int32 max_def_level_,
        Int32 elem_bytes_num_,
        std::unique_ptr<RleValuesReader> def_level_reader_,
        ParquetDataBuffer data_buffer_)
        : max_def_level(max_def_level_)
        , elem_bytes_num(elem_bytes_num_)
        , def_level_reader(std::move(def_level_reader_))
        , plain_data_buffer(std::move(data_buffer_))
    {}

    void readOverBigDecimal(MutableColumnPtr & col_ptr, LazyNullMap & null_map, UInt32 num_values);

    void readBatch(MutableColumnPtr & col_ptr, LazyNullMap & null_map, UInt32 num_values) override;

private:
    Int32 max_def_level;
    Int32 elem_bytes_num;
    std::unique_ptr<RleValuesReader> def_level_reader;
    ParquetDataBuffer plain_data_buffer;
};

/**
 * Read data according to the format of ColumnLowCardinality format.
 *
 * Only index and null column are processed in this class.
 * And all null value is mapped to first index in dictionary,
 * so the result index valued is added by one.
*/
template <typename TColumnVector>
class ParquetRleLCReader : public ParquetDataValuesReader
{
public:
    ParquetRleLCReader(
        Int32 max_def_level_,
        std::unique_ptr<RleValuesReader> def_level_reader_,
        std::unique_ptr<RleValuesReader> rle_data_reader_)
        : max_def_level(max_def_level_)
        , def_level_reader(std::move(def_level_reader_))
        , rle_data_reader(std::move(rle_data_reader_))
    {}

    void readBatch(MutableColumnPtr & index_col, LazyNullMap & null_map, UInt32 num_values) override;

private:
    Int32 max_def_level;
    std::unique_ptr<RleValuesReader> def_level_reader;
    std::unique_ptr<RleValuesReader> rle_data_reader;
};

/**
 * The definition level is RLE or BitPacked encoded,
 * and the index of dictionary is also RLE or BitPacked encoded.
 *
 * while the result is not parsed as a low cardinality column,
 * instead, a normal column is generated.
 */
template <typename TColumn>
class ParquetRleDictReader : public ParquetDataValuesReader
{
public:
    ParquetRleDictReader(
        Int32 max_def_level_,
        std::unique_ptr<RleValuesReader> def_level_reader_,
        std::unique_ptr<RleValuesReader> rle_data_reader_,
        const IColumn & page_dictionary_)
        : max_def_level(max_def_level_)
        , def_level_reader(std::move(def_level_reader_))
        , rle_data_reader(std::move(rle_data_reader_))
        , page_dictionary(page_dictionary_)
    {}

    void readBatch(MutableColumnPtr & col_ptr, LazyNullMap & null_map, UInt32 num_values) override;

private:
    Int32 max_def_level;
    std::unique_ptr<RleValuesReader> def_level_reader;
    std::unique_ptr<RleValuesReader> rle_data_reader;
    const IColumn & page_dictionary;
};

}
