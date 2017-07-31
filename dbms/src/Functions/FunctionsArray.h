#pragma once

#include <Core/FieldVisitors.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>

#include <Functions/IFunction.h>
#include <DataTypes/DataTypeTraits.h>
#include <Functions/ObjectPool.h>
#include <Functions/FunctionHelpers.h>
#include <Common/StringUtils.h>

#include <ext/range.h>

#include <unordered_map>
#include <numeric>


namespace DB
{

namespace ErrorCodes
{
    extern const int ZERO_ARRAY_OR_TUPLE_INDEX;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS;
    extern const int LOGICAL_ERROR;
}


/** Array functions:
  *
  * array(c1, c2, ...) - create an array.
  * arrayElement(arr, i) - get the array element by index. If index is not constant and out of range - return default value of data type.
  * The index begins with 1. Also, the index can be negative - then it is counted from the end of the array.
  * has(arr, x) - whether there is an element x in the array.
  * indexOf(arr, x) - returns the index of the element x (starting with 1), if it exists in the array, or 0 if it is not.
  * arrayEnumerate(arr) - Returns the array [1,2,3,..., length(arr)]
  *
  * arrayUniq(arr) - counts the number of different elements in the array,
  * arrayUniq(arr1, arr2, ...) - counts the number of different tuples from the elements in the corresponding positions in several arrays.
  *
  * arrayEnumerateUniq(arr)
  *  - outputs an array parallel (having same size) to this, where for each element specified
  *  how much times this element was encountered before (including this element) among elements with the same value.
  *  For example: arrayEnumerateUniq([10, 20, 10, 30]) = [1, 1, 2, 1]
  * arrayEnumerateUniq(arr1, arr2...)
  *  - for tuples from elements in the corresponding positions in several arrays.
  *
  * emptyArrayToSingle(arr) - replace empty arrays with arrays of one element with a default value.
  *
  * arrayReduce('agg', arr1, ...) - apply the aggregate function `agg` to arrays `arr1...`
  *  If multiple arrays passed, then elements on corresponding positions are passed as multiple arguments to the aggregate function.
  */


class FunctionArray : public IFunction
{
public:
    static constexpr auto name = "array";
    static FunctionPtr create(const Context & context);

    FunctionArray(const Context & context);

    bool useDefaultImplementationForNulls() const override { return false; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;

private:
    String getName() const override;

    bool addField(DataTypePtr type_res, const Field & f, Array & arr) const;
    static const DataTypePtr & getScalarType(const DataTypePtr & type);
    DataTypeTraits::EnrichedDataTypePtr getLeastCommonType(const DataTypes & arguments) const;

private:
    const Context & context;
};

namespace ArrayImpl
{
    class NullMapBuilder;
}

class FunctionArrayElement : public IFunction
{
public:
    static constexpr auto name = "arrayElement";
    static FunctionPtr create(const Context & context);

    String getName() const override;

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;

private:
    void perform(Block & block, const ColumnNumbers & arguments, size_t result, ArrayImpl::NullMapBuilder & builder);

    template <typename DataType>
    bool executeNumberConst(Block & block, const ColumnNumbers & arguments, size_t result, const Field & index,
        ArrayImpl::NullMapBuilder & builder);

    template <typename IndexType, typename DataType>
    bool executeNumber(Block & block, const ColumnNumbers & arguments, size_t result, const PaddedPODArray<IndexType> & indices,
        ArrayImpl::NullMapBuilder & builder);

    bool executeStringConst(Block & block, const ColumnNumbers & arguments, size_t result, const Field & index,
        ArrayImpl::NullMapBuilder & builder);

    template <typename IndexType>
    bool executeString(Block & block, const ColumnNumbers & arguments, size_t result, const PaddedPODArray<IndexType> & indices,
        ArrayImpl::NullMapBuilder & builder);

    bool executeGenericConst(Block & block, const ColumnNumbers & arguments, size_t result, const Field & index,
        ArrayImpl::NullMapBuilder & builder);

    template <typename IndexType>
    bool executeGeneric(Block & block, const ColumnNumbers & arguments, size_t result, const PaddedPODArray<IndexType> & indices,
        ArrayImpl::NullMapBuilder & builder);

    bool executeConstConst(Block & block, const ColumnNumbers & arguments, size_t result, const Field & index,
        ArrayImpl::NullMapBuilder & builder);

    template <typename IndexType>
    bool executeConst(Block & block, const ColumnNumbers & arguments, size_t result, const PaddedPODArray<IndexType> & indices,
        ArrayImpl::NullMapBuilder & builder);

    template <typename IndexType>
    bool executeArgument(Block & block, const ColumnNumbers & arguments, size_t result, ArrayImpl::NullMapBuilder & builder);

    /** For a tuple array, the function is evaluated component-wise for each element of the tuple.
      */
    bool executeTuple(Block & block, const ColumnNumbers & arguments, size_t result);
};


/// For has.
struct IndexToOne
{
    using ResultType = UInt8;
    static bool apply(size_t j, ResultType & current) { current = 1; return false; }
};

/// For indexOf.
struct IndexIdentity
{
    using ResultType = UInt64;
    /// The index is returned starting from 1.
    static bool apply(size_t j, ResultType & current) { current = j + 1; return false; }
};

/// For countEqual.
struct IndexCount
{
    using ResultType = UInt32;
    static bool apply(size_t j, ResultType & current) { ++current; return true; }
};


template <typename T, typename U, typename IndexConv>
struct ArrayIndexNumImpl
{
private:

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

    /// compares `lhs` against `i`-th element of `rhs`
    static bool compare(const T & lhs, const PaddedPODArray<U> & rhs, const size_t i ) { return lhs == rhs[i]; }
    /// compares `lhs against `rhs`, third argument unused
    static bool compare(const T & lhs, const U & rhs, size_t) { return lhs == rhs; }

#pragma GCC diagnostic pop

    static bool hasNull(const PaddedPODArray<U> & value, const PaddedPODArray<UInt8> & null_map, size_t i)
    {
        return null_map[i] == 1;
    }

    static bool hasNull(const U & value, const PaddedPODArray<UInt8> & null_map, size_t i)
    {
        throw Exception{"Logical error: constant column cannot have null map.", ErrorCodes::LOGICAL_ERROR};
    }

    /// Both function arguments are ordinary.
    template <typename ScalarOrVector>
    static void vectorCase1(
        const PaddedPODArray<T> & data, const ColumnArray::Offsets_t & offsets,
        const ScalarOrVector & value,
        PaddedPODArray<typename IndexConv::ResultType> & result)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset_t current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if (compare(data[current_offset + j], value, i))
                {
                    if (!IndexConv::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }

    /// The 2nd function argument is nullable.
    template <typename ScalarOrVector>
    static void vectorCase2(
        const PaddedPODArray<T> & data, const ColumnArray::Offsets_t & offsets,
        const ScalarOrVector & value,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> & null_map_item)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset_t current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if (!hasNull(value, null_map_item, i) && compare(data[current_offset + j], value, i))
                {
                    if (!IndexConv::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }

    /// The 1st function argument is a non-constant array of nullable values.
    template <typename ScalarOrVector>
    static void vectorCase3(
        const PaddedPODArray<T> & data, const ColumnArray::Offsets_t & offsets,
        const ScalarOrVector & value,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> & null_map_data)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset_t current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if (null_map_data[current_offset + j] == 1)
                {
                }
                else if (compare(data[current_offset + j], value, i))
                {
                    if (!IndexConv::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }

    /// The 1st function argument is a non-constant array of nullable values.
    /// The 2nd function argument is nullable.
    template <typename ScalarOrVector>
    static void vectorCase4(
        const PaddedPODArray<T> & data, const ColumnArray::Offsets_t & offsets,
        const ScalarOrVector & value,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> & null_map_data,
        const PaddedPODArray<UInt8> & null_map_item)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset_t current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                bool hit = false;
                if (null_map_data[current_offset + j] == 1)
                {
                    if (hasNull(value, null_map_item, i))
                        hit = true;
                }
                else if (compare(data[current_offset + j], value, i))
                    hit = true;

                if (hit)
                {
                    if (!IndexConv::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }

public:
    template <typename ScalarOrVector>
    static void vector(
        const PaddedPODArray<T> & data, const ColumnArray::Offsets_t & offsets,
        const ScalarOrVector & value,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> * null_map_data,
        const PaddedPODArray<UInt8> * null_map_item)
    {
        /// Processing is split into 4 cases.
        if (!null_map_data && !null_map_item)
            vectorCase1(data, offsets, value, result);
        else if (!null_map_data && null_map_item)
            vectorCase2(data, offsets, value, result, *null_map_item);
        else if (null_map_data && !null_map_item)
            vectorCase3(data, offsets, value, result, *null_map_data);
        else
            vectorCase4(data, offsets, value, result, *null_map_data, *null_map_item);
    }
};

/// Specialization that catches internal errors.
template <typename T, typename IndexConv>
struct ArrayIndexNumImpl<T, Null, IndexConv>
{
    template <typename ScalarOrVector>
    static void vector(
        const PaddedPODArray<T> & data, const ColumnArray::Offsets_t & offsets,
        const ScalarOrVector & value,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> * null_map_data,
        const PaddedPODArray<UInt8> * null_map_item)
    {
        throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};
    }
};

/// Implementation for arrays of numbers when the 2nd function argument
/// is a NULL value.
template <typename T, typename IndexConv>
struct ArrayIndexNumNullImpl
{
    static void vector(
        const PaddedPODArray<T> & data, const ColumnArray::Offsets_t & offsets,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> * null_map_data)
    {
        size_t size = offsets.size();
        result.resize(size);

        if (!null_map_data)
            return;

        const auto & null_map_ref = *null_map_data;

        ColumnArray::Offset_t current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if (null_map_ref[current_offset + j] == 1)
                {
                    if (!IndexConv::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }
};

/// Implementation for arrays of strings when the 2nd function argument
/// is a NULL value.
template <typename IndexConv>
struct ArrayIndexStringNullImpl
{
    static void vector_const(
        const ColumnString::Chars_t & data, const ColumnArray::Offsets_t & offsets, const ColumnString::Offsets_t & string_offsets,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> * null_map_data)
    {
        const auto size = offsets.size();
        result.resize(size);

        if (!null_map_data)
            return;

        const auto & null_map_ref = *null_map_data;

        ColumnArray::Offset_t current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            const auto array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                size_t k = (current_offset == 0 && j == 0) ? 0 : current_offset + j - 1;
                if (null_map_ref[k] == 1)
                {
                    if (!IndexConv::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }
};

template <typename IndexConv>
struct ArrayIndexStringImpl
{
    static void vector_const(
        const ColumnString::Chars_t & data, const ColumnArray::Offsets_t & offsets, const ColumnString::Offsets_t & string_offsets,
        const String & value,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> * null_map_data)
    {
        const auto size = offsets.size();
        const auto value_size = value.size();
        result.resize(size);

        ColumnArray::Offset_t current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            const auto array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                ColumnArray::Offset_t string_pos = current_offset == 0 && j == 0
                    ? 0
                    : string_offsets[current_offset + j - 1];

                ColumnArray::Offset_t string_size = string_offsets[current_offset + j] - string_pos;

                size_t k = (current_offset == 0 && j == 0) ? 0 : current_offset + j - 1;
                if (null_map_data && ((*null_map_data)[k] == 1))
                {
                }
                else if (string_size == value_size + 1 && 0 == memcmp(value.data(), &data[string_pos], value_size))
                {
                    if (!IndexConv::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }

    static void vector_vector(
        const ColumnString::Chars_t & data, const ColumnArray::Offsets_t & offsets, const ColumnString::Offsets_t & string_offsets,
        const ColumnString::Chars_t & item_values, const ColumnString::Offsets_t & item_offsets,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> * null_map_data,
        const PaddedPODArray<UInt8> * null_map_item)
    {
        const auto size = offsets.size();
        result.resize(size);

        ColumnArray::Offset_t current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            const auto array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;
            const auto value_pos = 0 == i ? 0 : item_offsets[i - 1];
            const auto value_size = item_offsets[i] - value_pos;

            for (size_t j = 0; j < array_size; ++j)
            {
                ColumnArray::Offset_t string_pos = current_offset == 0 && j == 0
                                                   ? 0
                                                   : string_offsets[current_offset + j - 1];

                ColumnArray::Offset_t string_size = string_offsets[current_offset + j] - string_pos;

                bool hit = false;
                size_t k = (current_offset == 0 && j == 0) ? 0 : current_offset + j - 1;

                if (null_map_data && ((*null_map_data)[k] == 1))
                {
                    if (null_map_item && ((*null_map_item)[i] == 1))
                        hit = true;
                }
                else if (string_size == value_size && 0 == memcmp(&item_values[value_pos], &data[string_pos], value_size))
                        hit = true;

                if (hit)
                {
                    if (!IndexConv::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }
};

/// Catch-all implementation for arrays of arbitary type.
/// To compare with constant value, create non-constant column with single element,
/// and pass is_value_has_single_element_to_compare = true.
template <typename IndexConv, bool is_value_has_single_element_to_compare>
struct ArrayIndexGenericImpl
{
private:
    /// Both function arguments are ordinary.
    static void vectorCase1(
        const IColumn & data, const ColumnArray::Offsets_t & offsets,
        const IColumn & value,
        PaddedPODArray<typename IndexConv::ResultType> & result)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset_t current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if (0 == data.compareAt(current_offset + j, is_value_has_single_element_to_compare ? 0 : i, value, 1))
                {
                    if (!IndexConv::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }

    /// The 2nd function argument is nullable.
    static void vectorCase2(
        const IColumn & data, const ColumnArray::Offsets_t & offsets,
        const IColumn & value,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> & null_map_item)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset_t current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if ((null_map_item[i] == 0) &&
                    (0 == data.compareAt(current_offset + j, is_value_has_single_element_to_compare ? 0 : i, value, 1)))
                {
                    if (!IndexConv::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }

    /// The 1st function argument is a non-constant array of nullable values.
    static void vectorCase3(
        const IColumn & data, const ColumnArray::Offsets_t & offsets,
        const IColumn & value,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> & null_map_data)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset_t current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if (null_map_data[current_offset + j] == 1)
                {
                }
                else if (0 == data.compareAt(current_offset + j, is_value_has_single_element_to_compare ? 0 : i, value, 1))
                {
                    if (!IndexConv::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }

    /// The 1st function argument is a non-constant array of nullable values.
    /// The 2nd function argument is nullable.
    static void vectorCase4(
        const IColumn & data, const ColumnArray::Offsets_t & offsets,
        const IColumn & value,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> & null_map_data,
        const PaddedPODArray<UInt8> & null_map_item)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset_t current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                bool hit = false;
                if (null_map_data[current_offset + j] == 1)
                {
                    if (null_map_item[i] == 1)
                        hit = true;
                }
                else if (0 == data.compareAt(current_offset + j, is_value_has_single_element_to_compare ? 0 : i, value, 1))
                        hit = true;

                if (hit)
                {
                    if (!IndexConv::apply(j, current))
                        break;
                }
            }
        }
    }

public:
    static void vector(
        const IColumn & data, const ColumnArray::Offsets_t & offsets,
        const IColumn & value,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> * null_map_data,
        const PaddedPODArray<UInt8> * null_map_item)
    {
        /// Processing is split into 4 cases.
        if (!null_map_data && !null_map_item)
            vectorCase1(data, offsets, value, result);
        else if (!null_map_data && null_map_item)
            vectorCase2(data, offsets, value, result, *null_map_item);
        else if (null_map_data && !null_map_item)
            vectorCase3(data, offsets, value, result, *null_map_data);
        else
            vectorCase4(data, offsets, value, result, *null_map_data, *null_map_item);
    }
};

/// Catch-all implementation for arrays of arbitary type
/// when the 2nd function argument is a NULL value.
template <typename IndexConv>
struct ArrayIndexGenericNullImpl
{
    static void vector(
        const IColumn & data, const ColumnArray::Offsets_t & offsets,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> * null_map_data)
    {
        size_t size = offsets.size();
        result.resize(size);

        if (!null_map_data)
            return;

        const auto & null_map_ref = *null_map_data;

        ColumnArray::Offset_t current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if (null_map_ref[current_offset + j] == 1)
                {
                    if (!IndexConv::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }
};

template <typename IndexConv, typename Name>
class FunctionArrayIndex : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionArrayIndex>(); }

private:
    using ResultColumnType = ColumnVector<typename IndexConv::ResultType>;

    template <typename T>
    bool executeNumber(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        return executeNumberNumber<T, UInt8>(block, arguments, result)
            || executeNumberNumber<T, UInt16>(block, arguments, result)
            || executeNumberNumber<T, UInt32>(block, arguments, result)
            || executeNumberNumber<T, UInt64>(block, arguments, result)
            || executeNumberNumber<T, Int8>(block, arguments, result)
            || executeNumberNumber<T, Int16>(block, arguments, result)
            || executeNumberNumber<T, Int32>(block, arguments, result)
            || executeNumberNumber<T, Int64>(block, arguments, result)
            || executeNumberNumber<T, Float32>(block, arguments, result)
            || executeNumberNumber<T, Float64>(block, arguments, result)
            || executeNumberNumber<T, Null>(block, arguments, result);
    }

    template <typename T, typename U>
    bool executeNumberNumber(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());

        if (!col_array)
            return false;

        const ColumnVector<T> * col_nested = checkAndGetColumn<ColumnVector<T>>(&col_array->getData());

        if (!col_nested)
            return false;

        const auto col_res = std::make_shared<ResultColumnType>();
        block.getByPosition(result).column = col_res;

        /// Null maps of the 1st and second function arguments,
        /// if it applies.
        const PaddedPODArray<UInt8> * null_map_data = nullptr;
        const PaddedPODArray<UInt8> * null_map_item = nullptr;

        if (arguments.size() > 2)
        {
            const auto & null_map1 = block.getByPosition(arguments[2]).column;
            if (null_map1)
                null_map_data = &static_cast<const ColumnUInt8 &>(*null_map1).getData();

            const auto & null_map2 = block.getByPosition(arguments[3]).column;
            if (null_map2)
                null_map_item = &static_cast<const ColumnUInt8 &>(*null_map2).getData();
        }

        const auto item_arg = block.getByPosition(arguments[1]).column.get();

        if (item_arg->isNull())
            ArrayIndexNumNullImpl<T, IndexConv>::vector(col_nested->getData(), col_array->getOffsets(),
                col_res->getData(), null_map_data);
        else if (const auto item_arg_const = checkAndGetColumnConst<ColumnVector<U>>(item_arg))
            ArrayIndexNumImpl<T, U, IndexConv>::vector(col_nested->getData(), col_array->getOffsets(),
                item_arg_const->template getValue<U>(), col_res->getData(), null_map_data, nullptr);
        else if (const auto item_arg_vector = checkAndGetColumn<ColumnVector<U>>(item_arg))
            ArrayIndexNumImpl<T, U, IndexConv>::vector(col_nested->getData(), col_array->getOffsets(),
                item_arg_vector->getData(), col_res->getData(), null_map_data, null_map_item);
        else
            return false;

        return true;
    }

    bool executeString(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());

        if (!col_array)
            return false;

        const ColumnString * col_nested = checkAndGetColumn<ColumnString>(&col_array->getData());

        if (!col_nested)
            return false;

        const auto col_res = std::make_shared<ResultColumnType>();
        block.getByPosition(result).column = col_res;

        /// Null maps of the 1st and second function arguments,
        /// if it applies.
        const PaddedPODArray<UInt8> * null_map_data = nullptr;
        const PaddedPODArray<UInt8> * null_map_item = nullptr;

        if (arguments.size() > 2)
        {
            const auto & col1 = block.getByPosition(arguments[2]).column;
            if (col1)
                null_map_data = &static_cast<const ColumnUInt8 &>(*col1).getData();

            const auto & col2 = block.getByPosition(arguments[3]).column;
            if (col2)
                null_map_item = &static_cast<const ColumnUInt8 &>(*col2).getData();
        }

        const auto item_arg = block.getByPosition(arguments[1]).column.get();

        if (item_arg->isNull())
            ArrayIndexStringNullImpl<IndexConv>::vector_const(col_nested->getChars(), col_array->getOffsets(),
                col_nested->getOffsets(), col_res->getData(), null_map_data);
        else if (const auto item_arg_const = checkAndGetColumnConstStringOrFixedString(item_arg))
            ArrayIndexStringImpl<IndexConv>::vector_const(col_nested->getChars(), col_array->getOffsets(),
                col_nested->getOffsets(), item_arg_const->getValue<String>(), col_res->getData(),
                null_map_data);
        else if (const auto item_arg_vector = checkAndGetColumn<ColumnString>(item_arg))
            ArrayIndexStringImpl<IndexConv>::vector_vector(col_nested->getChars(), col_array->getOffsets(),
                col_nested->getOffsets(), item_arg_vector->getChars(), item_arg_vector->getOffsets(),
                col_res->getData(), null_map_data, null_map_item);
        else
            return false;

        return true;
    }

    bool executeConst(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        const ColumnConst * col_array = checkAndGetColumnConst<ColumnArray>(block.getByPosition(arguments[0]).column.get());

        if (!col_array)
            return false;

        Array arr = col_array->getValue<Array>();

        const auto item_arg = block.getByPosition(arguments[1]).column.get();
        if (item_arg->isConst())
        {
            typename IndexConv::ResultType current = 0;
            const auto & value = (*item_arg)[0];

            for (size_t i = 0, size = arr.size(); i < size; ++i)
            {
                if (applyVisitor(FieldVisitorAccurateEquals(), arr[i], value))
                {
                    if (!IndexConv::apply(i, current))
                        break;
                }
            }

            block.getByPosition(result).column = block.getByPosition(result).type->createConstColumn(
                item_arg->size(),
                static_cast<typename NearestFieldType<typename IndexConv::ResultType>::Type>(current));
        }
        else
        {
            /// Null map of the 2nd function argument, if it applies.
            const PaddedPODArray<UInt8> * null_map = nullptr;

            if (arguments.size() > 2)
            {
                const auto & col = block.getByPosition(arguments[3]).column;
                if (col)
                    null_map = &static_cast<const ColumnUInt8 &>(*col).getData();
            }

            const auto size = item_arg->size();
            const auto col_res = std::make_shared<ResultColumnType>(size);
            block.getByPosition(result).column = col_res;

            auto & data = col_res->getData();

            for (size_t row = 0; row < size; ++row)
            {
                const auto & value = (*item_arg)[row];

                data[row] = 0;
                for (size_t i = 0, size = arr.size(); i < size; ++i)
                {
                    bool hit = false;

                    if (arr[i].isNull())
                    {
                        if (null_map && ((*null_map)[row] == 1))
                            hit = true;
                    }
                    else if (applyVisitor(FieldVisitorAccurateEquals(), arr[i], value))
                        hit = true;

                    if (hit)
                    {
                        if (!IndexConv::apply(i, data[row]))
                            break;
                    }
                }
            }
        }

        return true;
    }

    bool executeGeneric(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());

        if (!col_array)
            return false;

        const IColumn & col_nested = col_array->getData();
        const IColumn & item_arg = *block.getByPosition(arguments[1]).column;

        const auto col_res = std::make_shared<ResultColumnType>();
        block.getByPosition(result).column = col_res;

        /// Null maps of the 1st and second function arguments,
        /// if it applies.
        const PaddedPODArray<UInt8> * null_map_data = nullptr;
        const PaddedPODArray<UInt8> * null_map_item = nullptr;

        if (arguments.size() > 2)
        {
            const auto & null_map1 = block.getByPosition(arguments[2]).column;
            if (null_map1)
                null_map_data = &static_cast<const ColumnUInt8 &>(*null_map1).getData();

            const auto & null_map2 = block.getByPosition(arguments[3]).column;
            if (null_map2)
                null_map_item = &static_cast<const ColumnUInt8 &>(*null_map2).getData();
        }

        if (item_arg.isNull())
            ArrayIndexGenericNullImpl<IndexConv>::vector(col_nested, col_array->getOffsets(),
                col_res->getData(), null_map_data);
        else if (item_arg.isConst())
            ArrayIndexGenericImpl<IndexConv, true>::vector(col_nested, col_array->getOffsets(),
                static_cast<const ColumnConst &>(item_arg).getDataColumn(), col_res->getData(),    /// TODO This is wrong.
                null_map_data, nullptr);
        else
        {
            /// If item_arg is tuple and have constants.
            if (auto materialized_tuple = item_arg.convertToFullColumnIfConst())
                ArrayIndexGenericImpl<IndexConv, false>::vector(
                    col_nested, col_array->getOffsets(), *materialized_tuple, col_res->getData(),
                    null_map_data, null_map_item);
            else
                ArrayIndexGenericImpl<IndexConv, false>::vector(
                    col_nested, col_array->getOffsets(), item_arg, col_res->getData(),
                    null_map_data, null_map_item);
        }

        return true;
    }


public:
    /// Get function name.
    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!array_type)
            throw Exception("First argument for function " + getName() + " must be an array.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isNull())
        {
            const IDataType * observed_type0 = DataTypeTraits::removeNullable(array_type->getNestedType()).get();
            const IDataType * observed_type1 = DataTypeTraits::removeNullable(arguments[1]).get();

            if (!(observed_type0->behavesAsNumber() && observed_type1->behavesAsNumber())
                && !observed_type0->equals(*observed_type1))
                throw Exception("Types of array and 2nd argument of function "
                    + getName() + " must be identical up to nullability. Passed: "
                    + arguments[0]->getName() + " and " + arguments[1]->getName() + ".",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeNumber<typename IndexConv::ResultType>>();
    }

    /// Perform function on the given block.
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        /// If one or both arguments passed to this function are nullable,
        /// we create a new block that contains non-nullable arguments:
        /// - if the 1st argument is a non-constant array of nullable values,
        /// it is turned into a non-constant array of ordinary values + a null
        /// byte map;
        /// - if the 2nd argument is a nullable value, it is turned into an
        /// ordinary value + a null byte map.
        /// Note that since constant arrays have quite a specific structure
        /// (they are vectors of Fields, which may represent the NULL value),
        /// they do not require any preprocessing

        /// Check if the 1st function argument is a non-constant array of nullable
        /// values.
        bool is_nullable;

        const ColumnArray * col_array = nullptr;
        col_array = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());
        if (col_array)
            is_nullable = col_array->getData().isNullable();
        else
            is_nullable = false;

        /// Check nullability of the 2nd function argument.
        bool is_arg_nullable = block.getByPosition(arguments[1]).column->isNullable();

        if (!is_nullable && !is_arg_nullable)
        {
            /// Simple case: no nullable value is passed.
            perform(block, arguments, result);
        }
        else
        {
            /// Template of the block on which we will actually apply the function.
            /// Its elements will be filled later.
            Block source_block =
            {
                /// 1st function argument (data)
                {
                },

                /// 2nd function argument
                {
                },

                /// 1st argument null map
                {
                },

                /// 2nd argument null map
                {
                },

                /// Function result.
                {
                    nullptr,
                    block.getByPosition(result).type,
                    ""
                }
            };

            if (is_nullable)
            {
                const auto & nullable_col = static_cast<const ColumnNullable &>(col_array->getData());
                const auto & nested_col = nullable_col.getNestedColumn();

                auto & data = source_block.getByPosition(0);
                data.column = std::make_shared<ColumnArray>(nested_col, col_array->getOffsetsColumn());
                data.type = static_cast<const DataTypeNullable &>(*block.getByPosition(arguments[0]).type).getNestedType();

                auto & null_map = source_block.getByPosition(2);
                null_map.column = nullable_col.getNullMapColumn();
                null_map.type = std::make_shared<DataTypeUInt8>();
            }
            else
            {
                auto & data = source_block.getByPosition(0);
                data = block.getByPosition(arguments[0]);
            }

            if (is_arg_nullable)
            {
                const auto & col = block.getByPosition(arguments[1]).column;
                const auto & nullable_col = static_cast<const ColumnNullable &>(*col);

                auto & arg = source_block.getByPosition(1);
                arg.column = nullable_col.getNestedColumn();
                arg.type = static_cast<const DataTypeNullable &>(*block.getByPosition(arguments[1]).type).getNestedType();

                auto & null_map = source_block.getByPosition(3);
                null_map.column = nullable_col.getNullMapColumn();
                null_map.type = std::make_shared<DataTypeUInt8>();
            }
            else
            {
                auto & arg = source_block.getByPosition(1);
                arg = block.getByPosition(arguments[1]);
            }

            /// Now perform the function.
            perform(source_block, {0, 1, 2, 3}, 4);

            /// Move the result to its final position.
            const ColumnWithTypeAndName & source_col = source_block.getByPosition(4);
            ColumnWithTypeAndName & dest_col = block.getByPosition(result);
            dest_col.column = std::move(source_col.column);
        }
    }

private:
    /// Perform function on the given block. Internal version.
    void perform(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (!(executeNumber<UInt8>(block, arguments, result)
            || executeNumber<UInt16>(block, arguments, result)
            || executeNumber<UInt32>(block, arguments, result)
            || executeNumber<UInt64>(block, arguments, result)
            || executeNumber<Int8>(block, arguments, result)
            || executeNumber<Int16>(block, arguments, result)
            || executeNumber<Int32>(block, arguments, result)
            || executeNumber<Int64>(block, arguments, result)
            || executeNumber<Float32>(block, arguments, result)
            || executeNumber<Float64>(block, arguments, result)
            || executeConst(block, arguments, result)
            || executeString(block, arguments, result)
            || executeGeneric(block, arguments, result)))
            throw Exception{
                "Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN};
    }
};


class FunctionArrayEnumerate : public IFunction
{
public:
    static constexpr auto name = "arrayEnumerate";
    static FunctionPtr create(const Context & context);

    String getName() const override;

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


/// Counts the number of different elements in the array, or the number of different tuples from the elements at the corresponding positions in several arrays.
/// NOTE The implementation partially matches arrayEnumerateUniq.
class FunctionArrayUniq : public IFunction
{
public:
    static constexpr auto name = "arrayUniq";
    static FunctionPtr create(const Context & context);

    String getName() const override;

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;

private:
    /// Initially allocate a piece of memory for 512 elements. NOTE: This is just a guess.
    static constexpr size_t INITIAL_SIZE_DEGREE = 9;

    template <typename T>
    bool executeNumber(const ColumnArray * array,  const IColumn * null_map, ColumnUInt32::Container_t & res_values);

    bool executeString(const ColumnArray * array,  const IColumn * null_map, ColumnUInt32::Container_t & res_values);

    bool executeConst(Block & block, const ColumnNumbers & arguments, size_t result);

    bool execute128bit(
        const ColumnArray::Offsets_t & offsets,
        const ConstColumnPlainPtrs & columns,
        const ConstColumnPlainPtrs & null_maps,
        ColumnUInt32::Container_t & res_values,
        bool has_nullable_columns);

    void executeHashed(
        const ColumnArray::Offsets_t & offsets,
        const ConstColumnPlainPtrs & columns,
        ColumnUInt32::Container_t & res_values);
};


class FunctionArrayEnumerateUniq : public IFunction
{
public:
    static constexpr auto name = "arrayEnumerateUniq";
    static FunctionPtr create(const Context & context);

    String getName() const override;

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;

private:
    /// Initially allocate a piece of memory for 512 elements. NOTE: This is just a guess.
    static constexpr size_t INITIAL_SIZE_DEGREE = 9;

    template <typename T>
    bool executeNumber(const ColumnArray * array, const IColumn * null_map, ColumnUInt32::Container_t & res_values);

    bool executeString(const ColumnArray * array, const IColumn * null_map, ColumnUInt32::Container_t & res_values);

    bool executeConst(Block & block, const ColumnNumbers & arguments, size_t result);

    bool execute128bit(
        const ColumnArray::Offsets_t & offsets,
        const ConstColumnPlainPtrs & columns,
        const ConstColumnPlainPtrs & null_maps,
        ColumnUInt32::Container_t & res_values,
        bool has_nullable_columns);

    void executeHashed(
        const ColumnArray::Offsets_t & offsets,
        const ConstColumnPlainPtrs & columns,
        ColumnUInt32::Container_t & res_values);
};


template <typename Type> struct TypeToColumnType { using ColumnType = ColumnVector<Type>; };
template <> struct TypeToColumnType<String> { using ColumnType = ColumnString; };

template <typename DataType> struct DataTypeToName : TypeName<typename DataType::FieldType> { };
template <> struct DataTypeToName<DataTypeDate> { static std::string get() { return "Date"; } };
template <> struct DataTypeToName<DataTypeDateTime> { static std::string get() { return "DateTime"; } };

template <typename DataType>
struct FunctionEmptyArray : public IFunction
{
    static constexpr auto base_name = "emptyArray";
    static const String name;
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionEmptyArray>(); }

private:
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataType>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        using UnderlyingColumnType = typename TypeToColumnType<typename DataType::FieldType>::ColumnType;

        block.getByPosition(result).column = std::make_shared<ColumnArray>(
            std::make_shared<UnderlyingColumnType>(),
            std::make_shared<ColumnArray::ColumnOffsets_t>(block.rows(), 0));
    }
};

template <typename DataType>
const String FunctionEmptyArray<DataType>::name = FunctionEmptyArray::base_name + String(DataTypeToName<DataType>::get());

class FunctionRange : public IFunction
{
public:
    static constexpr auto name = "range";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionRange>(); }

private:
    String getName() const override;

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    template <typename T>
    bool executeInternal(Block & block, const IColumn * arg, const size_t result);

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override;
};


class FunctionEmptyArrayToSingle : public IFunction
{
public:
    static constexpr auto name = "emptyArrayToSingle";
    static FunctionPtr create(const Context & context);

    String getName() const override;

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


class FunctionArrayReverse : public IFunction
{
public:
    static constexpr auto name = "arrayReverse";
    static FunctionPtr create(const Context & context);

    String getName() const override;

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;

private:
    bool executeConst(Block & block, const ColumnNumbers & arguments, size_t result);

    template <typename T>
    bool executeNumber(
        const IColumn & src_data, const ColumnArray::Offsets_t & src_offsets,
        IColumn & res_data_col,
        const ColumnNullable * nullable_col,
        ColumnNullable * nullable_res_col);

    bool executeFixedString(
        const IColumn & src_data, const ColumnArray::Offsets_t & src_offsets,
        IColumn & res_data_col,
        const ColumnNullable * nullable_col,
        ColumnNullable * nullable_res_col);

    bool executeString(
        const IColumn & src_data, const ColumnArray::Offsets_t & src_array_offsets,
        IColumn & res_data_col,
        const ColumnNullable * nullable_col,
        ColumnNullable * nullable_res_col);
};


class IAggregateFunction;
using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;

/** Applies an aggregate function to array and returns its result.
  * If aggregate function has multiple arguments, then this function can be applied to multiple arrays of the same size.
  */
class FunctionArrayReduce : public IFunction
{
public:
    static constexpr auto name = "arrayReduce";
    static FunctionPtr create(const Context & context);

    String getName() const override;

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    void getReturnTypeAndPrerequisitesImpl(
        const ColumnsWithTypeAndName & arguments,
        DataTypePtr & out_return_type,
        std::vector<ExpressionAction> & out_prerequisites) override;

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
private:
    AggregateFunctionPtr aggregate_function;
};


struct NameHas { static constexpr auto name = "has"; };
struct NameIndexOf { static constexpr auto name = "indexOf"; };
struct NameCountEqual { static constexpr auto name = "countEqual"; };

using FunctionHas = FunctionArrayIndex<IndexToOne, NameHas>;
using FunctionIndexOf = FunctionArrayIndex<IndexIdentity, NameIndexOf>;
using FunctionCountEqual = FunctionArrayIndex<IndexCount, NameCountEqual>;

using FunctionEmptyArrayUInt8 = FunctionEmptyArray<DataTypeUInt8>;
using FunctionEmptyArrayUInt16 = FunctionEmptyArray<DataTypeUInt16>;
using FunctionEmptyArrayUInt32 = FunctionEmptyArray<DataTypeUInt32>;
using FunctionEmptyArrayUInt64 = FunctionEmptyArray<DataTypeUInt64>;
using FunctionEmptyArrayInt8 = FunctionEmptyArray<DataTypeInt8>;
using FunctionEmptyArrayInt16 = FunctionEmptyArray<DataTypeInt16>;
using FunctionEmptyArrayInt32 = FunctionEmptyArray<DataTypeInt32>;
using FunctionEmptyArrayInt64 = FunctionEmptyArray<DataTypeInt64>;
using FunctionEmptyArrayFloat32 = FunctionEmptyArray<DataTypeFloat32>;
using FunctionEmptyArrayFloat64 = FunctionEmptyArray<DataTypeFloat64>;
using FunctionEmptyArrayDate = FunctionEmptyArray<DataTypeDate>;
using FunctionEmptyArrayDateTime = FunctionEmptyArray<DataTypeDateTime>;
using FunctionEmptyArrayString = FunctionEmptyArray<DataTypeString>;


}
