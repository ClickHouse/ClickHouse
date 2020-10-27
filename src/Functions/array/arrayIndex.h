#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <Common/memcmpSmall.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// For has.
struct IndexToOne
{
    using ResultType = UInt8;
    static bool apply(size_t, ResultType & current) { current = 1; return false; }
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
    using ResultType = UInt64;
    static bool apply(size_t, ResultType & current) { ++current; return true; }
};


template <typename T, typename U, typename IndexConv>
struct ArrayIndexNumImpl
{
private:

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

    /// compares `lhs` against `i`-th element of `rhs`
    static bool compare(const T & lhs, const PaddedPODArray<U> & rhs, const size_t i) { return lhs == rhs[i]; }
    /// compares `lhs against `rhs`, third argument unused
    static bool compare(const T & lhs, const U & rhs, size_t) { return lhs == rhs; }

#pragma GCC diagnostic pop

    static bool hasNull(const PaddedPODArray<UInt8> & null_map, size_t i)
    {
        return null_map[i];
    }

    /// Both function arguments are ordinary.
    template <typename ScalarOrVector>
    static void vectorCase1(
        const PaddedPODArray<T> & data, const ColumnArray::Offsets & offsets,
        const ScalarOrVector & value,
        PaddedPODArray<typename IndexConv::ResultType> & result)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
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
        const PaddedPODArray<T> & data, const ColumnArray::Offsets & offsets,
        const ScalarOrVector & value,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> & null_map_item)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if (!hasNull(null_map_item, i) && compare(data[current_offset + j], value, i))
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
        const PaddedPODArray<T> & data, const ColumnArray::Offsets & offsets,
        const ScalarOrVector & value,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> & null_map_data)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if (null_map_data[current_offset + j])
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
        const PaddedPODArray<T> & data, const ColumnArray::Offsets & offsets,
        const ScalarOrVector & value,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> & null_map_data,
        const PaddedPODArray<UInt8> & null_map_item)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                bool hit = false;
                if (null_map_data[current_offset + j])
                {
                    if (hasNull(null_map_item, i))
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
        const PaddedPODArray<T> & data, const ColumnArray::Offsets & offsets,
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


/// Implementation for arrays of numbers when the 2nd function argument
/// is a NULL value.
template <typename T, typename IndexConv>
struct ArrayIndexNumNullImpl
{
    static void vector(
        const PaddedPODArray<T> & /*data*/, const ColumnArray::Offsets & offsets,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> * null_map_data)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if (null_map_data && (*null_map_data)[current_offset + j])
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

/// Implementation for arrays of strings when the 2nd function argument is a NULL value.
template <typename IndexConv>
struct ArrayIndexStringNullImpl
{
    static void vector_const(
        const ColumnString::Chars & /*data*/, const ColumnArray::Offsets & offsets, const ColumnString::Offsets & /*string_offsets*/,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> * null_map_data)
    {
        const auto size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            const auto array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if (null_map_data && (*null_map_data)[current_offset + j])
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
        const ColumnString::Chars & data, const ColumnArray::Offsets & offsets, const ColumnString::Offsets & string_offsets,
        const ColumnString::Chars & value, ColumnString::Offset value_size,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> * null_map_data)
    {
        const auto size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            const auto array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                ColumnArray::Offset string_pos = current_offset == 0 && j == 0
                    ? 0
                    : string_offsets[current_offset + j - 1];

                ColumnArray::Offset string_size = string_offsets[current_offset + j] - string_pos - 1;

                if (null_map_data && (*null_map_data)[current_offset + j])
                {
                }
                else if (memequalSmallAllowOverflow15(value.data(), value_size, &data[string_pos], string_size))
                {
                    if (!IndexConv::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }

    static void vectorVector(
        const ColumnString::Chars & data, const ColumnArray::Offsets & offsets, const ColumnString::Offsets & string_offsets,
        const ColumnString::Chars & item_values, const ColumnString::Offsets & item_offsets,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> * null_map_data,
        const PaddedPODArray<UInt8> * null_map_item)
    {
        const auto size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            const auto array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;
            const auto value_pos = 0 == i ? 0 : item_offsets[i - 1];
            const auto value_size = item_offsets[i] - value_pos;

            for (size_t j = 0; j < array_size; ++j)
            {
                ColumnArray::Offset string_pos = current_offset == 0 && j == 0
                    ? 0
                    : string_offsets[current_offset + j - 1];

                ColumnArray::Offset string_size = string_offsets[current_offset + j] - string_pos;

                bool hit = false;

                if (null_map_data && (*null_map_data)[current_offset + j])
                {
                    if (null_map_item && (*null_map_item)[i])
                        hit = true;
                }
                else if (memequalSmallAllowOverflow15(&item_values[value_pos], value_size, &data[string_pos], string_size))
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

/// Catch-all implementation for arrays of arbitrary type.
/// To compare with constant value, create non-constant column with single element,
/// and pass is_value_has_single_element_to_compare = true.
template <typename IndexConv, bool is_value_has_single_element_to_compare>
struct ArrayIndexGenericImpl
{
private:
    /// Both function arguments are ordinary.
    static void vectorCase1(
        const IColumn & data, const ColumnArray::Offsets & offsets,
        const IColumn & value,
        PaddedPODArray<typename IndexConv::ResultType> & result)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
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
        const IColumn & data, const ColumnArray::Offsets & offsets,
        const IColumn & value,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> & null_map_item)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
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
        const IColumn & data, const ColumnArray::Offsets & offsets,
        const IColumn & value,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> & null_map_data)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if (null_map_data[current_offset + j])
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
        const IColumn & data, const ColumnArray::Offsets & offsets,
        const IColumn & value,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> & null_map_data,
        const PaddedPODArray<UInt8> & null_map_item)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                bool hit = false;
                if (null_map_data[current_offset + j])
                {
                    if (null_map_item[i])
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
        const IColumn & data, const ColumnArray::Offsets & offsets,
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

/// Catch-all implementation for arrays of arbitrary type
/// when the 2nd function argument is a NULL value.
template <typename IndexConv>
struct ArrayIndexGenericNullImpl
{
    static void vector(
        const IColumn & /*data*/, const ColumnArray::Offsets & offsets,
        PaddedPODArray<typename IndexConv::ResultType> & result,
        const PaddedPODArray<UInt8> * null_map_data)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename IndexConv::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if (null_map_data && (*null_map_data)[current_offset + j])
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


inline bool allowArrayIndex(const DataTypePtr & type0, const DataTypePtr & type1)
{
    DataTypePtr data_type0 = removeNullable(type0);
    DataTypePtr data_type1 = removeNullable(type1);

    return ((isNativeNumber(data_type0) || isEnum(data_type0)) && isNativeNumber(data_type1))
        || data_type0->equals(*data_type1);
}


template <typename IndexConv, typename Name>
class FunctionArrayIndex : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionArrayIndex>(); }

private:
    using ResultColumnType = ColumnVector<typename IndexConv::ResultType>;

    template <typename T>
    bool executeNumber(Block & block, const ColumnNumbers & arguments, size_t result) const
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
            || executeNumberNumber<T, Float64>(block, arguments, result);
    }

    template <typename T, typename U>
    bool executeNumberNumber(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());

        if (!col_array)
            return false;

        const ColumnVector<T> * col_nested = checkAndGetColumn<ColumnVector<T>>(&col_array->getData());

        if (!col_nested)
            return false;

        auto col_res = ResultColumnType::create();

        /// Null maps of the 1st and second function arguments,
        /// if it applies.
        const PaddedPODArray<UInt8> * null_map_data = nullptr;
        const PaddedPODArray<UInt8> * null_map_item = nullptr;

        if (arguments.size() > 2)
        {
            const auto & null_map1 = block.getByPosition(arguments[2]).column;
            if (null_map1)
                null_map_data = &assert_cast<const ColumnUInt8 &>(*null_map1).getData();

            const auto & null_map2 = block.getByPosition(arguments[3]).column;
            if (null_map2)
                null_map_item = &assert_cast<const ColumnUInt8 &>(*null_map2).getData();
        }

        const auto item_arg = block.getByPosition(arguments[1]).column.get();

        if (item_arg->onlyNull())
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

        block.getByPosition(result).column = std::move(col_res);
        return true;
    }

    bool executeString(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());

        if (!col_array)
            return false;

        const ColumnString * col_nested = checkAndGetColumn<ColumnString>(&col_array->getData());

        if (!col_nested)
            return false;

        auto col_res = ResultColumnType::create();

        /// Null maps of the 1st and second function arguments,
        /// if it applies.
        const PaddedPODArray<UInt8> * null_map_data = nullptr;
        const PaddedPODArray<UInt8> * null_map_item = nullptr;

        if (arguments.size() > 2)
        {
            const auto & col1 = block.getByPosition(arguments[2]).column;
            if (col1)
                null_map_data = &assert_cast<const ColumnUInt8 &>(*col1).getData();

            const auto & col2 = block.getByPosition(arguments[3]).column;
            if (col2)
                null_map_item = &assert_cast<const ColumnUInt8 &>(*col2).getData();
        }

        const auto item_arg = block.getByPosition(arguments[1]).column.get();

        if (item_arg->onlyNull())
        {
            ArrayIndexStringNullImpl<IndexConv>::vector_const(col_nested->getChars(), col_array->getOffsets(),
                col_nested->getOffsets(), col_res->getData(), null_map_data);
        }
        else if (const auto item_arg_const = checkAndGetColumnConstStringOrFixedString(item_arg))
        {
            const ColumnString * item_const_string = checkAndGetColumn<ColumnString>(&item_arg_const->getDataColumn());
            const ColumnFixedString * item_const_fixedstring = checkAndGetColumn<ColumnFixedString>(&item_arg_const->getDataColumn());

            if (item_const_string)
                ArrayIndexStringImpl<IndexConv>::vector_const(col_nested->getChars(), col_array->getOffsets(), col_nested->getOffsets(),
                    item_const_string->getChars(), item_const_string->getDataAt(0).size,
                    col_res->getData(), null_map_data);
            else if (item_const_fixedstring)
                ArrayIndexStringImpl<IndexConv>::vector_const(col_nested->getChars(), col_array->getOffsets(), col_nested->getOffsets(),
                    item_const_fixedstring->getChars(), item_const_fixedstring->getN(),
                    col_res->getData(), null_map_data);
            else
                throw Exception("Logical error: ColumnConst contains not String nor FixedString column", ErrorCodes::ILLEGAL_COLUMN);
        }
        else if (const auto item_arg_vector = checkAndGetColumn<ColumnString>(item_arg))
        {
            ArrayIndexStringImpl<IndexConv>::vectorVector(col_nested->getChars(), col_array->getOffsets(),
                col_nested->getOffsets(), item_arg_vector->getChars(), item_arg_vector->getOffsets(),
                col_res->getData(), null_map_data, null_map_item);
        }
        else
            return false;

        block.getByPosition(result).column = std::move(col_res);
        return true;
    }

    bool executeConst(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        const ColumnConst * col_array = checkAndGetColumnConst<ColumnArray>(block.getByPosition(arguments[0]).column.get());

        if (!col_array)
            return false;

        Array arr = col_array->getValue<Array>();

        const auto item_arg = block.getByPosition(arguments[1]).column.get();
        if (isColumnConst(*item_arg))
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

            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(
                item_arg->size(),
                static_cast<typename IndexConv::ResultType>(current));
        }
        else
        {
            /// Null map of the 2nd function argument, if it applies.
            const PaddedPODArray<UInt8> * null_map = nullptr;

            if (arguments.size() > 2)
            {
                const auto & col = block.getByPosition(arguments[3]).column;
                if (col)
                    null_map = &assert_cast<const ColumnUInt8 &>(*col).getData();
            }

            const auto size = item_arg->size();
            auto col_res = ResultColumnType::create(size);

            auto & data = col_res->getData();

            for (size_t row = 0; row < size; ++row)
            {
                const auto & value = (*item_arg)[row];

                data[row] = 0;
                for (size_t i = 0, arr_size = arr.size(); i < arr_size; ++i)
                {
                    bool hit = false;

                    if (arr[i].isNull())
                    {
                        if (null_map && (*null_map)[row])
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

            block.getByPosition(result).column = std::move(col_res);
        }

        return true;
    }

    bool executeGeneric(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());

        if (!col_array)
            return false;

        const IColumn & col_nested = col_array->getData();
        const IColumn & item_arg = *block.getByPosition(arguments[1]).column;

        auto col_res = ResultColumnType::create();

        /// Null maps of the 1st and second function arguments,
        /// if it applies.
        const PaddedPODArray<UInt8> * null_map_data = nullptr;
        const PaddedPODArray<UInt8> * null_map_item = nullptr;

        if (arguments.size() > 2)
        {
            const auto & null_map1 = block.getByPosition(arguments[2]).column;
            if (null_map1)
                null_map_data = &assert_cast<const ColumnUInt8 &>(*null_map1).getData();

            const auto & null_map2 = block.getByPosition(arguments[3]).column;
            if (null_map2)
                null_map_item = &assert_cast<const ColumnUInt8 &>(*null_map2).getData();
        }

        if (item_arg.onlyNull())
            ArrayIndexGenericNullImpl<IndexConv>::vector(col_nested, col_array->getOffsets(),
                col_res->getData(), null_map_data);
        else if (isColumnConst(item_arg))
            ArrayIndexGenericImpl<IndexConv, true>::vector(col_nested, col_array->getOffsets(),
                assert_cast<const ColumnConst &>(item_arg).getDataColumn(), col_res->getData(),    /// TODO This is wrong.
                null_map_data, nullptr);
        else
        {
            ArrayIndexGenericImpl<IndexConv, false>::vector(
                col_nested, col_array->getOffsets(), *item_arg.convertToFullColumnIfConst(), col_res->getData(),
                null_map_data, null_map_item);
        }

        block.getByPosition(result).column = std::move(col_res);
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

        if (!arguments[1]->onlyNull())
        {
            if (!allowArrayIndex(array_type->getNestedType(), arguments[1]))
                throw Exception("Types of array and 2nd argument of function "
                    + getName() + " must be identical up to nullability or numeric types or Enum and numeric type. Passed: "
                    + arguments[0]->getName() + " and " + arguments[1]->getName() + ".",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeNumber<typename IndexConv::ResultType>>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
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

        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());

        const ColumnNullable * nullable = nullptr;
        if (col_array)
            nullable = checkAndGetColumn<ColumnNullable>(col_array->getData());

        auto & arg_column = block.getByPosition(arguments[1]).column;

        const ColumnNullable * arg_nullable = nullptr;
        arg_nullable = checkAndGetColumn<ColumnNullable>(*arg_column);

        if (!nullable && !arg_nullable)
        {
            /// Simple case: no nullable values passeded.
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

            if (nullable)
            {
                const auto & nested_col = nullable->getNestedColumnPtr();

                auto & data = source_block.getByPosition(0);
                data.column = ColumnArray::create(nested_col, col_array->getOffsetsPtr());
                data.type = std::make_shared<DataTypeArray>(
                    static_cast<const DataTypeNullable &>(
                        *static_cast<const DataTypeArray &>(*block.getByPosition(arguments[0]).type).getNestedType()).getNestedType());

                auto & null_map = source_block.getByPosition(2);
                null_map.column = nullable->getNullMapColumnPtr();
                null_map.type = std::make_shared<DataTypeUInt8>();
            }
            else
            {
                auto & data = source_block.getByPosition(0);
                data = block.getByPosition(arguments[0]);
            }

            if (arg_nullable)
            {
                auto & arg = source_block.getByPosition(1);
                arg.column = arg_nullable->getNestedColumnPtr();
                arg.type = static_cast<const DataTypeNullable &>(*block.getByPosition(arguments[1]).type).getNestedType();

                auto & null_map = source_block.getByPosition(3);
                null_map.column = arg_nullable->getNullMapColumnPtr();
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
    void perform(Block & block, const ColumnNumbers & arguments, size_t result) const
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
            throw Exception{"Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
    }
};

}
