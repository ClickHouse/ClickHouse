#include <optional>
#include <type_traits>
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
#include "Columns/ColumnLowCardinality.h"
#include "DataTypes/DataTypeLowCardinality.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/**
 * ConcreteActions -- what to do when the index was found.
 */

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

/**
 * Impls -- how to perform the search depending on the arguments data types.
 * They usually provide methods @e vector (for processing ordinary columns), @e vector_const (for processing
 * ConstColumn s), and @e vectorVector for processing vectors of vectors.
 */

template <class Initial, class Result, class ConcreteAction, bool ResizeRes = true>
struct ArrayIndexNumImpl
{
private:

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

    /// compares `lhs` against `i`-th element of `rhs`
    static bool compare(const Initial & lhs, const PaddedPODArray<Result> & rhs, const size_t i) { return lhs == rhs[i]; }

    /// compares `lhs against `rhs`, third argument unused
    static bool compare(const Initial & lhs, const Result & rhs, size_t) { return lhs == rhs; }

    static Initial extract(const PaddedPODArray<Initial> & a, size_t i) { return a[i]; }

    static Initial extract(const IColumn & a, size_t i)
    {
        if constexpr (std::is_same_v<Initial, Float32>)
            return a.getFloat32(i);
        if constexpr (std::is_same_v<Initial, Float64>)
            return a.getFloat64(i);
        if constexpr (is_signed_v<Initial>)
            return a.getInt(i);
        return a.getUInt(i);
    }

#pragma GCC diagnostic pop

    static bool hasNull(const PaddedPODArray<UInt8> & null_map, size_t i)
    {
        return null_map[i];
    }

    /// Both function arguments are ordinary.
    template <class Data, class ScalarOrVector>
    static void vectorCase1(
        const Data & data,
        const ColumnArray::Offsets & offsets,
        const ScalarOrVector & value,
        PaddedPODArray<typename ConcreteAction::ResultType> & result)
    {
        size_t size = offsets.size();

        if constexpr (ResizeRes)
            result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename ConcreteAction::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if (compare(extract(data, current_offset + j), value, i))
                {
                    if (!ConcreteAction::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }

    /// The 2nd function argument is nullable.
    template <class Data, class ScalarOrVector>
    static void vectorCase2(
        const Data & data,
        const ColumnArray::Offsets & offsets,
        const ScalarOrVector & value,
        PaddedPODArray<typename ConcreteAction::ResultType> & result,
        const PaddedPODArray<UInt8> & null_map_item)
    {
        size_t size = offsets.size();

        if constexpr (ResizeRes)
            result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename ConcreteAction::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if (!hasNull(null_map_item, i) && compare(extract(data, current_offset + j), value, i))
                {
                    if (!ConcreteAction::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }

    /// The 1st function argument is a non-constant array of nullable values.
    template <class Data, class ScalarOrVector>
    static void vectorCase3(
        const Data & data,
        const ColumnArray::Offsets & offsets,
        const ScalarOrVector & value,
        PaddedPODArray<typename ConcreteAction::ResultType> & result,
        const PaddedPODArray<UInt8> & null_map_data)
    {
        size_t size = offsets.size();

        if constexpr (ResizeRes)
            result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename ConcreteAction::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if (null_map_data[current_offset + j])
                {
                }
                else if (compare(extract(data, current_offset + j), value, i))
                {
                    if (!ConcreteAction::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }

    /// The 1st function argument is a non-constant array of nullable values.
    /// The 2nd function argument is nullable.
    template <class Data, class ScalarOrVector>
    static void vectorCase4(
        const Data & data,
        const ColumnArray::Offsets & offsets,
        const ScalarOrVector & value,
        PaddedPODArray<typename ConcreteAction::ResultType> & result,
        const PaddedPODArray<UInt8> & null_map_data,
        const PaddedPODArray<UInt8> & null_map_item)
    {
        size_t size = offsets.size();

        if constexpr (ResizeRes)
            result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename ConcreteAction::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                bool hit = false;
                if (null_map_data[current_offset + j])
                {
                    if (hasNull(null_map_item, i))
                        hit = true;
                }
                else if (compare(extract(data, current_offset + j), value, i))
                    hit = true;

                if (hit)
                {
                    if (!ConcreteAction::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }

public:
    template <class Data, class ScalarOrVector>
    static void vector(
        const Data & data,
        const ColumnArray::Offsets & offsets,
        const ScalarOrVector & value,
        PaddedPODArray<typename ConcreteAction::ResultType> & result,
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
template <class ConcreteAction, bool ResizeRes = true>
struct ArrayIndexNumNullImpl
{
    static void vector(
        const ColumnArray::Offsets & offsets,
        PaddedPODArray<typename ConcreteAction::ResultType> & result,
        const PaddedPODArray<UInt8> * null_map_data)
    {
        size_t size = offsets.size();

        if constexpr (ResizeRes)
            result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename ConcreteAction::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
                if (null_map_data && (*null_map_data)[current_offset + j] & !ConcreteAction::apply(j, current))
                    break;

            result[i] = current;
            current_offset = offsets[i];
        }
    }
};

/// Implementation for arrays of strings when the 2nd function argument is a NULL value.
template <class ConcreteAction>
struct ArrayIndexStringNullImpl
{
    static void vector_const(
        const ColumnString::Chars & /*data*/,
        const ColumnArray::Offsets & offsets,
        const ColumnString::Offsets & /*string_offsets*/,
        PaddedPODArray<typename ConcreteAction::ResultType> & result,
        const PaddedPODArray<UInt8> * null_map_data)
    {
        const auto size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            const auto array_size = offsets[i] - current_offset;
            typename ConcreteAction::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if (null_map_data && (*null_map_data)[current_offset + j])
                {
                    if (!ConcreteAction::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }
};

template <class ConcreteAction>
struct ArrayIndexStringImpl
{
    static void vector_const(
        const ColumnString::Chars & data,
        const ColumnArray::Offsets & offsets,
        const ColumnString::Offsets & string_offsets,
        const ColumnString::Chars & value,
        ColumnString::Offset value_size,
        PaddedPODArray<typename ConcreteAction::ResultType> & result,
        const PaddedPODArray<UInt8> * null_map_data)
    {
        const auto size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            const auto array_size = offsets[i] - current_offset;
            typename ConcreteAction::ResultType current = 0;

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
                    if (!ConcreteAction::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }

    static void vectorVector(
        const ColumnString::Chars & data,
        const ColumnArray::Offsets & offsets,
        const ColumnString::Offsets & string_offsets,
        const ColumnString::Chars & item_values,
        const ColumnString::Offsets & item_offsets,
        PaddedPODArray<typename ConcreteAction::ResultType> & result,
        const PaddedPODArray<UInt8> * null_map_data,
        const PaddedPODArray<UInt8> * null_map_item)
    {
        const auto size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            const auto array_size = offsets[i] - current_offset;
            typename ConcreteAction::ResultType current = 0;
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
                    if (!ConcreteAction::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }
};

/// Catch-all implementation for arrays of arbitrary type.
/// To compare with a constant value, create a non-constant column with a single element and pass 
/// #is_value_has_single_element_to_compare = true.
template <class ConcreteAction, bool is_value_has_single_element_to_compare>
struct ArrayIndexGenericImpl
{
private:
    /// Both function arguments are ordinary.
    static void vectorCase1(
        const IColumn & data,
        const ColumnArray::Offsets & offsets,
        const IColumn & value,
        PaddedPODArray<typename ConcreteAction::ResultType> & result)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename ConcreteAction::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if (0 == data.compareAt(current_offset + j, is_value_has_single_element_to_compare ? 0 : i, value, 1))
                {
                    if (!ConcreteAction::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }

    /// The 2nd function argument is nullable.
    static void vectorCase2(
        const IColumn & data,
        const ColumnArray::Offsets & offsets,
        const IColumn & value,
        PaddedPODArray<typename ConcreteAction::ResultType> & result,
        const PaddedPODArray<UInt8> & null_map_item)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename ConcreteAction::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if ((null_map_item[i] == 0) &&
                    (0 == data.compareAt(current_offset + j, is_value_has_single_element_to_compare ? 0 : i, value, 1)))
                {
                    if (!ConcreteAction::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }

    /// The 1st function argument is a non-constant array of nullable values.
    static void vectorCase3(
        const IColumn & data,
        const ColumnArray::Offsets & offsets,
        const IColumn & value,
        PaddedPODArray<typename ConcreteAction::ResultType> & result,
        const PaddedPODArray<UInt8> & null_map_data)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename ConcreteAction::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if (null_map_data[current_offset + j])
                {
                }
                else if (0 == data.compareAt(current_offset + j, is_value_has_single_element_to_compare ? 0 : i, value, 1))
                {
                    if (!ConcreteAction::apply(j, current))
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
        const IColumn & data,
        const ColumnArray::Offsets & offsets,
        const IColumn & value,
        PaddedPODArray<typename ConcreteAction::ResultType> & result,
        const PaddedPODArray<UInt8> & null_map_data,
        const PaddedPODArray<UInt8> & null_map_item)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            typename ConcreteAction::ResultType current = 0;

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
                    if (!ConcreteAction::apply(j, current))
                        break;
                }
            }
        }
    }

public:
    static void vector(
        const IColumn & data,
        const ColumnArray::Offsets & offsets,
        const IColumn & value,
        PaddedPODArray<typename ConcreteAction::ResultType> & result,
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

/**
 * Check types extracted from Nullable() and LowCardinality()
 */
inline bool allowArguments(const DataTypePtr & array_inner_type, const DataTypePtr & arg)
{
    /**
     * Possible cases for #arg and #array_inner_type:
     * 1. T
     * 2. LC(T)
     * 3. N(T)
     * 4. N(LC(T)) -- quite strange, rarely found but possible.
     * 5. LC(N(T))
     *
     * All other variants are considered wrong (Like N(N(N(T)))) or LC(N(LC(T))).
     * recursiveRemoveLowCardinality works only if the given type is LC(V).
     */
    DataTypePtr array_extracted =
        removeNullable(                    /// remove outer Nullable, cases 3 and 4
            recursiveRemoveLowCardinality( /// remove LC, cases 2 and 4
                removeNullable(            /// remove inner Nullable, cases 3 and 5
                    array_inner_type)));

    DataTypePtr arg_extracted =
        removeNullable(
            recursiveRemoveLowCardinality(
                removeNullable(
                    arg)));

    return ((isNativeNumber(array_extracted) || isEnum(array_extracted)) && isNativeNumber(arg_extracted))
        || array_extracted->equals(*arg_extracted);
}

template <class ConcreteAction, class Name>
class FunctionArrayIndex : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionArrayIndex>(); }

private:
    using ResultType = typename ConcreteAction::ResultType;
    using ResultColumnType = ColumnVector<ResultType>;

    /**
     * The Array's internal data type may be quite tricky (containing a Nullable type somewhere). To process the 
     * Nullable types correctly, for each data type specialisation we provide two null maps (one for the data and one 
     * for the items). By convention they are passed as the third and the fourth argument, respectively 
     * (counting from 1).
     *
     * @return {nullptr, nullptr} if there are less then 3 arguments.
     * @return {null_map_data, nullptr} if there are three arguments
     * @return {nullptr, null_map_item} if there are four arguments but the third is missing.
     * @return {null_map_data, null_map_item} if there are four arguments.
     */
    std::pair<
        const PaddedPODArray<UInt8> *,
        const PaddedPODArray<UInt8> *>
        nullMapsBuilder(const Block& block, const ColumnNumbers & arguments) const noexcept
    {
        if (arguments.size() < 3)
            return {nullptr, nullptr};

        const PaddedPODArray<UInt8> * null_map_data = nullptr;
        const PaddedPODArray<UInt8> * null_map_item = nullptr;

        if (const auto & data_map = block.getByPosition(arguments[2]).column; data_map)
            null_map_data = &assert_cast<const ColumnUInt8 &>(*data_map).getData();

        if (const auto & item_map = block.getByPosition(arguments[3]).column; item_map)
            null_map_item = &assert_cast<const ColumnUInt8 &>(*item_map).getData();

        return {null_map_data, null_map_item};
    }

    /**
     * Given a variadic pack #Integral, apply executeIntegralExpanded with such parameters:
     * Integral s = {s1, s2, ...}
     * (s1, s1, s2, ...), (s2, s1, s2, ...), (s3, s1, s2, ...)
     */
    template <class ...Integral>
    bool executeIntegral(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        return (executeIntegralExpanded<Integral, Integral...>(block, arguments, result) || ...);
    }

    /// Invoke executeIntegralImpl with such parameters: (A, other1), (A, other2), ...
    template <class A, class ...Other>
    bool executeIntegralExpanded(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        return (executeIntegralImpl<A, Other>(block, arguments, result) || ...);
    }

    /**
     * The internal data type of the first argument (target array), if it's integral, like UInt8, may differ from the 
     * second argument, namely, the @e value, so it's possible to invoke the <tt>has(Array(Int8), UInt64)</tt> e.g. 
     * so we have to check all possible variants for #Initial and #Resulting types.
     */
    template <typename Initial, typename Resulting>
    bool executeIntegralImpl(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(
                block.getByPosition(arguments[0]).column.get());

        if (!col_array)
            return false;

        const ColumnVector<Initial> * col_nested = checkAndGetColumn<ColumnVector<Initial>>(&col_array->getData());

        if (!col_nested)
            return false;

        auto col_res = ResultColumnType::create();

        const auto [null_map_data, null_map_item] = nullMapsBuilder(block, arguments);
        const IColumn* item_arg = block.getByPosition(arguments[1]).column.get();

        if (item_arg->onlyNull())
            ArrayIndexNumNullImpl<ConcreteAction>::vector(
                col_array->getOffsets(),
                col_res->getData(),
                null_map_data);
        else if (const auto item_arg_const = checkAndGetColumnConst<ColumnVector<Resulting>>(item_arg))
            ArrayIndexNumImpl<Initial, Resulting, ConcreteAction>::vector(
                col_nested->getData(),
                col_array->getOffsets(),
                item_arg_const->template getValue<Resulting>(),
                col_res->getData(),
                null_map_data,
                nullptr);
        else if (const auto item_arg_vector = checkAndGetColumn<ColumnVector<Resulting>>(item_arg))
            ArrayIndexNumImpl<Initial, Resulting, ConcreteAction>::vector(
                col_nested->getData(),
                col_array->getOffsets(),
                item_arg_vector->getData(),
                col_res->getData(),
                null_map_data,
                null_map_item);
        else
            return false;

        block.getByPosition(result).column = std::move(col_res);
        return true;
    }

    /**
     * 1. Obtain the right-side argument column @e C. If @e C is a non-const column (thus the argument is not constant,
     * loop through all @e C's values).
     * 2. Obtain the value's index.
     * 3. Invoke the ArrayIndexNumImpl to find the desired value
     * 4. Fill the desired values in the resulting column
     */
    bool executeLowCardinality(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(
                block.getByPosition(arguments[0]).column.get());

        if (col_array)
            return false;

        const ColumnLowCardinality * col_lc =
            checkAndGetColumn<ColumnLowCardinality>(&col_array->getData());

        if (!col_lc)
            return false;

        auto col_res = ResultColumnType::create();
        col_res->getData().resize_fill(col_array->getOffsets().size()); /// fill with default values

        const auto [null_map_data, null_map_item] = nullMapsBuilder(block, arguments);
        const IColumn * col_arg = block.getByPosition(arguments[1]).column.get();

        const size_t size = isColumnConst(*col_arg)
            ? 1 /// We have a column with just one value. Arbitrary n is allowed (as the column is const, so take 0).
            : col_arg->size();

        for (size_t i = 0; i < size; ++i)
        {
            if (col_arg->onlyNull())
            {
                ArrayIndexNumNullImpl<
                    ConcreteAction,
                    /* already resized*/ false>::vector(
                    col_array->getOffsets(),
                    col_res->getData(),
                    null_map_data);

                continue;
            }

            const StringRef elem = col_arg->getDataAt(i);
            const std::optional<UInt64> value_index = col_lc->getDictionary().getOrFindIndex(elem);

            if (!value_index)
                continue; /// position already zeroed out

            ArrayIndexNumImpl<
                /* Initial data type -- DB::ReverseIndex index */ UInt64,
                /* Resulting data type -- same */ UInt64,
                ConcreteAction,
                /* Resize col_res -- already resized */ false>::vector(
                    /* data -- indices column */ col_lc->getIndexes(),
                    col_array->getOffsets(),
                    /* target value */ *value_index,
                    col_res->getData(),
                    null_map_data,
                    null_map_item);
        }

        block.getByPosition(result).column = std::move(col_res);
        return true;
    }

    bool executeString(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(
                block.getByPosition(arguments[0]).column.get());

        if (!col_array)
            return false;

        const ColumnString * col_nested = checkAndGetColumn<ColumnString>(&col_array->getData());

        if (!col_nested)
            return false;

        auto col_res = ResultColumnType::create();

        const auto [null_map_data, null_map_item] = nullMapsBuilder(block, arguments);
        const IColumn * item_arg = block.getByPosition(arguments[1]).column.get();

        if (item_arg->onlyNull())
        {
            ArrayIndexStringNullImpl<ConcreteAction>::vector_const(
                col_nested->getChars(),
                col_array->getOffsets(),
                col_nested->getOffsets(),
                col_res->getData(),
                null_map_data);
        }
        else if (const auto item_arg_const = checkAndGetColumnConstStringOrFixedString(item_arg))
        {
            const ColumnString * item_const_string =
                checkAndGetColumn<ColumnString>(&item_arg_const->getDataColumn());

            const ColumnFixedString * item_const_fixedstring =
                checkAndGetColumn<ColumnFixedString>(&item_arg_const->getDataColumn());

            if (item_const_string)
                ArrayIndexStringImpl<ConcreteAction>::vector_const(
                    col_nested->getChars(),
                    col_array->getOffsets(),
                    col_nested->getOffsets(),
                    item_const_string->getChars(),
                    item_const_string->getDataAt(0).size,
                    col_res->getData(),
                    null_map_data);
            else if (item_const_fixedstring)
                ArrayIndexStringImpl<ConcreteAction>::vector_const(
                    col_nested->getChars(),
                    col_array->getOffsets(),
                    col_nested->getOffsets(),
                    item_const_fixedstring->getChars(),
                    item_const_fixedstring->getN(),
                    col_res->getData(),
                    null_map_data);
            else
                throw Exception(
                    "Logical error: ColumnConst contains not String nor FixedString column",
                        ErrorCodes::ILLEGAL_COLUMN);
        }
        else if (const auto item_arg_vector = checkAndGetColumn<ColumnString>(item_arg))
        {
            ArrayIndexStringImpl<ConcreteAction>::vectorVector(
                col_nested->getChars(),
                col_array->getOffsets(),
                col_nested->getOffsets(),
                item_arg_vector->getChars(),
                item_arg_vector->getOffsets(),
                col_res->getData(),
                null_map_data,
                null_map_item);
        }
        else
            return false;

        block.getByPosition(result).column = std::move(col_res);
        return true;
    }

    bool executeConst(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        const ColumnConst * col_array = checkAndGetColumnConst<ColumnArray>(
                block.getByPosition(arguments[0]).column.get());

        if (!col_array)
            return false;

        Array arr = col_array->getValue<Array>();

        const IColumn * item_arg = block.getByPosition(arguments[1]).column.get();

        if (isColumnConst(*item_arg))
        {
            ResultType current = 0;
            const auto & value = (*item_arg)[0];

            for (size_t i = 0, size = arr.size(); i < size; ++i)
            {
                if (applyVisitor(FieldVisitorAccurateEquals(), arr[i], value))
                {
                    if (!ConcreteAction::apply(i, current))
                        break;
                }
            }

            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(
                item_arg->size(), static_cast<ResultType>(current));
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
                        if (!ConcreteAction::apply(i, data[row]))
                            break;
                    }
                }
            }

            block.getByPosition(result).column = std::move(col_res);
        }

        return true;
    }

    bool executeGeneric(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        const ColumnArray * col = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());

        if (!col)
            return false;

        const IColumn & col_nested = col->getData();
        const IColumn & item_arg = *block.getByPosition(arguments[1]).column;

        auto col_res = ResultColumnType::create();

        auto [null_map_data, null_map_item] = nullMapsBuilder(block, arguments);

        if (item_arg.onlyNull())
            ArrayIndexGenericNullImpl<ConcreteAction>::vector(
                col_nested,
                col->getOffsets(),
                col_res->getData(),
                null_map_data);
        else if (isColumnConst(item_arg))
            ArrayIndexGenericImpl<ConcreteAction, true>::vector(
                col_nested,
                col->getOffsets(),
                assert_cast<const ColumnConst &>(item_arg).getDataColumn(),
                col_res->getData(), /// TODO This is wrong.
                null_map_data,
                nullptr);
        else
            ArrayIndexGenericImpl<ConcreteAction, false>::vector(
                col_nested,
                col->getOffsets(),
                *item_arg.convertToFullColumnIfConst(),
                col_res->getData(),
                null_map_data,
                null_map_item);

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
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!array_type)
            throw Exception("First argument for function " + getName() + " must be an array.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->onlyNull() && !allowArguments(array_type->getNestedType(), arguments[1]))
            throw Exception("Types of array and 2nd argument of function \""
                + getName() + "\" must be identical up to nullability or cardinality or "
                "numeric types or Enum and numeric type. Passed: "
                + arguments[0]->getName() + " and " + arguments[1]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<typename ConcreteAction::ResultType>>();
    }

    /**
      * If one or both arguments passed to this function are nullable,
      * we create a new block that contains non-nullable arguments:
      *
      * - if the 1st argument is a non-constant array of nullable values,
      * it is turned into a non-constant array of ordinary values + a null
      * byte map;
      * - if the 2nd argument is a nullable value, it is turned into an
      * ordinary value + a null byte map.
      *
      * Note that since constant arrays have quite a specific structure
      * (they are vectors of Fields, which may represent the NULL value),
      * they do not require any preprocessing.
      */
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        ColumnPtr& ptr = block.getByPosition(arguments[0]).column;

        /**
         * The columns here have two general cases, either being Array(T) or Const(Array(T)).
         * The last type will return nullptr after casting to ColumnArray, so we leave the casting
         * to execute* functions.
         */
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(ptr.get());
        const ColumnNullable * nullable = nullptr;

        if (col_array)
            nullable = checkAndGetColumn<ColumnNullable>(col_array->getData());

        auto & arg_column = block.getByPosition(arguments[1]).column;
        const ColumnNullable * arg_nullable = checkAndGetColumn<ColumnNullable>(*arg_column);

        if (!nullable && !arg_nullable)
            executeOnNonNullable(block, arguments, result);
        else
        {
            /**
             * To correctly process the Nullable values (either #col_array, #arg_column or both) we create a new block 
             * and operate on it. The block structure follows:
             * {0, 1, 2, 3, 4}
             * {data (array) argument, "value" argument, data null map, "value" null map, function result}.
             */
            Block source_block = { {}, {}, {}, {}, {nullptr, block.getByPosition(result).type, ""} };

            if (nullable)
            {
                const auto & nested_col = nullable->getNestedColumnPtr();

                auto & data = source_block.getByPosition(0);

                data.column = ColumnArray::create(nested_col, col_array->getOffsetsPtr());
                data.type = std::make_shared<DataTypeArray>(
                    static_cast<const DataTypeNullable &>(
                        *static_cast<const DataTypeArray &>(
                            *block.getByPosition(arguments[0]).type
                        ).getNestedType()
                    ).getNestedType());

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
                arg.type =
                    static_cast<const DataTypeNullable &>(
                        *block.getByPosition(arguments[1]).type
                    ).getNestedType();

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
            executeOnNonNullable(source_block, {0, 1, 2, 3}, 4);

            /// Move the result to its final position.
            const ColumnWithTypeAndName & source_col = source_block.getByPosition(4);
            ColumnWithTypeAndName & dest_col = block.getByPosition(result);
            dest_col.column = std::move(source_col.column);
        }
    }

private:
    void executeOnNonNullable(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (!(
            executeIntegral<
                UInt8, UInt16, UInt32, UInt64,
                Int8, Int16, Int32, Int64,
                Float32, Float64>(block, arguments, result)
            || executeConst(block, arguments, result) // special case
            || executeString(block, arguments, result)
            || executeLowCardinality(block, arguments, result)
            || executeGeneric(block, arguments, result)))
            throw Exception(
                "Illegal internal type of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};
}

