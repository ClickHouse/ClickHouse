#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnMap.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ZERO_ARRAY_OR_TUPLE_INDEX;
}

namespace ArrayImpl
{
    class NullMapBuilder;
}

/** arrayElement(arr, i) - get the array element by index. If index is not constant and out of range - return default value of data type.
  * The index begins with 1. Also, the index can be negative - then it is counted from the end of the array.
  */
class FunctionArrayElement : public IFunction
{
public:
    static constexpr auto name = "arrayElement";
    static FunctionPtr create(const Context & context);

    String getName() const override;

    bool useDefaultImplementationForConstants() const override { return true; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    void executeImpl(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override;

private:
    void perform(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result,
                 ArrayImpl::NullMapBuilder & builder, size_t input_rows_count) const;

    template <typename DataType>
    static bool executeNumberConst(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, const Field & index,
                                   ArrayImpl::NullMapBuilder & builder);

    template <typename IndexType, typename DataType>
    static bool executeNumber(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, const PaddedPODArray<IndexType> & indices,
                              ArrayImpl::NullMapBuilder & builder);

    static bool executeStringConst(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, const Field & index,
                                   ArrayImpl::NullMapBuilder & builder);

    template <typename IndexType>
    static bool executeString(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, const PaddedPODArray<IndexType> & indices,
                              ArrayImpl::NullMapBuilder & builder);

    static bool executeGenericConst(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, const Field & index,
                                    ArrayImpl::NullMapBuilder & builder);

    template <typename IndexType>
    static bool executeGeneric(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, const PaddedPODArray<IndexType> & indices,
                               ArrayImpl::NullMapBuilder & builder);

    template <typename IndexType>
    static bool executeConst(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result,
                             const PaddedPODArray <IndexType> & indices, ArrayImpl::NullMapBuilder & builder,
                             size_t input_rows_count);

    template <typename IndexType>
    bool executeArgument(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result,
                         ArrayImpl::NullMapBuilder & builder, size_t input_rows_count) const;

    /** For a tuple array, the function is evaluated component-wise for each element of the tuple.
      */
    bool executeTuple(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const;

    /** For a Map, the function is to find the matched key's value
     */
    static bool executeMappedKeyString(const ColumnArray * column, Field & index, std::vector<int> &matched_idxs);

    template <typename DataType>
    static bool executeMappedKeyNumber(const ColumnArray * column, Field & index, std::vector<int> &matched_idxs);

    static bool getMappedKey(const ColumnArray * col_keys_untyped, Field & index, std::vector<int> &matched_idxs);

    template <typename DataType>
    static bool executeMappedValueNumber(const ColumnArray * column, std::vector<int> matched_idxs,
        IColumn * col_res_untyped);

    static bool executeMappedValueString(const ColumnArray * column, std::vector<int> matched_idxs,
        IColumn * col_res_untyped);

    static bool executeMappedValueArray(const ColumnArray * column, std::vector<int> matched_idxs,
        IColumn * col_res_untyped);

    static bool getMappedValue(const ColumnArray * col_values_untyped,
        std::vector<int> matched_idxs, /*const DB::TypeIndex value_type, */IColumn * col_res_untyped);

    static bool executeMap(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t input_rows_count);
};


namespace ArrayImpl
{

class NullMapBuilder
{
public:
    explicit operator bool() const { return src_null_map; }
    bool operator!() const { return !src_null_map; }

    void initSource(const UInt8 * src_null_map_)
    {
        src_null_map = src_null_map_;
    }

    void initSink(size_t size)
    {
        auto sink = ColumnUInt8::create(size);
        sink_null_map = sink->getData().data();
        sink_null_map_holder = std::move(sink);
    }

    void update(size_t from)
    {
        sink_null_map[index] = bool(src_null_map && src_null_map[from]);
        ++index;
    }

    void update()
    {
        sink_null_map[index] = bool(src_null_map);
        ++index;
    }

    ColumnPtr getNullMapColumnPtr() && { return std::move(sink_null_map_holder); }

private:
    const UInt8 * src_null_map = nullptr;
    UInt8 * sink_null_map = nullptr;
    MutableColumnPtr sink_null_map_holder;
    size_t index = 0;
};

}

namespace
{

template <typename T>
struct ArrayElementNumImpl
{
    /** Implementation for constant index.
      * If negative = false - index is from beginning of array, started from 0.
      * If negative = true - index is from end of array, started from 0.
      */
    template <bool negative>
    static void vectorConst(
        const PaddedPODArray<T> & data, const ColumnArray::Offsets & offsets,
        const ColumnArray::Offset index,
        PaddedPODArray<T> & result, ArrayImpl::NullMapBuilder & builder)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;

            if (index < array_size)
            {
                size_t j = !negative ? (current_offset + index) : (offsets[i] - index - 1);
                result[i] = data[j];
                if (builder)
                    builder.update(j);
            }
            else
            {
                result[i] = T();

                if (builder)
                    builder.update();
            }

            current_offset = offsets[i];
        }
    }

    /** Implementation for non-constant index.
      */
    template <typename TIndex>
    static void vector(
        const PaddedPODArray<T> & data, const ColumnArray::Offsets & offsets,
        const PaddedPODArray<TIndex> & indices,
        PaddedPODArray<T> & result, ArrayImpl::NullMapBuilder & builder)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;

            TIndex index = indices[i];
            if (index > 0 && static_cast<size_t>(index) <= array_size)
            {
                size_t j = current_offset + index - 1;
                result[i] = data[j];

                if (builder)
                    builder.update(j);
            }
            else if (index < 0 && static_cast<size_t>(-index) <= array_size)
            {
                size_t j = offsets[i] + index;
                result[i] = data[j];

                if (builder)
                    builder.update(j);
            }
            else
            {
                result[i] = T();

                if (builder)
                    builder.update();
            }

            current_offset = offsets[i];
        }
    }
};

struct ArrayElementStringImpl
{
    template <bool negative>
    static void vectorConst(
        const ColumnString::Chars & data, const ColumnArray::Offsets & offsets, const ColumnString::Offsets & string_offsets,
        const ColumnArray::Offset index,
        ColumnString::Chars & result_data, ColumnArray::Offsets & result_offsets,
        ArrayImpl::NullMapBuilder & builder)
    {
        size_t size = offsets.size();
        result_offsets.resize(size);
        result_data.reserve(data.size());

        ColumnArray::Offset current_offset = 0;
        ColumnArray::Offset current_result_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;

            if (index < array_size)
            {
                size_t adjusted_index = !negative ? index : (array_size - index - 1);

                size_t j = current_offset + adjusted_index;
                if (builder)
                    builder.update(j);

                ColumnArray::Offset string_pos = current_offset == 0 && adjusted_index == 0
                    ? 0
                    : string_offsets[current_offset + adjusted_index - 1];

                ColumnArray::Offset string_size = string_offsets[current_offset + adjusted_index] - string_pos;

                result_data.resize(current_result_offset + string_size);
                memcpySmallAllowReadWriteOverflow15(&result_data[current_result_offset], &data[string_pos], string_size);
                current_result_offset += string_size;
                result_offsets[i] = current_result_offset;
            }
            else
            {
                /// Insert an empty row.
                result_data.resize(current_result_offset + 1);
                result_data[current_result_offset] = 0;
                current_result_offset += 1;
                result_offsets[i] = current_result_offset;

                if (builder)
                    builder.update();
            }

            current_offset = offsets[i];
        }
    }

    /** Implementation for non-constant index.
      */
    template <typename TIndex>
    static void vector(
        const ColumnString::Chars & data, const ColumnArray::Offsets & offsets, const ColumnString::Offsets & string_offsets,
        const PaddedPODArray<TIndex> & indices,
        ColumnString::Chars & result_data, ColumnArray::Offsets & result_offsets,
        ArrayImpl::NullMapBuilder & builder)
    {
        size_t size = offsets.size();
        result_offsets.resize(size);
        result_data.reserve(data.size());

        ColumnArray::Offset current_offset = 0;
        ColumnArray::Offset current_result_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            size_t adjusted_index;    /// index in array from zero

            TIndex index = indices[i];
            if (index > 0 && static_cast<size_t>(index) <= array_size)
                adjusted_index = index - 1;
            else if (index < 0 && static_cast<size_t>(-index) <= array_size)
                adjusted_index = array_size + index;
            else
                adjusted_index = array_size;    /// means no element should be taken

            if (adjusted_index < array_size)
            {
                size_t j = current_offset + adjusted_index;
                if (builder)
                    builder.update(j);

                ColumnArray::Offset string_pos = current_offset == 0 && adjusted_index == 0
                    ? 0
                    : string_offsets[current_offset + adjusted_index - 1];

                ColumnArray::Offset string_size = string_offsets[current_offset + adjusted_index] - string_pos;

                result_data.resize(current_result_offset + string_size);
                memcpySmallAllowReadWriteOverflow15(&result_data[current_result_offset], &data[string_pos], string_size);
                current_result_offset += string_size;
                result_offsets[i] = current_result_offset;
            }
            else
            {
                /// Insert empty string
                result_data.resize(current_result_offset + 1);
                result_data[current_result_offset] = 0;
                current_result_offset += 1;
                result_offsets[i] = current_result_offset;

                if (builder)
                    builder.update();
            }

            current_offset = offsets[i];
        }
    }
};

/// Generic implementation for other nested types.
struct ArrayElementGenericImpl
{
    template <bool negative>
    static void vectorConst(
        const IColumn & data, const ColumnArray::Offsets & offsets,
        const ColumnArray::Offset index,
        IColumn & result, ArrayImpl::NullMapBuilder & builder)
    {
        size_t size = offsets.size();
        result.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;

            if (index < array_size)
            {
                size_t j = !negative ? current_offset + index : offsets[i] - index - 1;
                result.insertFrom(data, j);
                if (builder)
                    builder.update(j);
            }
            else
            {
                result.insertDefault();
                if (builder)
                    builder.update();
            }

            current_offset = offsets[i];
        }
    }

    /** Implementation for non-constant index.
      */
    template <typename TIndex>
    static void vector(
        const IColumn & data, const ColumnArray::Offsets & offsets,
        const PaddedPODArray<TIndex> & indices,
        IColumn & result, ArrayImpl::NullMapBuilder & builder)
    {
        size_t size = offsets.size();
        result.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;

            TIndex index = indices[i];
            if (index > 0 && static_cast<size_t>(index) <= array_size)
            {
                size_t j = current_offset + index - 1;
                result.insertFrom(data, j);
                if (builder)
                    builder.update(j);
            }
            else if (index < 0 && static_cast<size_t>(-index) <= array_size)
            {
                size_t j = offsets[i] + index;
                result.insertFrom(data, j);
                if (builder)
                    builder.update(j);
            }
            else
            {
                result.insertDefault();
                if (builder)
                    builder.update();
            }

            current_offset = offsets[i];
        }
    }
};

}


FunctionPtr FunctionArrayElement::create(const Context &)
{
    return std::make_shared<FunctionArrayElement>();
}


template <typename DataType>
bool FunctionArrayElement::executeNumberConst(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, const Field & index,
                                              ArrayImpl::NullMapBuilder & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(columns[arguments[0]].column.get());

    if (!col_array)
        return false;

    const ColumnVector<DataType> * col_nested = checkAndGetColumn<ColumnVector<DataType>>(&col_array->getData());

    if (!col_nested)
        return false;

    auto col_res = ColumnVector<DataType>::create();

    if (index.getType() == Field::Types::UInt64)
        ArrayElementNumImpl<DataType>::template vectorConst<false>(
            col_nested->getData(), col_array->getOffsets(), safeGet<UInt64>(index) - 1, col_res->getData(), builder);
    else if (index.getType() == Field::Types::Int64)
        ArrayElementNumImpl<DataType>::template vectorConst<true>(
            col_nested->getData(), col_array->getOffsets(), -safeGet<Int64>(index) - 1, col_res->getData(), builder);
    else
        throw Exception("Illegal type of array index", ErrorCodes::LOGICAL_ERROR);

    columns[result].column = std::move(col_res);
    return true;
}

template <typename IndexType, typename DataType>
bool FunctionArrayElement::executeNumber(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, const PaddedPODArray<IndexType> & indices,
                                         ArrayImpl::NullMapBuilder & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(columns[arguments[0]].column.get());

    if (!col_array)
        return false;

    const ColumnVector<DataType> * col_nested = checkAndGetColumn<ColumnVector<DataType>>(&col_array->getData());

    if (!col_nested)
        return false;

    auto col_res = ColumnVector<DataType>::create();

    ArrayElementNumImpl<DataType>::template vector<IndexType>(
        col_nested->getData(), col_array->getOffsets(), indices, col_res->getData(), builder);

    columns[result].column = std::move(col_res);
    return true;
}

bool FunctionArrayElement::executeStringConst(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, const Field & index,
                                              ArrayImpl::NullMapBuilder & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(columns[arguments[0]].column.get());

    if (!col_array)
        return false;

    const ColumnString * col_nested = checkAndGetColumn<ColumnString>(&col_array->getData());

    if (!col_nested)
        return false;

    auto col_res = ColumnString::create();

    if (index.getType() == Field::Types::UInt64)
        ArrayElementStringImpl::vectorConst<false>(
            col_nested->getChars(),
            col_array->getOffsets(),
            col_nested->getOffsets(),
            safeGet<UInt64>(index) - 1,
            col_res->getChars(),
            col_res->getOffsets(),
            builder);
    else if (index.getType() == Field::Types::Int64)
        ArrayElementStringImpl::vectorConst<true>(
            col_nested->getChars(),
            col_array->getOffsets(),
            col_nested->getOffsets(),
            -safeGet<Int64>(index) - 1,
            col_res->getChars(),
            col_res->getOffsets(),
            builder);
    else
        throw Exception("Illegal type of array index", ErrorCodes::LOGICAL_ERROR);

    columns[result].column = std::move(col_res);
    return true;
}

template <typename IndexType>
bool FunctionArrayElement::executeString(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, const PaddedPODArray<IndexType> & indices,
                                         ArrayImpl::NullMapBuilder & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(columns[arguments[0]].column.get());

    if (!col_array)
        return false;

    const ColumnString * col_nested = checkAndGetColumn<ColumnString>(&col_array->getData());

    if (!col_nested)
        return false;

    auto col_res = ColumnString::create();

    ArrayElementStringImpl::vector<IndexType>(
        col_nested->getChars(),
        col_array->getOffsets(),
        col_nested->getOffsets(),
        indices,
        col_res->getChars(),
        col_res->getOffsets(),
        builder);

    columns[result].column = std::move(col_res);
    return true;
}

bool FunctionArrayElement::executeGenericConst(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, const Field & index,
                                               ArrayImpl::NullMapBuilder & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(columns[arguments[0]].column.get());

    if (!col_array)
        return false;

    const auto & col_nested = col_array->getData();
    auto col_res = col_nested.cloneEmpty();

    if (index.getType() == Field::Types::UInt64)
        ArrayElementGenericImpl::vectorConst<false>(
            col_nested, col_array->getOffsets(), safeGet<UInt64>(index) - 1, *col_res, builder);
    else if (index.getType() == Field::Types::Int64)
        ArrayElementGenericImpl::vectorConst<true>(
            col_nested, col_array->getOffsets(), -safeGet<Int64>(index) - 1, *col_res, builder);
    else
        throw Exception("Illegal type of array index", ErrorCodes::LOGICAL_ERROR);

    columns[result].column = std::move(col_res);
    return true;
}

template <typename IndexType>
bool FunctionArrayElement::executeGeneric(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, const PaddedPODArray<IndexType> & indices,
                                          ArrayImpl::NullMapBuilder & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(columns[arguments[0]].column.get());

    if (!col_array)
        return false;

    const auto & col_nested = col_array->getData();
    auto col_res = col_nested.cloneEmpty();

    ArrayElementGenericImpl::vector<IndexType>(
        col_nested, col_array->getOffsets(), indices, *col_res, builder);

    columns[result].column = std::move(col_res);
    return true;
}

template <typename IndexType>
bool FunctionArrayElement::executeConst(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result,
                                        const PaddedPODArray <IndexType> & indices, ArrayImpl::NullMapBuilder & builder,
                                        size_t input_rows_count)
{
    const ColumnArray * col_array = checkAndGetColumnConstData<ColumnArray>(columns[arguments[0]].column.get());

    if (!col_array)
        return false;

    auto res = columns[result].type->createColumn();

    size_t rows = input_rows_count;
    const IColumn & array_elements = col_array->getData();
    size_t array_size = array_elements.size();

    for (size_t i = 0; i < rows; ++i)
    {
        IndexType index = indices[i];
        if (index > 0 && static_cast<size_t>(index) <= array_size)
        {
            size_t j = index - 1;
            res->insertFrom(array_elements, j);
            if (builder)
                builder.update(j);
        }
        else if (index < 0 && static_cast<size_t>(-index) <= array_size)
        {
            size_t j = array_size + index;
            res->insertFrom(array_elements, j);
            if (builder)
                builder.update(j);
        }
        else
        {
            res->insertDefault();
            if (builder)
                builder.update();
        }
    }

    columns[result].column = std::move(res);
    return true;
}

template <typename IndexType>
bool FunctionArrayElement::executeArgument(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result,
                                           ArrayImpl::NullMapBuilder & builder, size_t input_rows_count) const
{
    auto index = checkAndGetColumn<ColumnVector<IndexType>>(columns[arguments[1]].column.get());

    if (!index)
        return false;

    const auto & index_data = index->getData();

    if (builder)
        builder.initSink(index_data.size());

    if (!(executeNumber<IndexType, UInt8>(columns, arguments, result, index_data, builder)
        || executeNumber<IndexType, UInt16>(columns, arguments, result, index_data, builder)
        || executeNumber<IndexType, UInt32>(columns, arguments, result, index_data, builder)
        || executeNumber<IndexType, UInt64>(columns, arguments, result, index_data, builder)
        || executeNumber<IndexType, Int8>(columns, arguments, result, index_data, builder)
        || executeNumber<IndexType, Int16>(columns, arguments, result, index_data, builder)
        || executeNumber<IndexType, Int32>(columns, arguments, result, index_data, builder)
        || executeNumber<IndexType, Int64>(columns, arguments, result, index_data, builder)
        || executeNumber<IndexType, Float32>(columns, arguments, result, index_data, builder)
        || executeNumber<IndexType, Float64>(columns, arguments, result, index_data, builder)
        || executeConst<IndexType>(columns, arguments, result, index_data, builder, input_rows_count)
        || executeString<IndexType>(columns, arguments, result, index_data, builder)
        || executeGeneric<IndexType>(columns, arguments, result, index_data, builder)))
    throw Exception("Illegal column " + columns[arguments[0]].column->getName()
                + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

    return true;
}

bool FunctionArrayElement::executeTuple(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const
{
    const ColumnArray * col_array = typeid_cast<const ColumnArray *>(columns[arguments[0]].column.get());

    if (!col_array)
        return false;

    const ColumnTuple * col_nested = typeid_cast<const ColumnTuple *>(&col_array->getData());

    if (!col_nested)
        return false;

    const auto & tuple_columns = col_nested->getColumns();
    size_t tuple_size = tuple_columns.size();

    const DataTypes & tuple_types = typeid_cast<const DataTypeTuple &>(
        *typeid_cast<const DataTypeArray &>(*columns[arguments[0]].type).getNestedType()).getElements();

    /** We will calculate the function for the tuple of the internals of the array.
      * To do this, create a temporary columns.
      * It will consist of the following columns
      * - the index of the array to be taken;
      * - an array of the first elements of the tuples;
      * - the result of taking the elements by the index for an array of the first elements of the tuples;
      * - array of the second elements of the tuples;
      * - result of taking elements by index for an array of second elements of tuples;
      * ...
      */
    ColumnsWithTypeAndName temporary_results;
    temporary_results.emplace_back(columns[arguments[1]]);

    /// results of taking elements by index for arrays from each element of the tuples;
    Columns result_tuple_columns;

    for (size_t i = 0; i < tuple_size; ++i)
    {
        ColumnWithTypeAndName array_of_tuple_section;
        array_of_tuple_section.column = ColumnArray::create(tuple_columns[i], col_array->getOffsetsPtr());
        array_of_tuple_section.type = std::make_shared<DataTypeArray>(tuple_types[i]);
        temporary_results.emplace_back(array_of_tuple_section);

        ColumnWithTypeAndName array_elements_of_tuple_section;
        array_elements_of_tuple_section.type = getReturnTypeImpl(
            {temporary_results[i * 2 + 1].type, temporary_results[0].type});
        temporary_results.emplace_back(array_elements_of_tuple_section);

        executeImpl(temporary_results, ColumnNumbers{i * 2 + 1, 0}, i * 2 + 2, input_rows_count);

        result_tuple_columns.emplace_back(std::move(temporary_results[i * 2 + 2].column));
    }

    columns[result].column = ColumnTuple::create(result_tuple_columns);

    return true;
}

bool FunctionArrayElement::executeMappedKeyString(const ColumnArray * column, Field & index,
    std::vector<int> &matched_idxs)
{
    const ColumnString * keys = checkAndGetColumn<ColumnString>(&column->getData());
    const ColumnArray::Offsets & offsets = column->getOffsets();
    size_t rows = offsets.size();

    if (!keys)
        return false;

    String str = index.get<String>();
    for (size_t i = 0; i < rows; i++)
    {
        bool matched = false;
        size_t begin = offsets[i - 1];
        size_t end = offsets[i];
        for (size_t j = begin; j < end; j++)
        {
            if (strcmp(keys->getDataAt(j).data, str.data()) == 0)
            {
                matched_idxs.push_back(j);
                matched = true;
                break;
            }
        }
        if (!matched)
            matched_idxs.push_back(-1);
    }


    return true;
}

template <typename DataType>
bool FunctionArrayElement::executeMappedKeyNumber(const ColumnArray * column, Field & index,
    std::vector<int> &matched_idxs)
{
    const ColumnVector<DataType> * col_nested = checkAndGetColumn<ColumnVector<DataType>>(&column->getData());

    if (!col_nested)
        return false;

    DataType index_v = index.get<DataType>();

    const ColumnArray::Offsets & offsets = column->getOffsets();
    size_t rows = offsets.size();

    for (size_t i = 0; i < rows; i++)
    {
        bool matched = false;
        size_t begin = offsets[i - 1];
        size_t end = offsets[i];

        for (size_t j = begin; j < end; j++)
        {
            DataType ele = col_nested->getElement(j);

            if (!CompareHelper<DataType>::compare(ele, index_v, 0))
            {
                matched_idxs.push_back(j);
                matched = true;
                break;
            }
        }
        if (!matched)
            matched_idxs.push_back(-1);
    }

    return true;
}

bool FunctionArrayElement::getMappedKey(const ColumnArray * column, Field & index, std::vector<int> &matched_idxs)
{
    if (!(executeMappedKeyNumber<UInt8>(column, index, matched_idxs)
        || executeMappedKeyNumber<UInt16>(column, index, matched_idxs)
        || executeMappedKeyNumber<UInt32>(column, index, matched_idxs)
        || executeMappedKeyNumber<UInt64>(column, index, matched_idxs)
        || executeMappedKeyNumber<Int8>(column, index, matched_idxs)
        || executeMappedKeyNumber<Int16>(column, index, matched_idxs)
        || executeMappedKeyNumber<Int32>(column, index, matched_idxs)
        || executeMappedKeyNumber<Int64>(column, index, matched_idxs)
        || executeMappedKeyString(column, index, matched_idxs)))
        throw Exception("Illegal column" + column->getName()
            + "of first argument , type not match", ErrorCodes::ILLEGAL_COLUMN);
    return true;
}

template <typename DataType>
bool FunctionArrayElement::executeMappedValueNumber(const ColumnArray * column, std::vector<int> matched_idxs,
    IColumn * col_res_untyped)
{
    const ColumnVector<DataType> * col_nested = checkAndGetColumn<ColumnVector<DataType>>(&column->getData());

    if (!col_nested)
        return false;

    const ColumnArray::Offsets & offsets = column->getOffsets();
    size_t rows = offsets.size();

    ColumnVector<DataType> * col_res = assert_cast<ColumnVector<DataType> *>(col_res_untyped);

    // DataType mapped_value;
    for (size_t i = 0; i < rows; i++)
    {
        if (matched_idxs[i] != -1)
        {
            col_res->insertFrom(*col_nested, matched_idxs[i]);
        }
        else
        {
            // Default value for unmatched keys
            DataType default_value = -1;
            col_res->insertValue(default_value);
        }
    }

    return true;
}

bool FunctionArrayElement::executeMappedValueString(const ColumnArray * column, std::vector<int> matched_idxs,
    IColumn * col_res_untyped)
{
    const ColumnString * col_nested = checkAndGetColumn<ColumnString>(&column->getData());

    if (!col_nested)
        return false;

    const ColumnArray::Offsets & offsets = column->getOffsets();
    size_t rows = offsets.size();

    ColumnString * col_res = assert_cast<ColumnString *>(col_res_untyped);

    StringRef res_str;
    for (size_t i = 0; i < rows; i++)
    {
        if (matched_idxs[i] != -1)
        {
            res_str = col_nested->getDataAt(matched_idxs[i]);
            col_res->insertData(res_str.data, res_str.size);
        }
        else
        {
            // Default value for unmatched keys
            col_res->insertData("null", 4);
        }
    }
    return true;
}

bool FunctionArrayElement::executeMappedValueArray(const ColumnArray * column, std::vector<int> matched_idxs,
    IColumn * col_res_untyped)
{
    const ColumnArray * col_nested = checkAndGetColumn<ColumnArray>(&column->getData());

    if (!col_nested)
        return false;

    const ColumnArray::Offsets & offsets = column->getOffsets();
    size_t rows = offsets.size();

    ColumnArray * col_res = assert_cast<ColumnArray *>(col_res_untyped);

    StringRef res_str;
    for (size_t i = 0; i < rows; i++)
    {
        if (matched_idxs[i] != -1)
        {
            res_str = col_nested->getDataAt(matched_idxs[i]);
            col_res->insertData(res_str.data, res_str.size);
        }
        else
        {
            col_res->insertData("", 0);
        }
    }
    return true;
}

bool FunctionArrayElement::getMappedValue(const ColumnArray * column, std::vector<int> matched_idxs, IColumn * col_res_untyped)
{
     if (!(executeMappedValueNumber<UInt8>(column, matched_idxs, col_res_untyped)
        || executeMappedValueNumber<UInt16>(column, matched_idxs, col_res_untyped)
        || executeMappedValueNumber<UInt32>(column, matched_idxs, col_res_untyped)
        || executeMappedValueNumber<UInt64>(column, matched_idxs, col_res_untyped)
        || executeMappedValueNumber<Int8>(column, matched_idxs, col_res_untyped)
        || executeMappedValueNumber<Int16>(column, matched_idxs, col_res_untyped)
        || executeMappedValueNumber<Int32>(column, matched_idxs, col_res_untyped)
        || executeMappedValueNumber<Int64>(column, matched_idxs, col_res_untyped)
        || executeMappedValueString(column, matched_idxs, col_res_untyped)
        || executeMappedValueArray(column, matched_idxs, col_res_untyped)))
        throw Exception("Map(value) column " + column->getName()
            + " , type not support yet", ErrorCodes::ILLEGAL_COLUMN);

    return true;
}

bool FunctionArrayElement::executeMap(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
{
    const ColumnMap * col_map = typeid_cast<const ColumnMap *>(columns[arguments[0]].column.get());
    if (!col_map)
        return false;

    const DataTypes & kv_types = assert_cast<const DataTypeMap *>(columns[arguments[0]].type.get())->getElements();
    const DataTypePtr & key_type = (typeid_cast<const DataTypeArray *>(kv_types[0].get()))->getNestedType();
    const DataTypePtr & value_type = (typeid_cast<const DataTypeArray *>(kv_types[1].get()))->getNestedType();

    Field index = (*columns[arguments[1]].column)[0];

    // Get Matched key's value
    const ColumnArray * col_keys_untyped = typeid_cast<const ColumnArray *>(&col_map->getColumn(0));
    const ColumnArray * col_values_untyped = typeid_cast<const ColumnArray *>(&col_map->getColumn(1));
    size_t rows = col_keys_untyped->getOffsets().size();

    auto col_res_untyped = value_type->createColumn();
    if (rows > 0)
    {
        if (input_rows_count)
            assert(input_rows_count == rows);

        std::vector<int> matched_idxs;
        if (!getMappedKey(col_keys_untyped, index, matched_idxs))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "key type unmatched, we need type '{}' failed", key_type->getName());

        if (!getMappedValue(col_values_untyped, matched_idxs, col_res_untyped.get()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "value type unmatched, we need type '{}' failed", value_type->getName());
    }
    columns[result].column = std::move(col_res_untyped);

    return true;
}

String FunctionArrayElement::getName() const
{
    return name;
}

DataTypePtr FunctionArrayElement::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments[0]->getTypeId() == TypeIndex::Map)
    {
        const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].get());
        const DataTypes & kv_types = map_type->getElements();
        return typeid_cast<const DataTypeArray *>(kv_types[1].get())->getNestedType();
    }
    const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
    if (!array_type)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument for function '{}' must be array, got '{}' instead",
            getName(), arguments[0]->getName());
    }

    if (!isInteger(arguments[1]))
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Second argument for function '{}' must be integer, got '{}' instead",
            getName(), arguments[1]->getName());
    }

    return array_type->getNestedType();
}

void FunctionArrayElement::executeImpl(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const
{
    /// Check nullability.
    bool is_array_of_nullable = false;

    const ColumnArray * col_array = nullptr;
    const ColumnArray * col_const_array = nullptr;
    const ColumnMap * col_map = checkAndGetColumn<ColumnMap>(columns[arguments[0]].column.get());
    if (col_map)
    {
        executeMap(columns, arguments, result, input_rows_count);
        return;
    }

    col_array = checkAndGetColumn<ColumnArray>(columns[arguments[0]].column.get());
    if (col_array)
        is_array_of_nullable = isColumnNullable(col_array->getData());
    else
    {
        col_const_array = checkAndGetColumnConstData<ColumnArray>(columns[arguments[0]].column.get());
        if (col_const_array)
            is_array_of_nullable = isColumnNullable(col_const_array->getData());
        else
            throw Exception("Illegal column " + columns[arguments[0]].column->getName()
            + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

    if (!is_array_of_nullable)
    {
        ArrayImpl::NullMapBuilder builder;
        perform(columns, arguments, result, builder, input_rows_count);
    }
    else
    {
        /// Perform initializations.
        ArrayImpl::NullMapBuilder builder;
        ColumnsWithTypeAndName source_columns;

        const DataTypePtr & input_type = typeid_cast<const DataTypeNullable &>(
            *typeid_cast<const DataTypeArray &>(*columns[arguments[0]].type).getNestedType()).getNestedType();

        DataTypePtr tmp_ret_type = removeNullable(columns[result].type);

        if (col_array)
        {
            const auto & nullable_col = typeid_cast<const ColumnNullable &>(col_array->getData());
            const auto & nested_col = nullable_col.getNestedColumnPtr();

            /// Put nested_col inside a ColumnArray.
            source_columns =
            {
                {
                    ColumnArray::create(nested_col, col_array->getOffsetsPtr()),
                    std::make_shared<DataTypeArray>(input_type),
                    ""
                },
                columns[arguments[1]],
                {
                    nullptr,
                    tmp_ret_type,
                    ""
                }
            };

            builder.initSource(nullable_col.getNullMapData().data());
        }
        else
        {
            /// ColumnConst(ColumnArray(ColumnNullable(...)))
            const auto & nullable_col = assert_cast<const ColumnNullable &>(col_const_array->getData());
            const auto & nested_col = nullable_col.getNestedColumnPtr();

            source_columns =
            {
                {
                    ColumnConst::create(ColumnArray::create(nested_col, col_const_array->getOffsetsPtr()), input_rows_count),
                    std::make_shared<DataTypeArray>(input_type),
                    ""
                },
                columns[arguments[1]],
                {
                    nullptr,
                    tmp_ret_type,
                    ""
                }
            };

            builder.initSource(nullable_col.getNullMapData().data());
        }

        perform(source_columns, {0, 1}, 2, builder, input_rows_count);

        /// Store the result.
        const ColumnWithTypeAndName & source_col = source_columns[2];
        ColumnWithTypeAndName & dest_col = columns[result];
        dest_col.column = ColumnNullable::create(source_col.column, builder ? std::move(builder).getNullMapColumnPtr() : ColumnUInt8::create());
    }
}

void FunctionArrayElement::perform(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result,
                                   ArrayImpl::NullMapBuilder & builder, size_t input_rows_count) const
{
    if (executeTuple(columns, arguments, result, input_rows_count))
    {
    }
    else if (!isColumnConst(*columns[arguments[1]].column))
    {
        if (!(executeArgument<UInt8>(columns, arguments, result, builder, input_rows_count)
            || executeArgument<UInt16>(columns, arguments, result, builder, input_rows_count)
            || executeArgument<UInt32>(columns, arguments, result, builder, input_rows_count)
            || executeArgument<UInt64>(columns, arguments, result, builder, input_rows_count)
            || executeArgument<Int8>(columns, arguments, result, builder, input_rows_count)
            || executeArgument<Int16>(columns, arguments, result, builder, input_rows_count)
            || executeArgument<Int32>(columns, arguments, result, builder, input_rows_count)
            || executeArgument<Int64>(columns, arguments, result, builder, input_rows_count)))
        throw Exception("Second argument for function " + getName() + " must must have UInt or Int type.",
                        ErrorCodes::ILLEGAL_COLUMN);
    }
    else
    {
        Field index = (*columns[arguments[1]].column)[0];

        if (builder)
            builder.initSink(input_rows_count);

        if (index == 0u)
            throw Exception("Array indices are 1-based", ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX);

        if (!(executeNumberConst<UInt8>(columns, arguments, result, index, builder)
            || executeNumberConst<UInt16>(columns, arguments, result, index, builder)
            || executeNumberConst<UInt32>(columns, arguments, result, index, builder)
            || executeNumberConst<UInt64>(columns, arguments, result, index, builder)
            || executeNumberConst<Int8>(columns, arguments, result, index, builder)
            || executeNumberConst<Int16>(columns, arguments, result, index, builder)
            || executeNumberConst<Int32>(columns, arguments, result, index, builder)
            || executeNumberConst<Int64>(columns, arguments, result, index, builder)
            || executeNumberConst<Float32>(columns, arguments, result, index, builder)
            || executeNumberConst<Float64>(columns, arguments, result, index, builder)
            || executeStringConst (columns, arguments, result, index, builder)
            || executeGenericConst (columns, arguments, result, index, builder)))
        throw Exception("Illegal column " + columns[arguments[0]].column->getName()
            + " of first argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN);
    }
}


void registerFunctionArrayElement(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayElement>();
}

}
