#include <Functions/FunctionsArray.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/ClearableHashMap.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/AggregationCommon.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Interpreters/castColumn.h>
#include <tuple>
#include <array>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/getMostSubtype.h>
#include <Core/TypeListNumber.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int ZERO_ARRAY_OR_TUPLE_INDEX;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


/// Implementation of FunctionArray.

String FunctionArray::getName() const
{
    return name;
}

FunctionPtr FunctionArray::create(const Context & context)
{
    return std::make_shared<FunctionArray>(context);
}

FunctionArray::FunctionArray(const Context & context)
    : context(context)
{
}

DataTypePtr FunctionArray::getReturnTypeImpl(const DataTypes & arguments) const
{
    return std::make_shared<DataTypeArray>(getLeastSupertype(arguments));
}

void FunctionArray::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    size_t num_elements = arguments.size();

    if (num_elements == 0)
    {
        /// We should return constant empty array.
        block.getByPosition(result).column = block.getByPosition(result).type->createColumnConstWithDefaultValue(block.rows());
        return;
    }

    const DataTypePtr & return_type = block.getByPosition(result).type;
    const DataTypePtr & elem_type = static_cast<const DataTypeArray &>(*return_type).getNestedType();

    size_t block_size = block.rows();

    /** If part of columns have not same type as common type of all elements of array,
        *  then convert them to common type.
        * If part of columns are constants,
        *  then convert them to full columns.
        */

    Columns columns_holder(num_elements);
    const IColumn * columns[num_elements];

    for (size_t i = 0; i < num_elements; ++i)
    {
        const auto & arg = block.getByPosition(arguments[i]);

        ColumnPtr preprocessed_column = arg.column;

        if (!arg.type->equals(*elem_type))
            preprocessed_column = castColumn(arg, elem_type, context);

        if (ColumnPtr materialized_column = preprocessed_column->convertToFullColumnIfConst())
            preprocessed_column = materialized_column;

        columns_holder[i] = std::move(preprocessed_column);
        columns[i] = columns_holder[i].get();
    }

    /// Create and fill the result array.

    auto out = ColumnArray::create(elem_type->createColumn());
    IColumn & out_data = out->getData();
    IColumn::Offsets & out_offsets = out->getOffsets();

    out_data.reserve(block_size * num_elements);
    out_offsets.resize(block_size);

    IColumn::Offset current_offset = 0;
    for (size_t i = 0; i < block_size; ++i)
    {
        for (size_t j = 0; j < num_elements; ++j)
            out_data.insertFrom(*columns[j], i);

        current_offset += num_elements;
        out_offsets[i] = current_offset;
    }

    block.getByPosition(result).column = std::move(out);
}


/// Implementation of FunctionArrayElement.

namespace ArrayImpl
{

class NullMapBuilder
{
public:
    operator bool() const { return src_null_map; }
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
        const ColumnString::Chars_t & data, const ColumnArray::Offsets & offsets, const ColumnString::Offsets & string_offsets,
        const ColumnArray::Offset index,
        ColumnString::Chars_t & result_data, ColumnArray::Offsets & result_offsets,
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
        const ColumnString::Chars_t & data, const ColumnArray::Offsets & offsets, const ColumnString::Offsets & string_offsets,
        const PaddedPODArray<TIndex> & indices,
        ColumnString::Chars_t & result_data, ColumnArray::Offsets & result_offsets,
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
bool FunctionArrayElement::executeNumberConst(Block & block, const ColumnNumbers & arguments, size_t result, const Field & index,
    ArrayImpl::NullMapBuilder & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());

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

    block.getByPosition(result).column = std::move(col_res);
    return true;
}

template <typename IndexType, typename DataType>
bool FunctionArrayElement::executeNumber(Block & block, const ColumnNumbers & arguments, size_t result, const PaddedPODArray<IndexType> & indices,
    ArrayImpl::NullMapBuilder & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());

    if (!col_array)
        return false;

    const ColumnVector<DataType> * col_nested = checkAndGetColumn<ColumnVector<DataType>>(&col_array->getData());

    if (!col_nested)
        return false;

    auto col_res = ColumnVector<DataType>::create();

    ArrayElementNumImpl<DataType>::template vector<IndexType>(
        col_nested->getData(), col_array->getOffsets(), indices, col_res->getData(), builder);

    block.getByPosition(result).column = std::move(col_res);
    return true;
}

bool FunctionArrayElement::executeStringConst(Block & block, const ColumnNumbers & arguments, size_t result, const Field & index,
    ArrayImpl::NullMapBuilder & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());

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

    block.getByPosition(result).column = std::move(col_res);
    return true;
}

template <typename IndexType>
bool FunctionArrayElement::executeString(Block & block, const ColumnNumbers & arguments, size_t result, const PaddedPODArray<IndexType> & indices,
    ArrayImpl::NullMapBuilder & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());

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

    block.getByPosition(result).column = std::move(col_res);
    return true;
}

bool FunctionArrayElement::executeGenericConst(Block & block, const ColumnNumbers & arguments, size_t result, const Field & index,
    ArrayImpl::NullMapBuilder & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());

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

    block.getByPosition(result).column = std::move(col_res);
    return true;
}

template <typename IndexType>
bool FunctionArrayElement::executeGeneric(Block & block, const ColumnNumbers & arguments, size_t result, const PaddedPODArray<IndexType> & indices,
    ArrayImpl::NullMapBuilder & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());

    if (!col_array)
        return false;

    const auto & col_nested = col_array->getData();
    auto col_res = col_nested.cloneEmpty();

    ArrayElementGenericImpl::vector<IndexType>(
        col_nested, col_array->getOffsets(), indices, *col_res, builder);

    block.getByPosition(result).column = std::move(col_res);
    return true;
}

template <typename IndexType>
bool FunctionArrayElement::executeConst(Block & block, const ColumnNumbers & arguments, size_t result, const PaddedPODArray<IndexType> & indices,
    ArrayImpl::NullMapBuilder & builder)
{
    const ColumnArray * col_array = checkAndGetColumnConstData<ColumnArray>(block.getByPosition(arguments[0]).column.get());

    if (!col_array)
        return false;

    auto res = block.getByPosition(result).type->createColumn();

    size_t rows = block.rows();
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

    block.getByPosition(result).column = std::move(res);
    return true;
}

template <typename IndexType>
bool FunctionArrayElement::executeArgument(Block & block, const ColumnNumbers & arguments, size_t result,
    ArrayImpl::NullMapBuilder & builder)
{
    auto index = checkAndGetColumn<ColumnVector<IndexType>>(block.getByPosition(arguments[1]).column.get());

    if (!index)
        return false;

    const auto & index_data = index->getData();

    if (builder)
        builder.initSink(index_data.size());

    if (!( executeNumber<IndexType, UInt8>(block, arguments, result, index_data, builder)
        || executeNumber<IndexType, UInt16>(block, arguments, result, index_data, builder)
        || executeNumber<IndexType, UInt32>(block, arguments, result, index_data, builder)
        || executeNumber<IndexType, UInt64>(block, arguments, result, index_data, builder)
        || executeNumber<IndexType, Int8>(block, arguments, result, index_data, builder)
        || executeNumber<IndexType, Int16>(block, arguments, result, index_data, builder)
        || executeNumber<IndexType, Int32>(block, arguments, result, index_data, builder)
        || executeNumber<IndexType, Int64>(block, arguments, result, index_data, builder)
        || executeNumber<IndexType, Float32>(block, arguments, result, index_data, builder)
        || executeNumber<IndexType, Float64>(block, arguments, result, index_data, builder)
        || executeConst <IndexType>(block, arguments, result, index_data, builder)
        || executeString<IndexType>(block, arguments, result, index_data, builder)
        || executeGeneric<IndexType>(block, arguments, result, index_data, builder)))
    throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

    return true;
}

bool FunctionArrayElement::executeTuple(Block & block, const ColumnNumbers & arguments, size_t result)
{
    const ColumnArray * col_array = typeid_cast<const ColumnArray *>(block.getByPosition(arguments[0]).column.get());

    if (!col_array)
        return false;

    const ColumnTuple * col_nested = typeid_cast<const ColumnTuple *>(&col_array->getData());

    if (!col_nested)
        return false;

    const Columns & tuple_columns = col_nested->getColumns();
    size_t tuple_size = tuple_columns.size();

    const DataTypes & tuple_types = typeid_cast<const DataTypeTuple &>(
        *typeid_cast<const DataTypeArray &>(*block.getByPosition(arguments[0]).type).getNestedType()).getElements();

    /** We will calculate the function for the tuple of the internals of the array.
      * To do this, create a temporary block.
      * It will consist of the following columns
      * - the index of the array to be taken;
      * - an array of the first elements of the tuples;
      * - the result of taking the elements by the index for an array of the first elements of the tuples;
      * - array of the second elements of the tuples;
      * - result of taking elements by index for an array of second elements of tuples;
      * ...
      */
    Block block_of_temporary_results;
    block_of_temporary_results.insert(block.getByPosition(arguments[1]));

    /// results of taking elements by index for arrays from each element of the tuples;
    Columns result_tuple_columns;

    for (size_t i = 0; i < tuple_size; ++i)
    {
        ColumnWithTypeAndName array_of_tuple_section;
        array_of_tuple_section.column = ColumnArray::create(tuple_columns[i], col_array->getOffsetsPtr());
        array_of_tuple_section.type = std::make_shared<DataTypeArray>(tuple_types[i]);
        block_of_temporary_results.insert(array_of_tuple_section);

        ColumnWithTypeAndName array_elements_of_tuple_section;
        array_elements_of_tuple_section.type = getReturnTypeImpl(
            {block_of_temporary_results.getByPosition(i * 2 + 1).type, block_of_temporary_results.getByPosition(0).type});
        block_of_temporary_results.insert(array_elements_of_tuple_section);

        executeImpl(block_of_temporary_results, ColumnNumbers{i * 2 + 1, 0}, i * 2 + 2);

        result_tuple_columns.emplace_back(std::move(block_of_temporary_results.getByPosition(i * 2 + 2).column));
    }

    block.getByPosition(result).column = ColumnTuple::create(result_tuple_columns);

    return true;
}

String FunctionArrayElement::getName() const
{
    return name;
}

DataTypePtr FunctionArrayElement::getReturnTypeImpl(const DataTypes & arguments) const
{
    const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
    if (!array_type)
        throw Exception("First argument for function " + getName() + " must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    if (!arguments[1]->isInteger())
        throw Exception("Second argument for function " + getName() + " must be integer.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return array_type->getNestedType();
}

void FunctionArrayElement::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    /// Check nullability.
    bool is_array_of_nullable = false;

    const ColumnArray * col_array = nullptr;
    const ColumnArray * col_const_array = nullptr;

    col_array = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());
    if (col_array)
        is_array_of_nullable = col_array->getData().isColumnNullable();
    else
    {
        col_const_array = checkAndGetColumnConstData<ColumnArray>(block.getByPosition(arguments[0]).column.get());
        if (col_const_array)
            is_array_of_nullable = col_const_array->getData().isColumnNullable();
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
            + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

    if (!is_array_of_nullable)
    {
        ArrayImpl::NullMapBuilder builder;
        perform(block, arguments, result, builder);
    }
    else
    {
        /// Perform initializations.
        ArrayImpl::NullMapBuilder builder;
        Block source_block;

        const auto & input_type = typeid_cast<const DataTypeNullable &>(*typeid_cast<const DataTypeArray &>(*block.getByPosition(arguments[0]).type).getNestedType()).getNestedType();
        const auto & tmp_ret_type = typeid_cast<const DataTypeNullable &>(*block.getByPosition(result).type).getNestedType();

        if (col_array)
        {
            const auto & nullable_col = typeid_cast<const ColumnNullable &>(col_array->getData());
            const auto & nested_col = nullable_col.getNestedColumnPtr();

            /// Put nested_col inside a ColumnArray.
            source_block =
            {
                {
                    ColumnArray::create(nested_col, col_array->getOffsetsPtr()),
                    std::make_shared<DataTypeArray>(input_type),
                    ""
                },
                block.getByPosition(arguments[1]),
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
            const auto & nullable_col = static_cast<const ColumnNullable &>(col_const_array->getData());
            const auto & nested_col = nullable_col.getNestedColumnPtr();

            source_block =
            {
                {
                    ColumnConst::create(ColumnArray::create(nested_col, col_const_array->getOffsetsPtr()), block.rows()),
                    std::make_shared<DataTypeArray>(input_type),
                    ""
                },
                block.getByPosition(arguments[1]),
                {
                    nullptr,
                    tmp_ret_type,
                    ""
                }
            };

            builder.initSource(nullable_col.getNullMapData().data());
        }

        perform(source_block, {0, 1}, 2, builder);

        /// Store the result.
        const ColumnWithTypeAndName & source_col = source_block.getByPosition(2);
        ColumnWithTypeAndName & dest_col = block.getByPosition(result);
        dest_col.column = ColumnNullable::create(source_col.column, builder ? std::move(builder).getNullMapColumnPtr() : ColumnUInt8::create());
    }
}

void FunctionArrayElement::perform(Block & block, const ColumnNumbers & arguments, size_t result,
    ArrayImpl::NullMapBuilder & builder)
{
    if (executeTuple(block, arguments, result))
    {
    }
    else if (!block.getByPosition(arguments[1]).column->isColumnConst())
    {
        if (!( executeArgument<UInt8>(block, arguments, result, builder)
            || executeArgument<UInt16>(block, arguments, result, builder)
            || executeArgument<UInt32>(block, arguments, result, builder)
            || executeArgument<UInt64>(block, arguments, result, builder)
            || executeArgument<Int8>(block, arguments, result, builder)
            || executeArgument<Int16>(block, arguments, result, builder)
            || executeArgument<Int32>(block, arguments, result, builder)
            || executeArgument<Int64>(block, arguments, result, builder)))
        throw Exception("Second argument for function " + getName() + " must must have UInt or Int type.",
                        ErrorCodes::ILLEGAL_COLUMN);
    }
    else
    {
        Field index = (*block.getByPosition(arguments[1]).column)[0];

        if (builder)
            builder.initSink(block.rows());

        if (index == UInt64(0))
            throw Exception("Array indices is 1-based", ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX);

        if (!( executeNumberConst<UInt8>(block, arguments, result, index, builder)
            || executeNumberConst<UInt16>(block, arguments, result, index, builder)
            || executeNumberConst<UInt32>(block, arguments, result, index, builder)
            || executeNumberConst<UInt64>(block, arguments, result, index, builder)
            || executeNumberConst<Int8>(block, arguments, result, index, builder)
            || executeNumberConst<Int16>(block, arguments, result, index, builder)
            || executeNumberConst<Int32>(block, arguments, result, index, builder)
            || executeNumberConst<Int64>(block, arguments, result, index, builder)
            || executeNumberConst<Float32>(block, arguments, result, index, builder)
            || executeNumberConst<Float64>(block, arguments, result, index, builder)
            || executeStringConst (block, arguments, result, index, builder)
            || executeGenericConst (block, arguments, result, index, builder)))
        throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
            + " of first argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN);
    }
}

/// Implementation of FunctionArrayEnumerate.

FunctionPtr FunctionArrayEnumerate::create(const Context &)
{
    return std::make_shared<FunctionArrayEnumerate>();
}

String FunctionArrayEnumerate::getName() const
{
    return name;
}

DataTypePtr FunctionArrayEnumerate::getReturnTypeImpl(const DataTypes & arguments) const
{
    const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
    if (!array_type)
        throw Exception("First argument for function " + getName() + " must be an array but it has type "
            + arguments[0]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>());
}

void FunctionArrayEnumerate::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    if (const ColumnArray * array = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get()))
    {
        const ColumnArray::Offsets & offsets = array->getOffsets();

        auto res_nested = ColumnUInt32::create();

        ColumnUInt32::Container & res_values = res_nested->getData();
        res_values.resize(array->getData().size());
        size_t prev_off = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            size_t off = offsets[i];
            for (size_t j = prev_off; j < off; ++j)
            {
                res_values[j] = j - prev_off + 1;
            }
            prev_off = off;
        }

        block.getByPosition(result).column = ColumnArray::create(std::move(res_nested), array->getOffsetsPtr());
    }
    else
    {
        throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of first argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN);
    }
}

/// Implementation of FunctionArrayUniq.

FunctionPtr FunctionArrayUniq::create(const Context &) { return std::make_shared<FunctionArrayUniq>(); }

String FunctionArrayUniq::getName() const
{
    return name;
}

DataTypePtr FunctionArrayUniq::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments.size() == 0)
        throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
            + toString(arguments.size()) + ", should be at least 1.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (size_t i = 0; i < arguments.size(); ++i)
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].get());
        if (!array_type)
            throw Exception("All arguments for function " + getName() + " must be arrays but argument " +
                toString(i + 1) + " has type " + arguments[i]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    return std::make_shared<DataTypeUInt32>();
}

void FunctionArrayUniq::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    Columns array_columns(arguments.size());
    const ColumnArray::Offsets * offsets = nullptr;
    ColumnRawPtrs data_columns(arguments.size());
    ColumnRawPtrs original_data_columns(arguments.size());
    ColumnRawPtrs null_maps(arguments.size());

    bool has_nullable_columns = false;

    for (size_t i = 0; i < arguments.size(); ++i)
    {
        ColumnPtr array_ptr = block.getByPosition(arguments[i]).column;
        const ColumnArray * array = checkAndGetColumn<ColumnArray>(array_ptr.get());
        if (!array)
        {
            const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(
                block.getByPosition(arguments[i]).column.get());
            if (!const_array)
                throw Exception("Illegal column " + block.getByPosition(arguments[i]).column->getName()
                    + " of " + toString(i + 1) + getOrdinalSuffix(i + 1) + " argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
            array_ptr = const_array->convertToFullColumn();
            array = static_cast<const ColumnArray *>(array_ptr.get());
        }

        array_columns[i] = array_ptr;

        const ColumnArray::Offsets & offsets_i = array->getOffsets();
        if (i == 0)
            offsets = &offsets_i;
        else if (offsets_i != *offsets)
            throw Exception("Lengths of all arrays passed to " + getName() + " must be equal.",
                ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);

        data_columns[i] = &array->getData();
        original_data_columns[i] = data_columns[i];

        if (data_columns[i]->isColumnNullable())
        {
            has_nullable_columns = true;
            const auto & nullable_col = static_cast<const ColumnNullable &>(*data_columns[i]);
            data_columns[i] = &nullable_col.getNestedColumn();
            null_maps[i] = &nullable_col.getNullMapColumn();
        }
        else
            null_maps[i] = nullptr;
    }

    const ColumnArray * first_array = static_cast<const ColumnArray *>(array_columns[0].get());
    const IColumn * first_null_map = null_maps[0];
    auto res = ColumnUInt32::create();

    ColumnUInt32::Container & res_values = res->getData();
    res_values.resize(offsets->size());

    if (arguments.size() == 1)
    {
        if (!( executeNumber<UInt8>(first_array, first_null_map, res_values)
            || executeNumber<UInt16>(first_array, first_null_map, res_values)
            || executeNumber<UInt32>(first_array, first_null_map, res_values)
            || executeNumber<UInt64>(first_array, first_null_map, res_values)
            || executeNumber<Int8>(first_array, first_null_map, res_values)
            || executeNumber<Int16>(first_array, first_null_map, res_values)
            || executeNumber<Int32>(first_array, first_null_map, res_values)
            || executeNumber<Int64>(first_array, first_null_map, res_values)
            || executeNumber<Float32>(first_array, first_null_map, res_values)
            || executeNumber<Float64>(first_array, first_null_map, res_values)
            || executeString(first_array, first_null_map, res_values)))
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                    + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
    else
    {
        if (!execute128bit(*offsets, data_columns, null_maps, res_values, has_nullable_columns))
            executeHashed(*offsets, original_data_columns, res_values);
    }

    block.getByPosition(result).column = std::move(res);
}

template <typename T>
bool FunctionArrayUniq::executeNumber(const ColumnArray * array, const IColumn * null_map, ColumnUInt32::Container & res_values)
{
    const IColumn * inner_col;

    const auto & array_data = array->getData();
    if (array_data.isColumnNullable())
    {
        const auto & nullable_col = static_cast<const ColumnNullable &>(array_data);
        inner_col = &nullable_col.getNestedColumn();
    }
    else
        inner_col = &array_data;

    const ColumnVector<T> * nested = checkAndGetColumn<ColumnVector<T>>(inner_col);
    if (!nested)
        return false;
    const ColumnArray::Offsets & offsets = array->getOffsets();
    const typename ColumnVector<T>::Container & values = nested->getData();

    using Set = ClearableHashSet<T, DefaultHash<T>, HashTableGrower<INITIAL_SIZE_DEGREE>,
        HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(T)>>;

    const PaddedPODArray<UInt8> * null_map_data = nullptr;
    if (null_map)
        null_map_data = &static_cast<const ColumnUInt8 *>(null_map)->getData();

    Set set;
    size_t prev_off = 0;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        set.clear();
        bool found_null = false;
        size_t off = offsets[i];
        for (size_t j = prev_off; j < off; ++j)
        {
            if (null_map_data && ((*null_map_data)[j] == 1))
                found_null = true;
            else
                set.insert(values[j]);
        }

        res_values[i] = set.size() + found_null;
        prev_off = off;
    }
    return true;
}

bool FunctionArrayUniq::executeString(const ColumnArray * array, const IColumn * null_map, ColumnUInt32::Container & res_values)
{
    const IColumn * inner_col;

    const auto & array_data = array->getData();
    if (array_data.isColumnNullable())
    {
        const auto & nullable_col = static_cast<const ColumnNullable &>(array_data);
        inner_col = &nullable_col.getNestedColumn();
    }
    else
        inner_col = &array_data;

    const ColumnString * nested = checkAndGetColumn<ColumnString>(inner_col);
    if (!nested)
        return false;
    const ColumnArray::Offsets & offsets = array->getOffsets();

    using Set = ClearableHashSet<StringRef, StringRefHash, HashTableGrower<INITIAL_SIZE_DEGREE>,
        HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(StringRef)>>;

    const PaddedPODArray<UInt8> * null_map_data = nullptr;
    if (null_map)
        null_map_data = &static_cast<const ColumnUInt8 *>(null_map)->getData();

    Set set;
    size_t prev_off = 0;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        set.clear();
        bool found_null = false;
        size_t off = offsets[i];
        for (size_t j = prev_off; j < off; ++j)
        {
            if (null_map_data && ((*null_map_data)[j] == 1))
                found_null = true;
            else
                set.insert(nested->getDataAt(j));
        }

        res_values[i] = set.size() + found_null;
        prev_off = off;
    }
    return true;
}


bool FunctionArrayUniq::execute128bit(
    const ColumnArray::Offsets & offsets,
    const ColumnRawPtrs & columns,
    const ColumnRawPtrs & null_maps,
    ColumnUInt32::Container & res_values,
    bool has_nullable_columns)
{
    size_t count = columns.size();
    size_t keys_bytes = 0;
    Sizes key_sizes(count);

    for (size_t j = 0; j < count; ++j)
    {
        if (!columns[j]->isFixedAndContiguous())
            return false;
        key_sizes[j] = columns[j]->sizeOfValueIfFixed();
        keys_bytes += key_sizes[j];
    }
    if (has_nullable_columns)
        keys_bytes += std::tuple_size<KeysNullMap<UInt128>>::value;

    if (keys_bytes > 16)
        return false;

    using Set = ClearableHashSet<UInt128, UInt128HashCRC32, HashTableGrower<INITIAL_SIZE_DEGREE>,
        HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(UInt128)>>;

    /// Suppose that, for a given row, each of the N columns has an array whose length is M.
    /// Denote arr_i each of these arrays (1 <= i <= N). Then the following is performed:
    ///
    /// col1      ...  colN
    ///
    /// arr_1[1], ..., arr_N[1] -> pack into a binary blob b1
    /// .
    /// .
    /// .
    /// arr_1[M], ..., arr_N[M] -> pack into a binary blob bM
    ///
    /// Each binary blob is inserted into a hash table.
    ///
    Set set;
    size_t prev_off = 0;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        set.clear();
        size_t off = offsets[i];
        for (size_t j = prev_off; j < off; ++j)
        {
            if (has_nullable_columns)
            {
                KeysNullMap<UInt128> bitmap{};

                for (size_t i = 0; i < columns.size(); ++i)
                {
                    if (null_maps[i])
                    {
                        const auto & null_map = static_cast<const ColumnUInt8 &>(*null_maps[i]).getData();
                        if (null_map[j] == 1)
                        {
                            size_t bucket = i / 8;
                            size_t offset = i % 8;
                            bitmap[bucket] |= UInt8(1) << offset;
                        }
                    }
                }
                set.insert(packFixed<UInt128>(j, count, columns, key_sizes, bitmap));
            }
            else
                set.insert(packFixed<UInt128>(j, count, columns, key_sizes));
        }

        res_values[i] = set.size();
        prev_off = off;
    }

    return true;
}

void FunctionArrayUniq::executeHashed(
    const ColumnArray::Offsets & offsets,
    const ColumnRawPtrs & columns,
    ColumnUInt32::Container & res_values)
{
    size_t count = columns.size();

    using Set = ClearableHashSet<UInt128, UInt128TrivialHash, HashTableGrower<INITIAL_SIZE_DEGREE>,
        HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(UInt128)>>;

    Set set;
    size_t prev_off = 0;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        set.clear();
        size_t off = offsets[i];
        for (size_t j = prev_off; j < off; ++j)
            set.insert(hash128(j, count, columns));

        res_values[i] = set.size();
        prev_off = off;
    }
}

/// Implementation of FunctionArrayEnumerateUniq.

FunctionPtr FunctionArrayEnumerateUniq::create(const Context &)
{
    return std::make_shared<FunctionArrayEnumerateUniq>();
}

String FunctionArrayEnumerateUniq::getName() const
{
    return name;
}

DataTypePtr FunctionArrayEnumerateUniq::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments.size() == 0)
        throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
            + toString(arguments.size()) + ", should be at least 1.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (size_t i = 0; i < arguments.size(); ++i)
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].get());
        if (!array_type)
            throw Exception("All arguments for function " + getName() + " must be arrays but argument " +
                toString(i + 1) + " has type " + arguments[i]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>());
}

void FunctionArrayEnumerateUniq::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    Columns array_columns(arguments.size());
    const ColumnArray::Offsets * offsets = nullptr;
    ColumnRawPtrs data_columns(arguments.size());
    ColumnRawPtrs original_data_columns(arguments.size());
    ColumnRawPtrs null_maps(arguments.size());

    bool has_nullable_columns = false;

    for (size_t i = 0; i < arguments.size(); ++i)
    {
        ColumnPtr array_ptr = block.getByPosition(arguments[i]).column;
        const ColumnArray * array = checkAndGetColumn<ColumnArray>(array_ptr.get());
        if (!array)
        {
            const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(
                block.getByPosition(arguments[i]).column.get());
            if (!const_array)
                throw Exception("Illegal column " + block.getByPosition(arguments[i]).column->getName()
                    + " of " + toString(i + 1) + "-th argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
            array_ptr = const_array->convertToFullColumn();
            array = checkAndGetColumn<ColumnArray>(array_ptr.get());
        }
        array_columns[i] = array_ptr;
        const ColumnArray::Offsets & offsets_i = array->getOffsets();
        if (i == 0)
            offsets = &offsets_i;
        else if (offsets_i != *offsets)
            throw Exception("Lengths of all arrays passed to " + getName() + " must be equal.",
                ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);

        data_columns[i] = &array->getData();
        original_data_columns[i] = data_columns[i];

        if (data_columns[i]->isColumnNullable())
        {
            has_nullable_columns = true;
            const auto & nullable_col = static_cast<const ColumnNullable &>(*data_columns[i]);
            data_columns[i] = &nullable_col.getNestedColumn();
            null_maps[i] = &nullable_col.getNullMapColumn();
        }
        else
            null_maps[i] = nullptr;
    }

    const ColumnArray * first_array = checkAndGetColumn<ColumnArray>(array_columns[0].get());
    const IColumn * first_null_map = null_maps[0];
    auto res_nested = ColumnUInt32::create();

    ColumnUInt32::Container & res_values = res_nested->getData();
    if (!offsets->empty())
        res_values.resize(offsets->back());

    if (arguments.size() == 1)
    {
        if (!( executeNumber<UInt8>(first_array, first_null_map, res_values)
            || executeNumber<UInt16>(first_array, first_null_map, res_values)
            || executeNumber<UInt32>(first_array, first_null_map, res_values)
            || executeNumber<UInt64>(first_array, first_null_map, res_values)
            || executeNumber<Int8>(first_array, first_null_map, res_values)
            || executeNumber<Int16>(first_array, first_null_map, res_values)
            || executeNumber<Int32>(first_array, first_null_map, res_values)
            || executeNumber<Int64>(first_array, first_null_map, res_values)
            || executeNumber<Float32>(first_array, first_null_map, res_values)
            || executeNumber<Float64>(first_array, first_null_map, res_values)
            || executeString (first_array, first_null_map, res_values)))
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                    + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
    else
    {
        if (!execute128bit(*offsets, data_columns, null_maps, res_values, has_nullable_columns))
            executeHashed(*offsets, original_data_columns, res_values);
    }

    block.getByPosition(result).column = ColumnArray::create(std::move(res_nested), first_array->getOffsetsPtr());
}


template <typename T>
bool FunctionArrayEnumerateUniq::executeNumber(const ColumnArray * array, const IColumn * null_map, ColumnUInt32::Container & res_values)
{
    const IColumn * inner_col;

    const auto & array_data = array->getData();
    if (array_data.isColumnNullable())
    {
        const auto & nullable_col = static_cast<const ColumnNullable &>(array_data);
        inner_col = &nullable_col.getNestedColumn();
    }
    else
        inner_col = &array_data;

    const ColumnVector<T> * nested = checkAndGetColumn<ColumnVector<T>>(inner_col);
    if (!nested)
        return false;
    const ColumnArray::Offsets & offsets = array->getOffsets();
    const typename ColumnVector<T>::Container & values = nested->getData();

    using ValuesToIndices = ClearableHashMap<T, UInt32, DefaultHash<T>, HashTableGrower<INITIAL_SIZE_DEGREE>,
        HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(T)>>;

    const PaddedPODArray<UInt8> * null_map_data = nullptr;
    if (null_map)
        null_map_data = &static_cast<const ColumnUInt8 *>(null_map)->getData();

    ValuesToIndices indices;
    size_t prev_off = 0;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        indices.clear();
        UInt32 null_count = 0;
        size_t off = offsets[i];
        for (size_t j = prev_off; j < off; ++j)
        {
            if (null_map_data && ((*null_map_data)[j] == 1))
                res_values[j] = ++null_count;
            else
                res_values[j] = ++indices[values[j]];
        }
        prev_off = off;
    }
    return true;
}

bool FunctionArrayEnumerateUniq::executeString(const ColumnArray * array, const IColumn * null_map, ColumnUInt32::Container & res_values)
{
    const IColumn * inner_col;

    const auto & array_data = array->getData();
    if (array_data.isColumnNullable())
    {
        const auto & nullable_col = static_cast<const ColumnNullable &>(array_data);
        inner_col = &nullable_col.getNestedColumn();
    }
    else
        inner_col = &array_data;

    const ColumnString * nested = checkAndGetColumn<ColumnString>(inner_col);
    if (!nested)
        return false;
    const ColumnArray::Offsets & offsets = array->getOffsets();

    size_t prev_off = 0;
    using ValuesToIndices = ClearableHashMap<StringRef, UInt32, StringRefHash, HashTableGrower<INITIAL_SIZE_DEGREE>,
        HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(StringRef)>>;

    const PaddedPODArray<UInt8> * null_map_data = nullptr;
    if (null_map)
        null_map_data = &static_cast<const ColumnUInt8 *>(null_map)->getData();

    ValuesToIndices indices;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        indices.clear();
        UInt32 null_count = 0;
        size_t off = offsets[i];
        for (size_t j = prev_off; j < off; ++j)
        {
            if (null_map_data && ((*null_map_data)[j] == 1))
                res_values[j] = ++null_count;
            else
                res_values[j] = ++indices[nested->getDataAt(j)];
        }
        prev_off = off;
    }
    return true;
}

bool FunctionArrayEnumerateUniq::execute128bit(
    const ColumnArray::Offsets & offsets,
    const ColumnRawPtrs & columns,
    const ColumnRawPtrs & null_maps,
    ColumnUInt32::Container & res_values,
    bool has_nullable_columns)
{
    size_t count = columns.size();
    size_t keys_bytes = 0;
    Sizes key_sizes(count);

    for (size_t j = 0; j < count; ++j)
    {
        if (!columns[j]->isFixedAndContiguous())
            return false;
        key_sizes[j] = columns[j]->sizeOfValueIfFixed();
        keys_bytes += key_sizes[j];
    }
    if (has_nullable_columns)
        keys_bytes += std::tuple_size<KeysNullMap<UInt128>>::value;

    if (keys_bytes > 16)
        return false;

    using ValuesToIndices = ClearableHashMap<UInt128, UInt32, UInt128HashCRC32, HashTableGrower<INITIAL_SIZE_DEGREE>,
        HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(UInt128)>>;

    ValuesToIndices indices;
    size_t prev_off = 0;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        indices.clear();
        size_t off = offsets[i];
        for (size_t j = prev_off; j < off; ++j)
        {
            if (has_nullable_columns)
            {
                KeysNullMap<UInt128> bitmap{};

                for (size_t i = 0; i < columns.size(); ++i)
                {
                    if (null_maps[i])
                    {
                        const auto & null_map = static_cast<const ColumnUInt8 &>(*null_maps[i]).getData();
                        if (null_map[j] == 1)
                        {
                            size_t bucket = i / 8;
                            size_t offset = i % 8;
                            bitmap[bucket] |= UInt8(1) << offset;
                        }
                    }
                }
                res_values[j] = ++indices[packFixed<UInt128>(j, count, columns, key_sizes, bitmap)];
            }
            else
                res_values[j] = ++indices[packFixed<UInt128>(j, count, columns, key_sizes)];
        }
        prev_off = off;
    }

    return true;
}

void FunctionArrayEnumerateUniq::executeHashed(
    const ColumnArray::Offsets & offsets,
    const ColumnRawPtrs & columns,
    ColumnUInt32::Container & res_values)
{
    size_t count = columns.size();

    using ValuesToIndices = ClearableHashMap<UInt128, UInt32, UInt128TrivialHash, HashTableGrower<INITIAL_SIZE_DEGREE>,
        HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(UInt128)>>;

    ValuesToIndices indices;
    size_t prev_off = 0;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        indices.clear();
        size_t off = offsets[i];
        for (size_t j = prev_off; j < off; ++j)
        {
            res_values[j] = ++indices[hash128(j, count, columns)];
        }
        prev_off = off;
    }
}

/// Implementation of FunctionEmptyArrayToSingle.

FunctionPtr FunctionEmptyArrayToSingle::create(const Context &) { return std::make_shared<FunctionEmptyArrayToSingle>(); }

String FunctionEmptyArrayToSingle::getName() const
{
    return name;
}

DataTypePtr FunctionEmptyArrayToSingle::getReturnTypeImpl(const DataTypes & arguments) const
{
    const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
    if (!array_type)
        throw Exception("Argument for function " + getName() + " must be array.",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return arguments[0];
}


namespace
{
    namespace FunctionEmptyArrayToSingleImpl
    {
        bool executeConst(Block & block, const ColumnNumbers & arguments, size_t result)
        {
            if (const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(block.getByPosition(arguments[0]).column.get()))
            {
                if (const_array->getValue<Array>().empty())
                {
                    auto nested_type = typeid_cast<const DataTypeArray &>(*block.getByPosition(arguments[0]).type).getNestedType();

                    block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(
                        block.rows(),
                        Array{nested_type->getDefault()});
                }
                else
                    block.getByPosition(result).column = block.getByPosition(arguments[0]).column;

                return true;
            }
            else
                return false;
        }

        template <typename T, bool nullable>
        bool executeNumber(
            const IColumn & src_data, const ColumnArray::Offsets & src_offsets,
            IColumn & res_data_col, ColumnArray::Offsets & res_offsets,
            const NullMap * src_null_map,
            NullMap * res_null_map)
        {
            if (const ColumnVector<T> * src_data_concrete = checkAndGetColumn<ColumnVector<T>>(&src_data))
            {
                const PaddedPODArray<T> & src_data = src_data_concrete->getData();
                PaddedPODArray<T> & res_data = static_cast<ColumnVector<T> &>(res_data_col).getData();

                size_t size = src_offsets.size();
                res_offsets.resize(size);
                res_data.reserve(src_data.size());

                if (nullable)
                    res_null_map->reserve(src_null_map->size());

                ColumnArray::Offset src_prev_offset = 0;
                ColumnArray::Offset res_prev_offset = 0;

                for (size_t i = 0; i < size; ++i)
                {
                    if (src_offsets[i] != src_prev_offset)
                    {
                        size_t size_to_write = src_offsets[i] - src_prev_offset;
                        res_data.resize(res_prev_offset + size_to_write);
                        memcpy(&res_data[res_prev_offset], &src_data[src_prev_offset], size_to_write * sizeof(T));

                        if (nullable)
                        {
                            res_null_map->resize(res_prev_offset + size_to_write);
                            memcpy(&(*res_null_map)[res_prev_offset], &(*src_null_map)[src_prev_offset], size_to_write);
                        }

                        res_prev_offset += size_to_write;
                        res_offsets[i] = res_prev_offset;
                    }
                    else
                    {
                        res_data.push_back(T());
                        ++res_prev_offset;
                        res_offsets[i] = res_prev_offset;

                        if (nullable)
                            res_null_map->push_back(1); /// Push NULL.
                    }

                    src_prev_offset = src_offsets[i];
                }

                return true;
            }
            else
                return false;
        }


        template <bool nullable>
        bool executeFixedString(
            const IColumn & src_data, const ColumnArray::Offsets & src_offsets,
            IColumn & res_data_col, ColumnArray::Offsets & res_offsets,
            const NullMap * src_null_map,
            NullMap * res_null_map)
        {
            if (const ColumnFixedString * src_data_concrete = checkAndGetColumn<ColumnFixedString>(&src_data))
            {
                const size_t n = src_data_concrete->getN();
                const ColumnFixedString::Chars_t & src_data = src_data_concrete->getChars();

                auto concrete_res_data = typeid_cast<ColumnFixedString *>(&res_data_col);
                if (!concrete_res_data)
                    throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};

                ColumnFixedString::Chars_t & res_data = concrete_res_data->getChars();
                size_t size = src_offsets.size();
                res_offsets.resize(size);
                res_data.reserve(src_data.size());

                if (nullable)
                    res_null_map->reserve(src_null_map->size());

                ColumnArray::Offset src_prev_offset = 0;
                ColumnArray::Offset res_prev_offset = 0;

                for (size_t i = 0; i < size; ++i)
                {
                    if (src_offsets[i] != src_prev_offset)
                    {
                        size_t size_to_write = src_offsets[i] - src_prev_offset;
                        size_t prev_res_data_size = res_data.size();
                        res_data.resize(prev_res_data_size + size_to_write * n);
                        memcpy(&res_data[prev_res_data_size], &src_data[src_prev_offset * n], size_to_write * n);

                        if (nullable)
                        {
                            res_null_map->resize(res_prev_offset + size_to_write);
                            memcpy(&(*res_null_map)[res_prev_offset], &(*src_null_map)[src_prev_offset], size_to_write);
                        }

                        res_prev_offset += size_to_write;
                        res_offsets[i] = res_prev_offset;
                    }
                    else
                    {
                        size_t prev_res_data_size = res_data.size();
                        res_data.resize(prev_res_data_size + n);
                        memset(&res_data[prev_res_data_size], 0, n);
                        ++res_prev_offset;
                        res_offsets[i] = res_prev_offset;

                        if (nullable)
                            res_null_map->push_back(1);
                    }

                    src_prev_offset = src_offsets[i];
                }

                return true;
            }
            else
                return false;
        }


        template <bool nullable>
        bool executeString(
            const IColumn & src_data, const ColumnArray::Offsets & src_array_offsets,
            IColumn & res_data_col, ColumnArray::Offsets & res_array_offsets,
            const NullMap * src_null_map,
            NullMap * res_null_map)
        {
            if (const ColumnString * src_data_concrete = checkAndGetColumn<ColumnString>(&src_data))
            {
                const ColumnString::Offsets & src_string_offsets = src_data_concrete->getOffsets();

                auto concrete_res_string_offsets = typeid_cast<ColumnString *>(&res_data_col);
                if (!concrete_res_string_offsets)
                    throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};
                ColumnString::Offsets & res_string_offsets = concrete_res_string_offsets->getOffsets();

                const ColumnString::Chars_t & src_data = src_data_concrete->getChars();

                auto concrete_res_data = typeid_cast<ColumnString *>(&res_data_col);
                if (!concrete_res_data)
                    throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};
                ColumnString::Chars_t & res_data = concrete_res_data->getChars();

                size_t size = src_array_offsets.size();
                res_array_offsets.resize(size);
                res_string_offsets.reserve(src_string_offsets.size());
                res_data.reserve(src_data.size());

                if (nullable)
                    res_null_map->reserve(src_null_map->size());

                ColumnArray::Offset src_array_prev_offset = 0;
                ColumnArray::Offset res_array_prev_offset = 0;

                ColumnString::Offset src_string_prev_offset = 0;
                ColumnString::Offset res_string_prev_offset = 0;

                for (size_t i = 0; i < size; ++i)
                {
                    if (src_array_offsets[i] != src_array_prev_offset)
                    {
                        size_t array_size = src_array_offsets[i] - src_array_prev_offset;

                        size_t bytes_to_copy = 0;
                        size_t from_string_prev_offset_local = src_string_prev_offset;
                        for (size_t j = 0; j < array_size; ++j)
                        {
                            size_t string_size = src_string_offsets[src_array_prev_offset + j] - from_string_prev_offset_local;

                            res_string_prev_offset += string_size;
                            res_string_offsets.push_back(res_string_prev_offset);

                            from_string_prev_offset_local += string_size;
                            bytes_to_copy += string_size;
                        }

                        size_t res_data_old_size = res_data.size();
                        res_data.resize(res_data_old_size + bytes_to_copy);
                        memcpy(&res_data[res_data_old_size], &src_data[src_string_prev_offset], bytes_to_copy);

                        if (nullable)
                        {
                            res_null_map->resize(res_array_prev_offset + array_size);
                            memcpy(&(*res_null_map)[res_array_prev_offset], &(*src_null_map)[src_array_prev_offset], array_size);
                        }

                        res_array_prev_offset += array_size;
                        res_array_offsets[i] = res_array_prev_offset;
                    }
                    else
                    {
                        res_data.push_back(0);  /// An empty string, including zero at the end.

                        if (nullable)
                            res_null_map->push_back(1);

                        ++res_string_prev_offset;
                        res_string_offsets.push_back(res_string_prev_offset);

                        ++res_array_prev_offset;
                        res_array_offsets[i] = res_array_prev_offset;
                    }

                    src_array_prev_offset = src_array_offsets[i];

                    if (src_array_prev_offset)
                        src_string_prev_offset = src_string_offsets[src_array_prev_offset - 1];
                }

                return true;
            }
            else
                return false;
        }


        template <bool nullable>
        void executeGeneric(
            const IColumn & src_data, const ColumnArray::Offsets & src_offsets,
            IColumn & res_data, ColumnArray::Offsets & res_offsets,
            const NullMap * src_null_map,
            NullMap * res_null_map)
        {
            size_t size = src_offsets.size();
            res_offsets.resize(size);
            res_data.reserve(src_data.size());

            if (nullable)
                res_null_map->reserve(src_null_map->size());

            ColumnArray::Offset src_prev_offset = 0;
            ColumnArray::Offset res_prev_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                if (src_offsets[i] != src_prev_offset)
                {
                    size_t size_to_write = src_offsets[i] - src_prev_offset;
                    res_data.insertRangeFrom(src_data, src_prev_offset, size_to_write);

                    if (nullable)
                    {
                        res_null_map->resize(res_prev_offset + size_to_write);
                        memcpy(&(*res_null_map)[res_prev_offset], &(*src_null_map)[src_prev_offset], size_to_write);
                    }

                    res_prev_offset += size_to_write;
                    res_offsets[i] = res_prev_offset;
                }
                else
                {
                    res_data.insertDefault();
                    ++res_prev_offset;
                    res_offsets[i] = res_prev_offset;

                    if (nullable)
                        res_null_map->push_back(1);
                }

                src_prev_offset = src_offsets[i];
            }
        }


        template <bool nullable>
        void executeDispatch(
            const IColumn & src_data, const ColumnArray::Offsets & src_array_offsets,
            IColumn & res_data_col, ColumnArray::Offsets & res_array_offsets,
            const NullMap * src_null_map,
            NullMap * res_null_map)
        {
            if (!( executeNumber<UInt8, nullable>  (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeNumber<UInt16, nullable> (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeNumber<UInt32, nullable> (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeNumber<UInt64, nullable> (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeNumber<Int8, nullable>   (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeNumber<Int16, nullable>  (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeNumber<Int32, nullable>  (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeNumber<Int64, nullable>  (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeNumber<Float32, nullable>(src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeNumber<Float64, nullable>(src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeString<nullable>         (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeFixedString<nullable>    (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)))
                executeGeneric<nullable>           (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map);
        }
    }
}

void FunctionEmptyArrayToSingle::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    if (FunctionEmptyArrayToSingleImpl::executeConst(block, arguments, result))
        return;

    const ColumnArray * array = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());
    if (!array)
        throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN);

    MutableColumnPtr res_ptr = array->cloneEmpty();
    ColumnArray & res = static_cast<ColumnArray &>(*res_ptr);

    const IColumn & src_data = array->getData();
    const ColumnArray::Offsets & src_offsets = array->getOffsets();
    IColumn & res_data = res.getData();
    ColumnArray::Offsets & res_offsets = res.getOffsets();

    const NullMap * src_null_map = nullptr;
    NullMap * res_null_map = nullptr;

    const IColumn * inner_col;
    IColumn * inner_res_col;

    bool nullable = src_data.isColumnNullable();
    if (nullable)
    {
        auto nullable_col = static_cast<const ColumnNullable *>(&src_data);
        inner_col = &nullable_col->getNestedColumn();
        src_null_map = &nullable_col->getNullMapData();

        auto nullable_res_col = static_cast<ColumnNullable *>(&res_data);
        inner_res_col = &nullable_res_col->getNestedColumn();
        res_null_map = &nullable_res_col->getNullMapData();
    }
    else
    {
        inner_col = &src_data;
        inner_res_col = &res_data;
    }

    if (nullable)
        FunctionEmptyArrayToSingleImpl::executeDispatch<true>(*inner_col, src_offsets, *inner_res_col, res_offsets, src_null_map, res_null_map);
    else
        FunctionEmptyArrayToSingleImpl::executeDispatch<false>(*inner_col, src_offsets, *inner_res_col, res_offsets, src_null_map, res_null_map);

    block.getByPosition(result).column = std::move(res_ptr);
}


/// Implementation of FunctionRange.

String FunctionRange::getName() const
{
    return name;
}

DataTypePtr FunctionRange::getReturnTypeImpl(const DataTypes & arguments) const
{
    const DataTypePtr & arg = arguments.front();

    if (!arg->isUnsignedInteger())
        throw Exception{
            "Illegal type " + arg->getName() + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

    return std::make_shared<DataTypeArray>(arg);
}

template <typename T>
bool FunctionRange::executeInternal(Block & block, const IColumn * arg, const size_t result)
{
    static constexpr size_t max_elements = 100'000'000;

    if (const auto in = checkAndGetColumn<ColumnVector<T>>(arg))
    {
        const auto & in_data = in->getData();
        const auto total_values = std::accumulate(std::begin(in_data), std::end(in_data), size_t{},
            [this] (const size_t lhs, const size_t rhs)
            {
                const auto sum = lhs + rhs;
                if (sum < lhs)
                    throw Exception{
                        "A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                        ErrorCodes::ARGUMENT_OUT_OF_BOUND};

                return sum;
            });

        if (total_values > max_elements)
            throw Exception{
                "A call to function " + getName() + " would produce " + std::to_string(total_values) +
                    " array elements, which is greater than the allowed maximum of " + std::to_string(max_elements),
                ErrorCodes::ARGUMENT_OUT_OF_BOUND};

        auto data_col = ColumnVector<T>::create(total_values);
        auto offsets_col = ColumnArray::ColumnOffsets::create(in->size());

        auto & out_data = data_col->getData();
        auto & out_offsets = offsets_col->getData();

        IColumn::Offset offset{};
        for (size_t row_idx = 0, rows = in->size(); row_idx < rows; ++row_idx)
        {
            for (size_t elem_idx = 0, elems = in_data[row_idx]; elem_idx < elems; ++elem_idx)
                out_data[offset + elem_idx] = elem_idx;

            offset += in_data[row_idx];
            out_offsets[row_idx] = offset;
        }

        block.getByPosition(result).column = ColumnArray::create(std::move(data_col), std::move(offsets_col));
        return true;
    }
    else
        return false;
}

void FunctionRange::executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result)
{
    const auto col = block.getByPosition(arguments[0]).column.get();

    if (!executeInternal<UInt8>(block, col, result) &&
        !executeInternal<UInt16>(block, col, result) &&
        !executeInternal<UInt32>(block, col, result) &&
        !executeInternal<UInt64>(block, col, result))
    {
        throw Exception{
            "Illegal column " + col->getName() + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN};
    }
}

/// Implementation of FunctionArrayReverse.

FunctionPtr FunctionArrayReverse::create(const Context &)
{
    return std::make_shared<FunctionArrayReverse>();
}

String FunctionArrayReverse::getName() const
{
    return name;
}

DataTypePtr FunctionArrayReverse::getReturnTypeImpl(const DataTypes & arguments) const
{
    const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
    if (!array_type)
        throw Exception("Argument for function " + getName() + " must be array.",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return arguments[0];
}

void FunctionArrayReverse::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    if (executeConst(block, arguments, result))
        return;

    const ColumnArray * array = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());
    if (!array)
        throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN);

    auto res_ptr = array->cloneEmpty();
    ColumnArray & res = static_cast<ColumnArray &>(*res_ptr);
    res.getOffsetsPtr() = array->getOffsetsPtr();

    const IColumn & src_data = array->getData();
    const ColumnArray::Offsets & offsets = array->getOffsets();
    IColumn & res_data = res.getData();

    const ColumnNullable * nullable_col = nullptr;
    ColumnNullable * nullable_res_col = nullptr;

    const IColumn * inner_col;
    IColumn * inner_res_col;

    if (src_data.isColumnNullable())
    {
        nullable_col = static_cast<const ColumnNullable *>(&src_data);
        inner_col = &nullable_col->getNestedColumn();

        nullable_res_col = static_cast<ColumnNullable *>(&res_data);
        inner_res_col = &nullable_res_col->getNestedColumn();
    }
    else
    {
        inner_col = &src_data;
        inner_res_col = &res_data;
    }

    if (!( executeNumber<UInt8>(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeNumber<UInt16>(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeNumber<UInt32>(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeNumber<UInt64>(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeNumber<Int8>(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeNumber<Int16>(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeNumber<Int32>(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeNumber<Int64>(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeNumber<Float32>(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeNumber<Float64>(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeString (*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeFixedString (*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)))
        throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
            + " of first argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN);

    block.getByPosition(result).column = std::move(res_ptr);
}

bool FunctionArrayReverse::executeConst(Block & block, const ColumnNumbers & arguments, size_t result)
{
    if (const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(block.getByPosition(arguments[0]).column.get()))
    {
        Array arr = const_array->getValue<Array>();

        size_t size = arr.size();
        Array res(size);

        for (size_t i = 0; i < size; ++i)
            res[i] = arr[size - i - 1];

        block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(block.rows(), res);

        return true;
    }
    else
        return false;
}

template <typename T>
bool FunctionArrayReverse::executeNumber(
    const IColumn & src_data, const ColumnArray::Offsets & src_offsets,
    IColumn & res_data_col,
    const ColumnNullable * nullable_col,
    ColumnNullable * nullable_res_col)
{
    auto do_reverse = [](const auto & src_data, const auto & src_offsets, auto & res_data)
    {
        size_t size = src_offsets.size();
        ColumnArray::Offset src_prev_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            const auto * src = &src_data[src_prev_offset];
            const auto * src_end = &src_data[src_offsets[i]];

            if (src == src_end)
                continue;

            auto dst = &res_data[src_offsets[i] - 1];

            while (src < src_end)
            {
                *dst = *src;
                ++src;
                --dst;
            }

            src_prev_offset = src_offsets[i];
        }
    };

    if (const ColumnVector<T> * src_data_concrete = checkAndGetColumn<ColumnVector<T>>(&src_data))
    {
        const PaddedPODArray<T> & src_data = src_data_concrete->getData();
        PaddedPODArray<T> & res_data = typeid_cast<ColumnVector<T> &>(res_data_col).getData();
        res_data.resize(src_data.size());
        do_reverse(src_data, src_offsets, res_data);

        if ((nullable_col) && (nullable_res_col))
        {
            /// Make a reverted null map.
            const auto & src_null_map = static_cast<const ColumnUInt8 &>(nullable_col->getNullMapColumn()).getData();
            auto & res_null_map = static_cast<ColumnUInt8 &>(nullable_res_col->getNullMapColumn()).getData();
            res_null_map.resize(src_data.size());
            do_reverse(src_null_map, src_offsets, res_null_map);
        }

        return true;
    }
    else
        return false;
}

bool FunctionArrayReverse::executeFixedString(
    const IColumn & src_data, const ColumnArray::Offsets & src_offsets,
    IColumn & res_data_col,
    const ColumnNullable * nullable_col,
    ColumnNullable * nullable_res_col)
{
    if (const ColumnFixedString * src_data_concrete = checkAndGetColumn<ColumnFixedString>(&src_data))
    {
        const size_t n = src_data_concrete->getN();
        const ColumnFixedString::Chars_t & src_data = src_data_concrete->getChars();
        ColumnFixedString::Chars_t & res_data = typeid_cast<ColumnFixedString &>(res_data_col).getChars();
        size_t size = src_offsets.size();
        res_data.resize(src_data.size());

        ColumnArray::Offset src_prev_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            const UInt8 * src = &src_data[src_prev_offset * n];
            const UInt8 * src_end = &src_data[src_offsets[i] * n];

            if (src == src_end)
                continue;

            UInt8 * dst = &res_data[src_offsets[i] * n - n];

            while (src < src_end)
            {
                /// NOTE: memcpySmallAllowReadWriteOverflow15 doesn't work correctly here.
                memcpy(dst, src, n);
                src += n;
                dst -= n;
            }

            src_prev_offset = src_offsets[i];
        }

        if ((nullable_col) && (nullable_res_col))
        {
            /// Make a reverted null map.
            const auto & src_null_map = static_cast<const ColumnUInt8 &>(nullable_col->getNullMapColumn()).getData();
            auto & res_null_map = static_cast<ColumnUInt8 &>(nullable_res_col->getNullMapColumn()).getData();
            res_null_map.resize(src_null_map.size());

            ColumnArray::Offset src_prev_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                const UInt8 * src = &src_null_map[src_prev_offset];
                const UInt8 * src_end = &src_null_map[src_offsets[i]];

                if (src == src_end)
                    continue;

                UInt8 * dst = &res_null_map[src_offsets[i] - 1];

                while (src < src_end)
                {
                    *dst = *src;
                    ++src;
                    --dst;
                }

                src_prev_offset = src_offsets[i];
            }
        }

        return true;
    }
    else
        return false;
}

bool FunctionArrayReverse::executeString(
    const IColumn & src_data, const ColumnArray::Offsets & src_array_offsets,
    IColumn & res_data_col,
    const ColumnNullable * nullable_col,
    ColumnNullable * nullable_res_col)
{
    if (const ColumnString * src_data_concrete = checkAndGetColumn<ColumnString>(&src_data))
    {
        const ColumnString::Offsets & src_string_offsets = src_data_concrete->getOffsets();
        ColumnString::Offsets & res_string_offsets = typeid_cast<ColumnString &>(res_data_col).getOffsets();

        const ColumnString::Chars_t & src_data = src_data_concrete->getChars();
        ColumnString::Chars_t & res_data = typeid_cast<ColumnString &>(res_data_col).getChars();

        size_t size = src_array_offsets.size();
        res_string_offsets.resize(src_string_offsets.size());
        res_data.resize(src_data.size());

        ColumnArray::Offset src_array_prev_offset = 0;
        ColumnString::Offset res_string_prev_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            if (src_array_offsets[i] != src_array_prev_offset)
            {
                size_t array_size = src_array_offsets[i] - src_array_prev_offset;

                for (size_t j = 0; j < array_size; ++j)
                {
                    size_t j_reversed = array_size - j - 1;

                    auto src_pos = src_array_prev_offset + j_reversed == 0 ? 0 : src_string_offsets[src_array_prev_offset + j_reversed - 1];
                    size_t string_size = src_string_offsets[src_array_prev_offset + j_reversed] - src_pos;

                    memcpySmallAllowReadWriteOverflow15(&res_data[res_string_prev_offset], &src_data[src_pos], string_size);

                    res_string_prev_offset += string_size;
                    res_string_offsets[src_array_prev_offset + j] = res_string_prev_offset;
                }
            }

            src_array_prev_offset = src_array_offsets[i];
        }

        if ((nullable_col) && (nullable_res_col))
        {
            /// Make a reverted null map.
            const auto & src_null_map = static_cast<const ColumnUInt8 &>(nullable_col->getNullMapColumn()).getData();
            auto & res_null_map = static_cast<ColumnUInt8 &>(nullable_res_col->getNullMapColumn()).getData();
            res_null_map.resize(src_string_offsets.size());

            size_t size = src_string_offsets.size();
            ColumnArray::Offset src_prev_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                const auto * src = &src_null_map[src_prev_offset];
                const auto * src_end = &src_null_map[src_array_offsets[i]];

                if (src == src_end)
                    continue;

                auto dst = &res_null_map[src_array_offsets[i] - 1];

                while (src < src_end)
                {
                    *dst = *src;
                    ++src;
                    --dst;
                }

                src_prev_offset = src_array_offsets[i];
            }
        }

        return true;
    }
    else
        return false;
}

/// Implementation of FunctionArrayReduce.

FunctionPtr FunctionArrayReduce::create(const Context &)
{
    return std::make_shared<FunctionArrayReduce>();
}

String FunctionArrayReduce::getName() const
{
    return name;
}

DataTypePtr FunctionArrayReduce::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    /// The first argument is a constant string with the name of the aggregate function
    ///  (possibly with parameters in parentheses, for example: "quantile(0.99)").

    if (arguments.size() < 2)
        throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
            + toString(arguments.size()) + ", should be at least 2.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const ColumnConst * aggregate_function_name_column = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
    if (!aggregate_function_name_column)
        throw Exception("First argument for function " + getName() + " must be constant string: name of aggregate function.",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    DataTypes argument_types(arguments.size() - 1);
    for (size_t i = 1, size = arguments.size(); i < size; ++i)
    {
        const DataTypeArray * arg = checkAndGetDataType<DataTypeArray>(arguments[i].type.get());
        if (!arg)
            throw Exception("Argument " + toString(i) + " for function " + getName() + " must be an array but it has type "
                + arguments[i].type->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        argument_types[i - 1] = arg->getNestedType();
    }

    if (!aggregate_function)
    {
        String aggregate_function_name_with_params = aggregate_function_name_column->getValue<String>();

        if (aggregate_function_name_with_params.empty())
            throw Exception("First argument for function " + getName() + " (name of aggregate function) cannot be empty.",
                ErrorCodes::BAD_ARGUMENTS);

        String aggregate_function_name;
        Array params_row;
        getAggregateFunctionNameAndParametersArray(aggregate_function_name_with_params,
                                                   aggregate_function_name, params_row, "function " + getName());

        aggregate_function = AggregateFunctionFactory::instance().get(aggregate_function_name, argument_types, params_row);
    }

    return aggregate_function->getReturnType();
}



void FunctionArrayReduce::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    IAggregateFunction & agg_func = *aggregate_function.get();
    std::unique_ptr<char[]> place_holder { new char[agg_func.sizeOfData()] };
    AggregateDataPtr place = place_holder.get();

    std::unique_ptr<Arena> arena = agg_func.allocatesMemoryInArena() ? std::make_unique<Arena>() : nullptr;

    size_t rows = block.rows();

    /// Aggregate functions do not support constant columns. Therefore, we materialize them.
    std::vector<ColumnPtr> materialized_columns;

    const size_t num_arguments_columns = arguments.size() - 1;

    std::vector<const IColumn *> aggregate_arguments_vec(num_arguments_columns);
    const ColumnArray::Offsets * offsets = nullptr;

    bool is_const = true;

    for (size_t i = 0; i < num_arguments_columns; ++i)
    {
        const IColumn * col = block.getByPosition(arguments[i + 1]).column.get();
        const ColumnArray::Offsets * offsets_i = nullptr;
        if (const ColumnArray * arr = checkAndGetColumn<ColumnArray>(col))
        {
            aggregate_arguments_vec[i] = &arr->getData();
            offsets_i = &arr->getOffsets();
            is_const = false;
        }
        else if (const ColumnConst * const_arr = checkAndGetColumnConst<ColumnArray>(col))
        {
            materialized_columns.emplace_back(const_arr->convertToFullColumn());
            const auto & arr = typeid_cast<const ColumnArray &>(*materialized_columns.back().get());
            aggregate_arguments_vec[i] = &arr.getData();
            offsets_i = &arr.getOffsets();
        }
        else
            throw Exception("Illegal column " + col->getName() + " as argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

        if (i == 0)
            offsets = offsets_i;
        else if (*offsets_i != *offsets)
            throw Exception("Lengths of all arrays passed to " + getName() + " must be equal.",
                ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
    }
    const IColumn ** aggregate_arguments = aggregate_arguments_vec.data();

    MutableColumnPtr result_holder = block.getByPosition(result).type->createColumn();
    IColumn & res_col = *result_holder;

    /// AggregateFunction's states should be inserted into column using specific way
    auto res_col_aggregate_function = typeid_cast<ColumnAggregateFunction *>(&res_col);

    if (!res_col_aggregate_function && agg_func.isState())
        throw Exception("State function " + agg_func.getName() + " inserts results into non-state column "
                        + block.getByPosition(result).type->getName(), ErrorCodes::ILLEGAL_COLUMN);

    ColumnArray::Offset current_offset = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        agg_func.create(place);
        ColumnArray::Offset next_offset = (*offsets)[i];

        try
        {
            for (size_t j = current_offset; j < next_offset; ++j)
                agg_func.add(place, aggregate_arguments, j, arena.get());

            if (!res_col_aggregate_function)
                agg_func.insertResultInto(place, res_col);
            else
                res_col_aggregate_function->insertFrom(place);
        }
        catch (...)
        {
            agg_func.destroy(place);
            throw;
        }

        agg_func.destroy(place);
        current_offset = next_offset;
    }

    if (!is_const)
    {
        block.getByPosition(result).column = std::move(result_holder);
    }
    else
    {
        block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(rows, res_col[0]);
    }
}

/// Implementation of FunctionArrayConcat.

FunctionPtr FunctionArrayConcat::create(const Context & context)
{
    return std::make_shared<FunctionArrayConcat>(context);
}

String FunctionArrayConcat::getName() const
{
    return name;
}

DataTypePtr FunctionArrayConcat::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments.empty())
        throw Exception{"Function " + getName() + " requires at least one argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

    for (auto i : ext::range(0, arguments.size()))
    {
        auto array_type = typeid_cast<const DataTypeArray *>(arguments[i].get());
        if (!array_type)
            throw Exception("Argument " + std::to_string(i) + " for function " + getName() + " must be an array but it has type "
                            + arguments[i]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    return getLeastSupertype(arguments);
}

void FunctionArrayConcat::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    const DataTypePtr & return_type = block.getByPosition(result).type;

    if (return_type->onlyNull())
    {
        block.getByPosition(result).column = return_type->createColumnConstWithDefaultValue(block.rows());
        return;
    }

    auto result_column = return_type->createColumn();

    size_t rows = block.rows();
    size_t num_args = arguments.size();

    Columns preprocessed_columns(num_args);

    for (size_t i = 0; i < num_args; ++i)
    {
        const ColumnWithTypeAndName & arg = block.getByPosition(arguments[i]);
        ColumnPtr preprocessed_column = arg.column;

        if (!arg.type->equals(*return_type))
            preprocessed_column = castColumn(arg, return_type, context);

        preprocessed_columns[i] = std::move(preprocessed_column);
    }

    std::vector<std::unique_ptr<GatherUtils::IArraySource>> sources;

    for (auto & argument_column : preprocessed_columns)
    {
        bool is_const = false;

        if (auto argument_column_const = typeid_cast<const ColumnConst *>(argument_column.get()))
        {
            is_const = true;
            argument_column = argument_column_const->getDataColumnPtr();
        }

        if (auto argument_column_array = typeid_cast<const ColumnArray *>(argument_column.get()))
            sources.emplace_back(GatherUtils::createArraySource(*argument_column_array, is_const, rows));
        else
            throw Exception{"Arguments for function " + getName() + " must be arrays.", ErrorCodes::LOGICAL_ERROR};
    }

    auto sink = GatherUtils::createArraySink(typeid_cast<ColumnArray &>(*result_column), rows);
    GatherUtils::concat(sources, *sink);

    block.getByPosition(result).column = std::move(result_column);
}


/// Implementation of FunctionArraySlice.

FunctionPtr FunctionArraySlice::create(const Context &)
{
    return std::make_shared<FunctionArraySlice>();
}

String FunctionArraySlice::getName() const
{
    return name;
}

DataTypePtr FunctionArraySlice::getReturnTypeImpl(const DataTypes & arguments) const
{
    size_t number_of_arguments = arguments.size();

    if (number_of_arguments < 2 || number_of_arguments > 3)
        throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                        + toString(number_of_arguments) + ", should be 2 or 3",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (arguments[0]->onlyNull())
        return arguments[0];

    auto array_type = typeid_cast<const DataTypeArray *>(arguments[0].get());
    if (!array_type)
        throw Exception("First argument for function " + getName() + " must be an array but it has type "
                        + arguments[0]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    for (size_t i = 1; i < number_of_arguments; ++i)
    {
        if (!removeNullable(arguments[i])->isInteger() && !arguments[i]->onlyNull())
            throw Exception(
                    "Argument " + toString(i) + " for function " + getName() + " must be integer but it has type "
                    + arguments[i]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    return arguments[0];
}

void FunctionArraySlice::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    const auto & return_type = block.getByPosition(result).type;

    if (return_type->onlyNull())
    {
        block.getByPosition(result).column = return_type->createColumnConstWithDefaultValue(block.rows());
        return;
    }

    auto result_column = return_type->createColumn();

    auto & array_column = block.getByPosition(arguments[0]).column;
    const auto & offset_column = block.getByPosition(arguments[1]).column;
    const auto & length_column = arguments.size() > 2 ? block.getByPosition(arguments[2]).column : nullptr;

    std::unique_ptr<GatherUtils::IArraySource> source;

    size_t size = array_column->size();
    bool is_const = false;

    if (auto const_array_column = typeid_cast<const ColumnConst *>(array_column.get()))
    {
        is_const = true;
        array_column = const_array_column->getDataColumnPtr();
    }

    if (auto argument_column_array = typeid_cast<const ColumnArray *>(array_column.get()))
        source = GatherUtils::createArraySource(*argument_column_array, is_const, size);
    else
        throw Exception{"First arguments for function " + getName() + " must be array.", ErrorCodes::LOGICAL_ERROR};

    auto sink = GatherUtils::createArraySink(typeid_cast<ColumnArray &>(*result_column), size);

    if (offset_column->onlyNull())
    {
        if (!length_column || length_column->onlyNull())
        {
            block.getByPosition(result).column = array_column;
            return;
        }
        else if (length_column->isColumnConst())
            GatherUtils::sliceFromLeftConstantOffsetBounded(*source, *sink, 0, length_column->getInt(0));
        else
        {
            auto const_offset_column = ColumnConst::create(ColumnInt8::create(1, 1), size);
            GatherUtils::sliceDynamicOffsetBounded(*source, *sink, *const_offset_column, *length_column);
        }
    }
    else if (offset_column->isColumnConst())
    {
        ssize_t offset = offset_column->getUInt(0);

        if (!length_column || length_column->onlyNull())
        {
            if (offset > 0)
                GatherUtils::sliceFromLeftConstantOffsetUnbounded(*source, *sink, static_cast<size_t>(offset - 1));
            else
                GatherUtils::sliceFromRightConstantOffsetUnbounded(*source, *sink, static_cast<size_t>(-offset));
        }
        else if (length_column->isColumnConst())
        {
            ssize_t length = length_column->getInt(0);
            if (offset > 0)
                GatherUtils::sliceFromLeftConstantOffsetBounded(*source, *sink, static_cast<size_t>(offset - 1), length);
            else
                GatherUtils::sliceFromRightConstantOffsetBounded(*source, *sink, static_cast<size_t>(-offset), length);
        }
        else
            GatherUtils::sliceDynamicOffsetBounded(*source, *sink, *offset_column, *length_column);
    }
    else
    {
        if (!length_column || length_column->onlyNull())
            GatherUtils::sliceDynamicOffsetUnbounded(*source, *sink, *offset_column);
        else
            GatherUtils::sliceDynamicOffsetBounded(*source, *sink, *offset_column, *length_column);
    }

    block.getByPosition(result).column = std::move(result_column);
}


/// Implementation of FunctionArrayPush.

DataTypePtr FunctionArrayPush::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments[0]->onlyNull())
        return arguments[0];

    auto array_type = typeid_cast<const DataTypeArray *>(arguments[0].get());
    if (!array_type)
        throw Exception("First argument for function " + getName() + " must be an array but it has type "
                        + arguments[0]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    auto nested_type = array_type->getNestedType();

    DataTypes types = {nested_type, arguments[1]};

    return std::make_shared<DataTypeArray>(getLeastSupertype(types));
}

void FunctionArrayPush::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    const auto & return_type = block.getByPosition(result).type;

    if (return_type->onlyNull())
    {
        block.getByPosition(result).column = return_type->createColumnConstWithDefaultValue(block.rows());
        return;
    }

    auto result_column = return_type->createColumn();

    auto array_column = block.getByPosition(arguments[0]).column;
    auto appended_column = block.getByPosition(arguments[1]).column;

    if (!block.getByPosition(arguments[0]).type->equals(*return_type))
        array_column = castColumn(block.getByPosition(arguments[0]), return_type, context);

    const DataTypePtr & return_nested_type = typeid_cast<const DataTypeArray &>(*return_type).getNestedType();
    if (!block.getByPosition(arguments[1]).type->equals(*return_nested_type))
        appended_column = castColumn(block.getByPosition(arguments[1]), return_nested_type, context);

    std::unique_ptr<GatherUtils::IArraySource> array_source;
    std::unique_ptr<GatherUtils::IValueSource> value_source;

    size_t size = array_column->size();
    bool is_const = false;

    if (auto const_array_column = typeid_cast<const ColumnConst *>(array_column.get()))
    {
        is_const = true;
        array_column = const_array_column->getDataColumnPtr();
    }

    if (auto argument_column_array = typeid_cast<const ColumnArray *>(array_column.get()))
        array_source = GatherUtils::createArraySource(*argument_column_array, is_const, size);
    else
        throw Exception{"First arguments for function " + getName() + " must be array.", ErrorCodes::LOGICAL_ERROR};


    bool is_appended_const = false;
    if (auto const_appended_column = typeid_cast<const ColumnConst *>(appended_column.get()))
    {
        is_appended_const = true;
        appended_column = const_appended_column->getDataColumnPtr();
    }

    value_source = GatherUtils::createValueSource(*appended_column, is_appended_const, size);

    auto sink = GatherUtils::createArraySink(typeid_cast<ColumnArray &>(*result_column), size);

    GatherUtils::push(*array_source, *value_source, *sink, push_front);

    block.getByPosition(result).column = std::move(result_column);
}

/// Implementation of FunctionArrayPushFront.

FunctionPtr FunctionArrayPushFront::create(const Context & context)
{
    return std::make_shared<FunctionArrayPushFront>(context);
}

/// Implementation of FunctionArrayPushBack.

FunctionPtr FunctionArrayPushBack::create(const Context & context)
{
    return std::make_shared<FunctionArrayPushBack>(context);
}

/// Implementation of FunctionArrayPop.

DataTypePtr FunctionArrayPop::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments[0]->onlyNull())
        return arguments[0];

    auto array_type = typeid_cast<const DataTypeArray *>(arguments[0].get());
    if (!array_type)
        throw Exception("First argument for function " + getName() + " must be an array but it has type "
                        + arguments[0]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return arguments[0];
}

void FunctionArrayPop::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    const auto & return_type = block.getByPosition(result).type;

    if (return_type->onlyNull())
    {
        block.getByPosition(result).column = return_type->createColumnConstWithDefaultValue(block.rows());
        return;
    }

    auto result_column = return_type->createColumn();

    const auto & array_column = block.getByPosition(arguments[0]).column;

    std::unique_ptr<GatherUtils::IArraySource> source;

    size_t size = array_column->size();

    if (auto argument_column_array = typeid_cast<const ColumnArray *>(array_column.get()))
        source = GatherUtils::createArraySource(*argument_column_array, false, size);
    else
        throw Exception{"First arguments for function " + getName() + " must be array.", ErrorCodes::LOGICAL_ERROR};

    auto sink = GatherUtils::createArraySink(typeid_cast<ColumnArray &>(*result_column), size);

    if (pop_front)
        GatherUtils::sliceFromLeftConstantOffsetUnbounded(*source, *sink, 1);
    else
        GatherUtils::sliceFromLeftConstantOffsetBounded(*source, *sink, 0, -1);

    block.getByPosition(result).column = std::move(result_column);
}

/// Implementation of FunctionArrayPopFront.

FunctionPtr FunctionArrayPopFront::create(const Context &)
{
    return std::make_shared<FunctionArrayPopFront>();
}

/// Implementation of FunctionArrayPopBack.

FunctionPtr FunctionArrayPopBack::create(const Context &)
{
    return std::make_shared<FunctionArrayPopBack>();
}


/// Implementation of FunctionArrayAllAny.

FunctionPtr FunctionArrayHasAll::create(const Context & context)
{
    return std::make_shared<FunctionArrayHasAll>(context);
}

FunctionPtr FunctionArrayHasAny::create(const Context & context)
{
    return std::make_shared<FunctionArrayHasAny>(context);
}


DataTypePtr FunctionArrayHasAllAny::getReturnTypeImpl(const DataTypes & arguments) const
{
    for (auto i : ext::range(0, arguments.size()))
    {
        auto array_type = typeid_cast<const DataTypeArray *>(arguments[i].get());
        if (!array_type)
            throw Exception("Argument " + std::to_string(i) + " for function " + getName() + " must be an array but it has type "
                            + arguments[i]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    return std::make_shared<DataTypeUInt8>();
}

void FunctionArrayHasAllAny::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    size_t rows = block.rows();
    size_t num_args = arguments.size();

    auto result_column = ColumnUInt8::create(rows);

    DataTypePtr common_type = nullptr;
    auto commonType = [& common_type, & block, & arguments]()
    {
        if (common_type == nullptr)
        {
            DataTypes data_types;
            data_types.reserve(arguments.size());
            for (const auto & argument : arguments)
                data_types.push_back(block.getByPosition(argument).type);

            common_type = getLeastSupertype(data_types);
        }

        return common_type;
    };

    Columns preprocessed_columns(num_args);

    for (size_t i = 0; i < num_args; ++i)
    {
        const auto & argument = block.getByPosition(arguments[i]);
        ColumnPtr preprocessed_column = argument.column;

        const auto argument_type = typeid_cast<const DataTypeArray *>(argument.type.get());
        const auto & nested_type = argument_type->getNestedType();

        /// Converts Array(Nothing) or Array(Nullable(Nothing) to common type. Example: hasAll([Null, 1], [Null]) -> 1
        if (typeid_cast<const DataTypeNothing *>(removeNullable(nested_type).get()))
            preprocessed_column = castColumn(argument, commonType(), context);

        preprocessed_columns[i] = std::move(preprocessed_column);
    }

    std::vector<std::unique_ptr<GatherUtils::IArraySource>> sources;

    for (auto & argument_column : preprocessed_columns)
    {
        bool is_const = false;

        if (auto argument_column_const = typeid_cast<const ColumnConst *>(argument_column.get()))
        {
            is_const = true;
            argument_column = argument_column_const->getDataColumnPtr();
        }

        if (auto argument_column_array = typeid_cast<const ColumnArray *>(argument_column.get()))
            sources.emplace_back(GatherUtils::createArraySource(*argument_column_array, is_const, rows));
        else
            throw Exception{"Arguments for function " + getName() + " must be arrays.", ErrorCodes::LOGICAL_ERROR};
    }

    auto result_column_ptr = typeid_cast<ColumnUInt8 *>(result_column.get());
    GatherUtils::sliceHas(*sources[0], *sources[1], all, *result_column_ptr);

    block.getByPosition(result).column = std::move(result_column);
}

/// Implementation of FunctionArrayIntersect.

FunctionPtr FunctionArrayIntersect::create(const Context & context)
{
    return std::make_shared<FunctionArrayIntersect>(context);
}

String FunctionArrayIntersect::getName() const
{
    return name;
}

DataTypePtr FunctionArrayIntersect::getReturnTypeImpl(const DataTypes & arguments) const
{
    DataTypes nested_types;
    nested_types.reserve(arguments.size());

    bool has_nothing = false;

    if (arguments.empty())
        throw Exception{"Function " + getName() + " requires at least one argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

    for (auto i : ext::range(0, arguments.size()))
    {
        auto array_type = typeid_cast<const DataTypeArray *>(arguments[i].get());
        if (!array_type)
            throw Exception("Argument " + std::to_string(i) + " for function " + getName() + " must be an array but it has type "
                            + arguments[i]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const auto & nested_type = array_type->getNestedType();

        if (typeid_cast<const DataTypeNothing *>(nested_type.get()))
            has_nothing = true;
        else
            nested_types.push_back(nested_type);
    }

    DataTypePtr result_type;

    if (!nested_types.empty())
        result_type = getMostSubtype(nested_types, true);

    if (has_nothing)
        result_type = std::make_shared<DataTypeNothing>();

    return std::make_shared<DataTypeArray>(result_type);
}

ColumnPtr FunctionArrayIntersect::castRemoveNullable(const ColumnPtr & column, const DataTypePtr & data_type) const
{
    if (auto column_nullable = checkAndGetColumn<ColumnNullable>(column.get()))
    {
        auto nullable_type = checkAndGetDataType<DataTypeNullable>(data_type.get());
        const auto & nested = column_nullable->getNestedColumnPtr();
        if (nullable_type)
        {
            auto casted_column = castRemoveNullable(nested, nullable_type->getNestedType());
            return ColumnNullable::create(casted_column, column_nullable->getNullMapColumnPtr());
        }
        return castRemoveNullable(nested, data_type);
    }
    else if (auto column_array = checkAndGetColumn<ColumnArray>(column.get()))
    {
        auto array_type = checkAndGetDataType<DataTypeArray>(data_type.get());
        if (!array_type)
            throw Exception{"Cannot cast array column to column with type "
                            + data_type->getName() + " in function " + getName(), ErrorCodes::LOGICAL_ERROR};

        auto casted_column = castRemoveNullable(column_array->getDataPtr(), array_type->getNestedType());
        return ColumnArray::create(casted_column, column_array->getOffsetsPtr());
    }
    else if (auto column_tuple = checkAndGetColumn<ColumnTuple>(column.get()))
    {
        auto tuple_type = checkAndGetDataType<DataTypeTuple>(data_type.get());

        if (!tuple_type)
            throw Exception{"Cannot cast tuple column to type "
                            + data_type->getName() + " in function " + getName(), ErrorCodes::LOGICAL_ERROR};

        auto columns_number = column_tuple->getColumns().size();
        Columns columns(columns_number);

        const auto & types = tuple_type->getElements();

        for (auto i : ext::range(0, columns_number))
        {
            columns[i] = castRemoveNullable(column_tuple->getColumnPtr(i), types[i]);
        }
        return ColumnTuple::create(columns);
    }

    return column;
}

Columns FunctionArrayIntersect::castColumns(
        Block & block, const ColumnNumbers & arguments, const DataTypePtr & return_type,
        const DataTypePtr & return_type_with_nulls) const
{
    size_t num_args = arguments.size();
    Columns columns(num_args);

    auto type_array = checkAndGetDataType<DataTypeArray>(return_type.get());
    auto & type_nested = type_array->getNestedType();
    auto type_not_nullable_nested = removeNullable(type_nested);

    const bool is_numeric_or_string = type_not_nullable_nested->isNumber()
                                      || type_not_nullable_nested->isDateOrDateTime()
                                      || type_not_nullable_nested->isStringOrFixedString();

    DataTypePtr nullable_return_type;

    if (is_numeric_or_string)
    {
        auto type_nullable_nested = makeNullable(type_nested);
        nullable_return_type = std::make_shared<DataTypeArray>(type_nullable_nested);
    }

    const bool nested_is_nullable = type_nested->isNullable();

    for (size_t i = 0; i < num_args; ++i)
    {
        const ColumnWithTypeAndName & arg = block.getByPosition(arguments[i]);
        auto & column = columns[i];

        if (is_numeric_or_string)
        {
            /// Cast to Array(T) or Array(Nullable(T)).
            if (nested_is_nullable)
            {
                if (arg.type->equals(*return_type))
                    column = arg.column;
                else
                    column = castColumn(arg, return_type, context);
            }
            else
            {
                /// If result has array type Array(T) still cast Array(Nullable(U)) to Array(Nullable(T))
                ///  because cannot cast Nullable(T) to T.
                if (arg.type->equals(*return_type) || arg.type->equals(*nullable_return_type))
                    column = arg.column;
                else if (static_cast<const DataTypeArray &>(*arg.type).getNestedType()->isNullable())
                    column = castColumn(arg, nullable_return_type, context);
                else
                    column = castColumn(arg, return_type, context);
            }
        }
        else
        {
            /// return_type_with_nulls is the most common subtype with possible nullable parts.
            if (arg.type->equals(*return_type_with_nulls))
                column = arg.column;
            else
                column = castColumn(arg, return_type_with_nulls, context);
        }
    }

    return columns;
}

FunctionArrayIntersect::UnpackedArrays FunctionArrayIntersect::prepareArrays(const Columns & columns) const
{
    UnpackedArrays arrays;

    size_t columns_number = columns.size();
    arrays.is_const.assign(columns_number, false);
    arrays.null_maps.resize(columns_number);
    arrays.offsets.resize(columns_number);
    arrays.nested_columns.resize(columns_number);

    for (auto i : ext::range(0, columns_number))
    {
        auto argument_column = columns[i].get();
        if (auto argument_column_const = typeid_cast<const ColumnConst *>(argument_column))
        {
            arrays.is_const[i] = true;
            argument_column = argument_column_const->getDataColumnPtr().get();
        }

        if (auto argument_column_array = typeid_cast<const ColumnArray *>(argument_column))
        {
            arrays.offsets[i] = &argument_column_array->getOffsets();
            arrays.nested_columns[i] = &argument_column_array->getData();
            if (auto column_nullable = typeid_cast<const ColumnNullable *>(arrays.nested_columns[i]))
            {
                arrays.null_maps[i] = &column_nullable->getNullMapData();
                arrays.nested_columns[i] = &column_nullable->getNestedColumn();
            }
        }
        else
            throw Exception{"Arguments for function " + getName() + " must be arrays.", ErrorCodes::LOGICAL_ERROR};
    }

    return arrays;
}

void FunctionArrayIntersect::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    const auto & return_type = block.getByPosition(result).type;
    auto return_type_array = checkAndGetDataType<DataTypeArray>(return_type.get());

    if (!return_type)
        throw Exception{"Return type for function " + getName() + " must be array.", ErrorCodes::LOGICAL_ERROR};

    const auto & nested_return_type = return_type_array->getNestedType();

    if (typeid_cast<const DataTypeNothing *>(nested_return_type.get()))
    {
        block.getByPosition(result).column = return_type->createColumnConstWithDefaultValue(block.rows());
        return;
    }

    auto num_args = arguments.size();
    DataTypes data_types;
    data_types.reserve(num_args);
    for (size_t i = 0; i < num_args; ++i)
        data_types.push_back(block.getByPosition(arguments[i]).type);

    auto return_type_with_nulls = getMostSubtype(data_types, true, true);

    Columns columns = castColumns(block, arguments, return_type, return_type_with_nulls);

    UnpackedArrays arrays = prepareArrays(columns);

    ColumnPtr result_column;
    auto not_nullable_nested_return_type = removeNullable(nested_return_type);
    TypeListNumbers::forEach(NumberExecutor(arrays, not_nullable_nested_return_type, result_column));

    using DateMap = ClearableHashMap<DataTypeDate::FieldType, size_t, DefaultHash<DataTypeDate::FieldType>,
            HashTableGrower<INITIAL_SIZE_DEGREE>,
            HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(DataTypeDate::FieldType)>>;

    using DateTimeMap = ClearableHashMap<DataTypeDateTime::FieldType, size_t, DefaultHash<DataTypeDateTime::FieldType>,
            HashTableGrower<INITIAL_SIZE_DEGREE>,
            HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(DataTypeDateTime::FieldType)>>;

    using StringMap = ClearableHashMap<StringRef, size_t, StringRefHash, HashTableGrower<INITIAL_SIZE_DEGREE>,
            HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(StringRef)>>;

    if (!result_column)
    {
        auto column = not_nullable_nested_return_type->createColumn();

        if (checkDataType<DataTypeDate>(not_nullable_nested_return_type.get()))
            result_column = execute<DateMap, ColumnVector<DataTypeDate::FieldType>, true>(arrays, std::move(column));
        else if (checkDataType<DataTypeDateTime>(not_nullable_nested_return_type.get()))
            result_column = execute<DateTimeMap, ColumnVector<DataTypeDateTime::FieldType>, true>(arrays, std::move(column));
        else if(not_nullable_nested_return_type->isString())
            result_column = execute<StringMap, ColumnString, false>(arrays, std::move(column));
        else if(not_nullable_nested_return_type->isFixedString())
            result_column = execute<StringMap, ColumnFixedString, false>(arrays, std::move(column));
        else
        {
            column = static_cast<const DataTypeArray &>(*return_type_with_nulls).getNestedType()->createColumn();
            result_column = castRemoveNullable(execute<StringMap, IColumn, false>(arrays, std::move(column)), return_type);
        }
    }

    block.getByPosition(result).column = std::move(result_column);
}

template <typename T, size_t>
void FunctionArrayIntersect::NumberExecutor::operator()()
{
    using Map = ClearableHashMap<T, size_t, DefaultHash<T>, HashTableGrower<INITIAL_SIZE_DEGREE>,
            HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(T)>>;

    if (!result && typeid_cast<const DataTypeNumber<T> *>(data_type.get()))
        result = execute<Map, ColumnVector<T>, true>(arrays, ColumnVector<T>::create());
};

template <typename Map, typename ColumnType, bool is_numeric_column>
ColumnPtr FunctionArrayIntersect::execute(const UnpackedArrays & arrays, MutableColumnPtr result_data_ptr)
{
    auto args = arrays.nested_columns.size();
    auto rows = arrays.offsets.front()->size();

    bool all_nullable = true;

    std::vector<const ColumnType *> columns;
    columns.reserve(args);
    for (auto arg : ext::range(0, args))
    {
        if constexpr (std::is_same<ColumnType, IColumn>::value)
            columns.push_back(arrays.nested_columns[arg]);
        else
            columns.push_back(checkAndGetColumn<ColumnType>(arrays.nested_columns[arg]));

        if (!columns.back())
            throw Exception("Unexpected array type for function arrayIntersect", ErrorCodes::LOGICAL_ERROR);

        if (!arrays.null_maps[arg])
            all_nullable = false;
    }

    auto & result_data = static_cast<ColumnType &>(*result_data_ptr);
    auto result_offsets_ptr = ColumnArray::ColumnOffsets::create(rows);
    auto & result_offsets = static_cast<ColumnArray::ColumnOffsets &>(*result_offsets_ptr);
    auto null_map_column = ColumnUInt8::create();
    NullMap & null_map = static_cast<ColumnUInt8 &>(*null_map_column).getData();

    Arena arena;

    Map map;
    std::vector<size_t> prev_off(args, 0);
    size_t result_offset = 0;
    for (auto row : ext::range(0, rows))
    {
        map.clear();

        bool all_has_nullable = all_nullable;

        for (auto arg : ext::range(0, args))
        {
            bool current_has_nullable = false;
            size_t off = (*arrays.offsets[arg])[row];
            for (auto i : ext::range(prev_off[arg], off))
            {
                if (arrays.null_maps[arg] && (*arrays.null_maps[arg])[i])
                    current_has_nullable = true;
                else
                {
                    if constexpr (is_numeric_column)
                        ++map[columns[arg]->getElement(i)];
                    else if constexpr (std::is_same<ColumnType, ColumnString>::value || std::is_same<ColumnType, ColumnFixedString>::value)
                        ++map[columns[arg]->getDataAt(i)];
                    else
                    {
                        const char * data = nullptr;
                        ++map[columns[arg]->serializeValueIntoArena(i, arena, data)];
                    }
                }
            }

            prev_off[arg] = off;
            if (!current_has_nullable)
                all_has_nullable = false;
        }

        if (all_has_nullable)
        {
            ++result_offset;
            result_data.insertDefault();
            null_map.push_back(1);
        }

        for (const auto & pair : map)
        {
            if (pair.second == args)
            {
                ++result_offset;
                if constexpr (is_numeric_column)
                    result_data.insert(pair.first);
                else if constexpr (std::is_same<ColumnType, ColumnString>::value || std::is_same<ColumnType, ColumnFixedString>::value)
                    result_data.insertData(pair.first.data, pair.first.size);
                else
                    result_data.deserializeAndInsertFromArena(pair.first.data);

                if (all_nullable)
                    null_map.push_back(0);
            }
        }
        result_offsets.getElement(row) = result_offset;
    }

    ColumnPtr result_column = std::move(result_data_ptr);
    if (all_nullable)
        result_column = ColumnNullable::create(result_column, std::move(null_map_column));
    return ColumnArray::create(result_column, std::move(result_offsets_ptr));
}

/// Implementation of FunctionArrayResize.

FunctionPtr FunctionArrayResize::create(const Context & context)
{
    return std::make_shared<FunctionArrayResize>(context);
}

String FunctionArrayResize::getName() const
{
    return name;
}

DataTypePtr FunctionArrayResize::getReturnTypeImpl(const DataTypes & arguments) const
{
    size_t number_of_arguments = arguments.size();

    if (number_of_arguments < 2 || number_of_arguments > 3)
        throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                        + toString(number_of_arguments) + ", should be 2 or 3",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (arguments[0]->onlyNull())
        return arguments[0];

    auto array_type = typeid_cast<const DataTypeArray *>(arguments[0].get());
    if (!array_type)
        throw Exception("First argument for function " + getName() + " must be an array but it has type "
                        + arguments[0]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    if (checkDataType<DataTypeNothing>(array_type->getNestedType().get()))
        throw Exception("Function " + getName() + " cannot resize " + array_type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    if (!removeNullable(arguments[1])->isInteger() && !arguments[1]->onlyNull())
        throw Exception(
                "Argument " + toString(1) + " for function " + getName() + " must be integer but it has type "
                + arguments[1]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    if (number_of_arguments)
        return arguments[0];
    else
        return std::make_shared<DataTypeArray>(getLeastSupertype({array_type->getNestedType(), arguments[2]}));
}

void FunctionArrayResize::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    const auto & return_type = block.getByPosition(result).type;

    if (return_type->onlyNull())
    {
        block.getByPosition(result).column = return_type->createColumnConstWithDefaultValue(block.rows());
        return;
    }

    auto result_column = return_type->createColumn();

    auto array_column = block.getByPosition(arguments[0]).column;
    auto size_column = block.getByPosition(arguments[1]).column;

    if (!block.getByPosition(arguments[0]).type->equals(*return_type))
        array_column = castColumn(block.getByPosition(arguments[0]), return_type, context);

    const DataTypePtr & return_nested_type = typeid_cast<const DataTypeArray &>(*return_type).getNestedType();
    size_t size = array_column->size();

    ColumnPtr appended_column;
    if (arguments.size() == 3)
    {
        appended_column = block.getByPosition(arguments[2]).column;
        if (!block.getByPosition(arguments[2]).type->equals(*return_nested_type))
            appended_column = castColumn(block.getByPosition(arguments[2]), return_nested_type, context);
    }
    else
        appended_column = return_nested_type->createColumnConstWithDefaultValue(size);

    std::unique_ptr<GatherUtils::IArraySource> array_source;
    std::unique_ptr<GatherUtils::IValueSource> value_source;

    bool is_const = false;

    if (auto const_array_column = typeid_cast<const ColumnConst *>(array_column.get()))
    {
        is_const = true;
        array_column = const_array_column->getDataColumnPtr();
    }

    if (auto argument_column_array = typeid_cast<const ColumnArray *>(array_column.get()))
        array_source = GatherUtils::createArraySource(*argument_column_array, is_const, size);
    else
        throw Exception{"First arguments for function " + getName() + " must be array.", ErrorCodes::LOGICAL_ERROR};


    bool is_appended_const = false;
    if (auto const_appended_column = typeid_cast<const ColumnConst *>(appended_column.get()))
    {
        is_appended_const = true;
        appended_column = const_appended_column->getDataColumnPtr();
    }

    value_source = GatherUtils::createValueSource(*appended_column, is_appended_const, size);

    auto sink = GatherUtils::createArraySink(typeid_cast<ColumnArray &>(*result_column), size);

    if (size_column->isColumnConst())
        GatherUtils::resizeConstantSize(*array_source, *value_source, *sink, size_column->getInt(0));
    else
        GatherUtils::resizeDynamicSize(*array_source, *value_source, *sink, *size_column);

    block.getByPosition(result).column = std::move(result_column);
}


}
