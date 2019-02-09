#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Interpreters/AggregationCommon.h>
#include <Common/HashTable/ClearableHashMap.h>

#include <Core/iostream_debug_helpers.cpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
}

class FunctionArrayEnumerateUniq;
class FunctionArrayEnumerateDense;

using DepthType = uint32_t;
using DepthTypes = std::vector<DepthType>;


std::tuple<DepthType, DepthTypes, DepthType> getDepths(/*Block & block,*/ const ColumnsWithTypeAndName & arguments)
{
    const size_t num_arguments = arguments.size();
    //ColumnRawPtrs data_columns; //(num_arguments);

    //Columns array_holders;
    //ColumnPtr offsets_column;
    DepthType depth = 0; //1;
    DepthType max_array_depth = 0; //1;
    //std::vector<DepthType> depths;
    DepthTypes depths;

//DUMP(block);
    //bool want_array = 0;

    for (size_t i = 0; i < num_arguments; ++i)
    {
        //const ColumnPtr & array_ptr = block.getByPosition(arguments[i]).column;
        const ColumnPtr & array_ptr = arguments[i].column;
//DUMP(array_ptr);
        //const ColumnArray * array = checkAndGetColumn<ColumnArray>(array_ptr.get());

        const auto array = checkAndGetColumnConst<ColumnArray>(array_ptr.get());
        //auto array = typeid_cast<const ColumnArray *>(array_ptr.get());

//DUMP(array);

//DUMP(i, arguments[i], !!array);

        if (!array)
        {

            //const auto & depth_column = block.getByPosition(arguments[i]).column;
            const auto & depth_column = arguments[i].column;

//DUMP(depth_column->isColumnConst(), depth_column->getUInt(0));
DUMP(depth_column->isColumnConst());
            if (depth_column->isColumnConst())
            {
                auto value = depth_column->getUInt(0);
DUMP(value);
                 if (i == 0)
                     depth = value;
                 else  {
                     depths.emplace_back(value);
                     if (max_array_depth < value)
                        max_array_depth = value;
                    }
                 // TODO: auto

            }
        }
    }
    return std::make_tuple(depth, depths, max_array_depth);
}



template <typename Derived>
class FunctionArrayEnumerateRankedExtended : public IFunction
{
public:
    static FunctionPtr create(const Context &) { return std::make_shared<Derived>(); }

    String getName() const override { return Derived::name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    //DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
DUMP(arguments);

DUMP(getDepths(arguments));

    DepthType depth; //1;
    DepthType max_array_depth; //1;
    //std::vector<DepthType> depths;
    DepthTypes depths;

    std::tie(depth, depths, max_array_depth) = getDepths(arguments);

        if (arguments.size() == 0)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be at least 1.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);


//auto dep = getDepths();

/*
        DepthType max_depth = 0;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            DepthType depth = 0;
            //const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].get());
            const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].type.get());
            if (!array_type)
                continue;
++depth;
DUMP(i, "Array", array_type->getName());
                if (const auto * array2_type = checkAndGetDataType<DataTypeArray>(array_type->getNestedType().get())) {
++depth;

                    DUMP(i, "Array2", array2_type->getName(), depth);
                }
            if (max_depth < depth)
                max_depth = depth;

                //if (const auto type_array = typeid_cast<const DataTypeArray *>(type.get()))
            // throw Exception("All arguments for function " + getName() + " must be arrays but argument " + toString(i + 1) + " has type " + arguments[i]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
DUMP(max_depth);
*/
        DataTypePtr type = std::make_shared<DataTypeUInt32>();
        for (DepthType i = 0; i < max_array_depth; ++i) {
            type = std::make_shared<DataTypeArray>(type);
        }
DUMP(type);

        return type; //std::make_shared<DataTypeArray>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;

private:
    /// Initially allocate a piece of memory for 512 elements. NOTE: This is just a guess.
    static constexpr size_t INITIAL_SIZE_DEGREE = 9;

    /*
    template <typename T>
    bool executeNumber(const ColumnArray::Offsets & offsets, const IColumn & data, const NullMap * null_map, ColumnUInt32::Container & res_values);
    bool executeString(const ColumnArray::Offsets & offsets, const IColumn & data, const NullMap * null_map, ColumnUInt32::Container & res_values);
    bool execute128bit(const ColumnArray::Offsets & offsets, const ColumnRawPtrs & columns, ColumnUInt32::Container & res_values);*/
    //bool executeHashed(const ColumnArray::Offsets & offsets, const ColumnRawPtrs & columns, DepthType depth, std::vector<DepthType> depths,  ColumnUInt32::Container & res_values);
    bool executeHashed(const ColumnArray::Offsets & offsets, const ColumnRawPtrs & columns, DepthType depth, DepthTypes depths, ColumnUInt32::Container & res_values);
};




template <typename Derived>
void FunctionArrayEnumerateRankedExtended<Derived>::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
{
    const ColumnArray::Offsets * offsets = nullptr;
    const size_t num_arguments = arguments.size();
    ColumnRawPtrs data_columns; //(num_arguments);

    Columns array_holders;
    ColumnPtr offsets_column;
    DepthType depth = 0; //1;
    //std::vector<DepthType> depths;
    DepthTypes depths;

DUMP(block);
    //bool want_array = 0;
    for (size_t i = 0; i < num_arguments; ++i)
    {
        const ColumnPtr & array_ptr = block.getByPosition(arguments[i]).column;
        const ColumnArray * array = checkAndGetColumn<ColumnArray>(array_ptr.get());

DUMP(i, arguments[i], !!array);

        if (!array)
        {

            const auto & depth_column = block.getByPosition(arguments[i]).column;

DUMP(depth_column->isColumnConst(), depth_column->getUInt(0));
            //if (depth_column->isColumnConst())
            {
                auto value = depth_column->getUInt(0);
DUMP(value);
                 if (i == 0)
                     depth = value;
                 else 
                     depths.emplace_back(value);
                 // TODO: auto

            }

/*

            //auto column_depth = checkAndGetColumn<ColumnUInt8>(array_ptr.get());
            auto column_depth = checkAndGetColumnConst<ColumnUInt8>(array_ptr.get()); // WRONG TODO

            //if (!column_depth)
            //    throw Exception("Unexpected type of depth column", ErrorCodes::ILLEGAL_COLUMN);

           if (column_depth) {

            if (auto value = column_depth->getValue<UInt16>())
            {
                 DUMP(value);
                 if (i == 0)
                     depth = value;
                 else 
                     depths.emplace_back(value);

            }
           }
*/
        }


        if (!array)
        {
            const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(
                block.getByPosition(arguments[i]).column.get());

DUMP(const_array);
            if (!const_array)
{
                continue;
}
/*                throw Exception("Illegal column " + block.getByPosition(arguments[i]).column->getName()
                    + " of " + toString(i + 1) + "-th argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
*/

            array_holders.emplace_back(const_array->convertToFullColumn());
            array = checkAndGetColumn<ColumnArray>(array_holders.back().get());
        }

        if ( array) {
        const ColumnArray::Offsets & offsets_i = array->getOffsets();

        if (!offsets_column && array)
        {
            offsets = &offsets_i;
            offsets_column = array->getOffsetsPtr();
        }
        else if (offsets_i != *offsets)
            throw Exception("Lengths of all arrays passed to " + getName() + " must be equal.",
                ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);


        auto * array_data = &array->getData();
        //data_columns[i] = array_data;
        data_columns.emplace_back(array_data);
        }
    }


    DUMP(depth, depths);


    //const NullMap * null_map = nullptr;

    for (size_t i = 0; i < data_columns.size() /*num_arguments*/; ++i)
    {
        if (data_columns[i]->isColumnNullable())
        {
            const auto & nullable_col = static_cast<const ColumnNullable &>(*data_columns[i]);

            if (num_arguments == 1)
                data_columns[i] = &nullable_col.getNestedColumn();

            //null_map = &nullable_col.getNullMapData();
            break;
        }
    }

    auto res_nested = ColumnUInt32::create();

    ColumnUInt32::Container & res_values = res_nested->getData();
    if (!offsets->empty())
        res_values.resize(offsets->back());

    if (num_arguments == 1)
    {
        /*executeNumber<UInt8>(*offsets, *data_columns[0], null_map, res_values)
            || executeNumber<UInt16>(*offsets, *data_columns[0], null_map, res_values)
            || executeNumber<UInt32>(*offsets, *data_columns[0], null_map, res_values)
            || executeNumber<UInt64>(*offsets, *data_columns[0], null_map, res_values)
            || executeNumber<Int8>(*offsets, *data_columns[0], null_map, res_values)
            || executeNumber<Int16>(*offsets, *data_columns[0], null_map, res_values)
            || executeNumber<Int32>(*offsets, *data_columns[0], null_map, res_values)
            || executeNumber<Int64>(*offsets, *data_columns[0], null_map, res_values)
            || executeNumber<Float32>(*offsets, *data_columns[0], null_map, res_values)
            || executeNumber<Float64>(*offsets, *data_columns[0], null_map, res_values)
            || executeString(*offsets, *data_columns[0], null_map, res_values)
            ||*/ executeHashed(*offsets, data_columns, 
                    depth, depths, 
                    res_values);
    }
    else
    {
        /*execute128bit(*offsets, data_columns, res_values)
            ||*/ executeHashed(*offsets, data_columns,
                    depth, depths,
                    res_values);
    }

    block.getByPosition(result).column = ColumnArray::create(std::move(res_nested), offsets_column);
}

/*
template <typename Derived>
template <typename T>
bool FunctionArrayEnumerateRankedExtended<Derived>::executeNumber(
    const ColumnArray::Offsets & offsets, const IColumn & data, const NullMap * null_map, ColumnUInt32::Container & res_values)
{
    const ColumnVector<T> * data_concrete = checkAndGetColumn<ColumnVector<T>>(&data);
    if (!data_concrete)
        return false;
    const auto & values = data_concrete->getData();

    using ValuesToIndices = ClearableHashMap<T, UInt32, DefaultHash<T>, HashTableGrower<INITIAL_SIZE_DEGREE>,
        HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(T)>>;

    ValuesToIndices indices;
    size_t prev_off = 0;
    if constexpr (std::is_same_v<Derived, FunctionArrayEnumerateUniq>)
    {
        // Unique
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            indices.clear();
            UInt32 null_count = 0;
            size_t off = offsets[i];
            for (size_t j = prev_off; j < off; ++j)
            {
                if (null_map && (*null_map)[j])
                    res_values[j] = ++null_count;
                else
                    res_values[j] = ++indices[values[j]];

                DUMP(i,j, prev_off, res_values[j], values[j]);
            }
            prev_off = off;
        }
    }
    else
    {
        // Dense
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            indices.clear();
            size_t rank = 0;
            UInt32 null_index = 0;
            size_t off = offsets[i];
            for (size_t j = prev_off; j < off; ++j)
            {
                if (null_map && (*null_map)[j])
                {
                    if (!null_index)
                        null_index = ++rank;
                    res_values[j] = null_index;
                }
                else
                {
                    auto & idx = indices[values[j]];
                    if (!idx)
                        idx = ++rank;
                    res_values[j] = idx;
                }
            }
            prev_off = off;
        }
    }

DUMP(res_values);

    return true;
}

template <typename Derived>
bool FunctionArrayEnumerateRankedExtended<Derived>::executeString(
    const ColumnArray::Offsets & offsets, const IColumn & data, const NullMap * null_map, ColumnUInt32::Container & res_values)
{
    const ColumnString * values = checkAndGetColumn<ColumnString>(&data);
    if (!values)
        return false;

    size_t prev_off = 0;
    using ValuesToIndices = ClearableHashMap<StringRef, UInt32, StringRefHash, HashTableGrower<INITIAL_SIZE_DEGREE>,
        HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(StringRef)>>;

    ValuesToIndices indices;
    if constexpr (std::is_same_v<Derived, FunctionArrayEnumerateUniq>)
    {
        // Unique
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            indices.clear();
            UInt32 null_count = 0;
            size_t off = offsets[i];
            for (size_t j = prev_off; j < off; ++j)
            {
                if (null_map && (*null_map)[j])
                    res_values[j] = ++null_count;
                else
                    res_values[j] = ++indices[values->getDataAt(j)];
            }
            prev_off = off;
        }
    }
    else
    {
        // Dense
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            indices.clear();
            size_t rank = 0;
            UInt32 null_index = 0;
            size_t off = offsets[i];
            for (size_t j = prev_off; j < off; ++j)
            {
                if (null_map && (*null_map)[j])
                {
                    if (!null_index)
                        null_index = ++rank;
                    res_values[j] = null_index;
                }
                else
                {
                    auto & idx = indices[values->getDataAt(j)];
                    if (!idx)
                        idx = ++rank;
                    res_values[j] = idx;
                }
            }
            prev_off = off;
        }
    }
DUMP(res_values);
    return true;
}

template <typename Derived>
bool FunctionArrayEnumerateRankedExtended<Derived>::execute128bit(
    const ColumnArray::Offsets & offsets,
    const ColumnRawPtrs & columns,
    ColumnUInt32::Container & res_values)
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

    if (keys_bytes > 16)
        return false;

    using ValuesToIndices = ClearableHashMap<UInt128, UInt32, UInt128HashCRC32, HashTableGrower<INITIAL_SIZE_DEGREE>,
        HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(UInt128)>>;

    ValuesToIndices indices;
    size_t prev_off = 0;
    if constexpr (std::is_same_v<Derived, FunctionArrayEnumerateUniq>)
    {
        // Unique
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            indices.clear();
            size_t off = offsets[i];
            for (size_t j = prev_off; j < off; ++j)
                res_values[j] = ++indices[packFixed<UInt128>(j, count, columns, key_sizes)];
            prev_off = off;
        }
    }
    else
    {
        // Dense
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            indices.clear();
            size_t off = offsets[i];
            size_t rank = 0;
            for (size_t j = prev_off; j < off; ++j)
            {
                auto & idx = indices[packFixed<UInt128>(j, count, columns, key_sizes)];
                if (!idx)
                    idx = ++rank;
                res_values[j] = idx;
            }
            prev_off = off;
        }
    }

DUMP(res_values);

    return true;
}
*/

template <typename Derived>
bool FunctionArrayEnumerateRankedExtended<Derived>::executeHashed(
    const ColumnArray::Offsets & offsets,
    const ColumnRawPtrs & columns,
    //DepthType depth, std::vector<DepthType> depths, 
    DepthType depth, DepthTypes depths, 
    ColumnUInt32::Container & res_values)
{
(void)depth;
(void)depths;

    size_t count = columns.size();

    using ValuesToIndices = ClearableHashMap<UInt128, UInt32, UInt128TrivialHash, HashTableGrower<INITIAL_SIZE_DEGREE>,
        HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(UInt128)>>;

    ValuesToIndices indices;
    size_t prev_off = 0;
/*
    if constexpr (std::is_same_v<Derived, FunctionArrayEnumerateUniq>)
    {
        // Unique
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
    else
*/
    {
        // Dense
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            indices.clear();
            size_t off = offsets[i];
            size_t rank = 0;
            for (size_t j = prev_off; j < off; ++j)
            {
                auto & idx = indices[hash128(j, count, columns)];
DUMP(idx, i, j, count, rank, columns);
                if (!idx)
                    idx = ++rank;
                res_values[j] = idx;
            }
            prev_off = off;
        }
    }

DUMP(res_values);

    return true;
}

}
