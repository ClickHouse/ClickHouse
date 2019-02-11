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
#include <Common/ColumnsHashing.h>

#include <Core/iostream_debug_helpers.h>

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
        //const ColumnPtr & array_ptr = arguments[i].column;
//DUMP(array_ptr);
        //const ColumnArray * array = checkAndGetColumn<ColumnArray>(array_ptr.get());

        //const auto array = checkAndGetColumnConst<ColumnArray>(array_ptr.get());
//DUMP(i, arguments[i]);
        const auto non_const = arguments[i].column->convertToFullColumnIfConst();
//DUMP(non_const);
        const auto array = typeid_cast<const ColumnArray *>(non_const.get());
        //const auto array = typeid_cast<const ColumnArray *>(array_ptr.get());

//DUMP(array);

//DUMP(i, arguments[i], !!array);

        if (!array)
        {

            //const auto & depth_column = block.getByPosition(arguments[i]).column;
            const auto & depth_column = arguments[i].column;
//DUMP(i, depth_column);


//DUMP(depth_column->isColumnConst(), depth_column->getUInt(0));
//DUMP(depth_column->isColumnConst());

            if (depth_column  // TODO
            //   && depth_column->isColumnConst()
            )
            {
//DUMP(depth_column->size());
                auto value = depth_column->getUInt(0);
//DUMP(value);
                 if (i == 0)
                     depth = value;
                 else
                 {
                     depths.emplace_back(value);
                     if (max_array_depth < value)
                        max_array_depth = value;
                 }
                 // TODO: auto

            }
        }
    }
    if (depth > max_array_depth)
        DUMP("IMPOSSIBLE");
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

/*
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
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
*/
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    //DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
//DUMP(arguments);

//DUMP(getDepths(arguments));

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

DUMP("return type=", type);

        return type; //std::make_shared<DataTypeArray>();
    }


    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;

private:
    /// Initially allocate a piece of memory for 512 elements. NOTE: This is just a guess.
    static constexpr size_t INITIAL_SIZE_DEGREE = 9;

    template <typename T>
    struct MethodOneNumber
    {
        using Set = ClearableHashMap<T, UInt32, DefaultHash<T>, HashTableGrower<INITIAL_SIZE_DEGREE>,
                HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(T)>>;
        using Method = ColumnsHashing::HashMethodOneNumber<typename Set::value_type, UInt32, T, false>;
    };

    struct MethodString
    {
        using Set = ClearableHashMap<StringRef, UInt32, StringRefHash, HashTableGrower<INITIAL_SIZE_DEGREE>,
                HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(StringRef)>>;
        using Method = ColumnsHashing::HashMethodString<typename Set::value_type, UInt32, false, false>;
    };

    struct MethodFixedString
    {
        using Set = ClearableHashMap<StringRef, UInt32, StringRefHash, HashTableGrower<INITIAL_SIZE_DEGREE>,
                HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(StringRef)>>;
        using Method = ColumnsHashing::HashMethodFixedString<typename Set::value_type, UInt32, false, false>;
    };

    struct MethodFixed
    {
        using Set = ClearableHashMap<UInt128, UInt32, UInt128HashCRC32, HashTableGrower<INITIAL_SIZE_DEGREE>,
                HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(UInt128)>>;
        using Method = ColumnsHashing::HashMethodKeysFixed<typename Set::value_type, UInt128, UInt32, false, false, false>;
    };

    struct MethodHashed
    {
        using Set =  ClearableHashMap<UInt128, UInt32, UInt128TrivialHash, HashTableGrower<INITIAL_SIZE_DEGREE>,
                HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(UInt128)>>;
        using Method = ColumnsHashing::HashMethodHashed<typename Set::value_type, UInt32, false>;
    };

    template <typename Method>
    void executeMethod(const ColumnArray::Offsets & offsets, const ColumnRawPtrs & columns, const Sizes & key_sizes,
                       const NullMap * null_map, ColumnUInt32::Container & res_values);

    template <typename Method, bool has_null_map>
    void executeMethodImpl(const ColumnArray::Offsets & offsets, const ColumnRawPtrs & columns, const Sizes & key_sizes,
                           const NullMap * null_map, ColumnUInt32::Container & res_values,
                           typename Method::Set &indices,
                           DepthType depth
    );

    template <typename T>
    bool executeNumber(const ColumnArray::Offsets & offsets, const IColumn & data, const NullMap * null_map, ColumnUInt32::Container & res_values);
    bool executeString(const ColumnArray::Offsets & offsets, const IColumn & data, const NullMap * null_map, ColumnUInt32::Container & res_values);
    bool executeFixedString(const ColumnArray::Offsets & offsets, const IColumn & data, const NullMap * null_map, ColumnUInt32::Container & res_values);
    bool execute128bit(const ColumnArray::Offsets & offsets, const ColumnRawPtrs & columns, ColumnUInt32::Container & res_values);
    void executeHashed(const ColumnArray::Offsets & offsets, const ColumnRawPtrs & columns, ColumnUInt32::Container & res_values);
};


template <typename Derived>
void FunctionArrayEnumerateRankedExtended<Derived>::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
{
    const ColumnArray::Offsets * offsets = nullptr;

    DepthType depth; //1;
    DepthType max_array_depth; //1;
    //std::vector<DepthType> depths;
    DepthTypes depths;

    ColumnsWithTypeAndName args;

    for (size_t i = 0; i < arguments.size(); ++i)
        args.emplace_back( block.getByPosition(arguments[i]) );

// block.getByPosition(arguments[i])

    //std::tie(depth, depths, max_array_depth) = getDepths(block.getColumnsWithTypeAndName());
    std::tie(depth, depths, max_array_depth) = getDepths(args);

DUMP(depth, depths, max_array_depth);

/*        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        return getReturnTypeImpl(data_types);*/


    const size_t num_arguments = arguments.size();
    ColumnRawPtrs data_columns; //(num_arguments);

    Columns array_holders;
    ColumnPtr offsets_column;
    size_t array_num = 0;


    auto get_array_column = [&] (const auto & column) -> const DB::ColumnArray * {
        const ColumnArray * array = checkAndGetColumn<ColumnArray>(column.get());
        if (!array)
        {
            const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(column.get());
            if (!const_array)
                return nullptr;
            array_holders.emplace_back(const_array->convertToFullColumn());
            array = checkAndGetColumn<ColumnArray>(array_holders.back().get());
        }
        return array;
    };

    size_t array_size = 0;
    if (max_array_depth == 2) {
     for (size_t i = 0; i < num_arguments; ++i)
     {
        auto array = get_array_column(block.getByPosition(arguments[i]).column);
        DUMP(i, block.getByPosition(arguments[i]).column);
        DUMP(array);
        if (!array)
            continue;
        //DUMP("siz=", array->size());
        array_size = array->sizeAt(0);
        //DUMP("arrsize=", array_size);
     }
    }

    auto res_nested = ColumnUInt32::create();
    ColumnUInt32::Container & res_values = res_nested->getData();

    size_t sub_array_num = 0; // TODO for(...
    //for (size_t sub_array_num = 0; sub_array_num < array_size; ++sub_array_num) {
DUMP(sub_array_num, array_size);

    for (size_t i = 0; i < num_arguments; ++i)
    {
        const ColumnPtr & array_ptr = block.getByPosition(arguments[i]).column;
DUMP("column", i, array_ptr);
        const ColumnArray * array = checkAndGetColumn<ColumnArray>(array_ptr.get());
        if (!array)
        {
            const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(block.getByPosition(arguments[i]).column.get());
            if (!const_array){ 
DUMP("skip not array", i, array_ptr);
                continue;
            }
/*
                throw Exception("Illegal column " + block.getByPosition(arguments[i]).column->getName()
                    + " of " + toString(i + 1) + "-th argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
*/


//            if (const_array) {
            array_holders.emplace_back(const_array->convertToFullColumn());
            array = checkAndGetColumn<ColumnArray>(array_holders.back().get());
//            }
        }

DUMP("offsets before depth was", array->getOffsets());
             DUMP("wantdepth=", depths[array_num], "of", array);
             if (depths[array_num] == 2) { // TODO We need to go deeper here
                 DUMP("get subarray here", array[sub_array_num]);
                 array = checkAndGetColumn<ColumnArray>(&array[sub_array_num]);
DUMP("result subarray=", array);
if (!array) {
DUMP("no nested array");
    continue;
}
             }


        const ColumnArray::Offsets & offsets_i = array->getOffsets();
        //if (i == 0)
        if (!data_columns.size())
        {
            offsets = &offsets_i;
            offsets_column = array->getOffsetsPtr();
DUMP("offsets first=", offsets);
        }
        else if (offsets_i != *offsets)
            throw Exception("Lengths of all arrays passed to " + getName() + " must be equal.",
                ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);

        auto * array_data = &array->getData();
        data_columns.emplace_back(array_data);
        ++array_num;
    }

    //DUMP(data_columns);

    [[maybe_unused]]
    const NullMap * null_map = nullptr;

    for (size_t i = 0; i < data_columns.size() /*num_arguments*/; ++i)
    {
        if (data_columns[i]->isColumnNullable())
        {
            const auto & nullable_col = static_cast<const ColumnNullable &>(*data_columns[i]);

            if (num_arguments == 1)
                data_columns[i] = &nullable_col.getNestedColumn();

            null_map = &nullable_col.getNullMapData();
            break;
        }
    }

    if (!offsets->empty())
        res_values.resize(offsets->back());

DUMP(offsets);
DUMP(null_map);
DUMP(data_columns);


/*
    //if (num_arguments == 1)
    if (data_columns.size() == 1)
    {
        if (!(executeNumber<UInt8>(*offsets, *data_columns[0], null_map, res_values)
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
            || executeFixedString(*offsets, *data_columns[0], null_map, res_values)))
            executeHashed(*offsets, data_columns, res_values);
    }
    else
    {
        if (!execute128bit(*offsets, data_columns, res_values))
            executeHashed(*offsets, data_columns, res_values);
    }
*/

    //typename Method::Set indices;
    typename MethodHashed::Set indices;
    //ecuteMethod<MethodHashed>(offsets, data_columns, {}, nullptr, res_values);
    //executeMethodImpl<MethodHashed, false>(offsets, data_columns, key_sizes, null_map, res_values, indices);

    if (max_array_depth == 1)
    {
         executeMethodImpl<MethodHashed, false>(*offsets, data_columns, {}, nullptr, res_values, indices, depth);
    }
    else if (max_array_depth == 2)
    {
// for...
         if (depth == 2)
             indices.clear();
         executeMethodImpl<MethodHashed, false>(*offsets, data_columns, {}, nullptr, res_values, indices, depth);

    }

    //}

DUMP("=========================================");
    block.getByPosition(result).column = ColumnArray::create(std::move(res_nested), offsets_column);
}

template <typename Derived>
template <typename Method, bool has_null_map>
void FunctionArrayEnumerateRankedExtended<Derived>::executeMethodImpl(
        const ColumnArray::Offsets & offsets,
        const ColumnRawPtrs & columns,
        const Sizes & key_sizes,
        [[maybe_unused]] const NullMap * null_map,
        ColumnUInt32::Container & res_values,
        typename Method::Set &indices,
        DepthType depth
)
{
(void)depth;
    //typename Method::Set indices;
    typename Method::Method method(columns, key_sizes, nullptr);
    Arena pool; /// Won't use it;

    ColumnArray::Offset prev_off = 0;


DUMP(columns.size());
// /*  TODO
    if constexpr (std::is_same_v<Derived, FunctionArrayEnumerateUniq>)
    {
        // Unique
        for (size_t off : offsets)
        {
DUMP(off);
            if (depth == 2)
                indices.clear();
            UInt32 null_count = 0;
            for (size_t j = prev_off; j < off; ++j)
            {
DUMP(j, prev_off, off);
                if constexpr (has_null_map)
                {
                    if ((*null_map)[j])
                    {
                        res_values[j] = ++null_count;
                        continue;
                    }
                }

                auto emplace_result = method.emplaceKey(indices, j, pool);
                auto idx = emplace_result.getMapped() + 1;
DUMP(indices, j, pool, emplace_result, idx);
                emplace_result.setMapped(idx);

                res_values[j] = idx;
DUMP(off, j, prev_off, res_values[j], idx);
            }
            prev_off = off;
        }
    }
    else
// */
    {
        // Dense
        for (size_t off : offsets)
        {
DUMP(off);
            if (depth == 2)
                indices.clear();
            UInt32 rank = 0;
            [[maybe_unused]] UInt32 null_index = 0;
            for (size_t j = prev_off; j < off; ++j)
            {
DUMP(j, prev_off, off);
                if constexpr (has_null_map)
                {
                    if ((*null_map)[j])
                    {
                        if (!null_index)
                            null_index = ++rank;

                        res_values[j] = null_index;
                        continue;
                    }
                }

//DUMP(0, j, columns[0]->getElement(j));
//DUMP(0, j,  columns.size(), columns[0]->getInt(j));
                auto emplace_result = method.emplaceKey(indices, j, pool);
                auto idx = emplace_result.getMapped();
DUMP(
//indices, 
j, 
//pool, 
emplace_result, 
idx);

                if (!idx)
                {
                    idx = ++rank;
                    emplace_result.setMapped(idx);
                }
DUMP(j, idx, rank);

                res_values[j] = idx;
            }
            prev_off = off;
        }
    }
DUMP(res_values);
}

/*
template <typename Derived>
template <typename Method>
void FunctionArrayEnumerateRankedExtended<Derived>::executeMethod(
    const ColumnArray::Offsets & offsets,
    const ColumnRawPtrs & columns,
    const Sizes & key_sizes,
    const NullMap * null_map,
    ColumnUInt32::Container & res_values)
{
    if (null_map)
        executeMethodImpl<Method, true>(offsets, columns, key_sizes, null_map, res_values);
    else
        executeMethodImpl<Method, false>(offsets, columns, key_sizes, null_map, res_values);

DUMP(res_values);

}

template <typename Derived>
template <typename T>
bool FunctionArrayEnumerateRankedExtended<Derived>::executeNumber(
    const ColumnArray::Offsets & offsets, const IColumn & data, const NullMap * null_map, ColumnUInt32::Container & res_values)
{
    const auto * nested = checkAndGetColumn<ColumnVector<T>>(&data);
    if (!nested)
        return false;

    executeMethod<MethodOneNumber<T>>(offsets, {nested}, {}, null_map, res_values);
    return true;
}

template <typename Derived>
bool FunctionArrayEnumerateRankedExtended<Derived>::executeString(
    const ColumnArray::Offsets & offsets, const IColumn & data, const NullMap * null_map, ColumnUInt32::Container & res_values)
{
    const auto * nested = checkAndGetColumn<ColumnString>(&data);
    if (nested)
        executeMethod<MethodString>(offsets, {nested}, {}, null_map, res_values);

    return nested;
}

template <typename Derived>
bool FunctionArrayEnumerateRankedExtended<Derived>::executeFixedString(
        const ColumnArray::Offsets & offsets, const IColumn & data, const NullMap * null_map, ColumnUInt32::Container & res_values)
{
    const auto * nested = checkAndGetColumn<ColumnString>(&data);
    if (nested)
        executeMethod<MethodFixedString>(offsets, {nested}, {}, null_map, res_values);

DUMP(res_values);

    return nested;
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

DUMP(offsets, columns, key_sizes, nullptr, res_values);
    executeMethod<MethodFixed>(offsets, columns, key_sizes, nullptr, res_values);
    return true;
}
*/

template <typename Derived>
void FunctionArrayEnumerateRankedExtended<Derived>::executeHashed(
    const ColumnArray::Offsets & offsets,
    const ColumnRawPtrs & columns,
    ColumnUInt32::Container & res_values)
{
    executeMethod<MethodHashed>(offsets, columns, {}, nullptr, res_values);
}

}
