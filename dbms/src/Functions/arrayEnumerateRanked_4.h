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

#include <Functions/FunctionFactory.h>
#include <DataTypes/getLeastSupertype.h> 
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

class FunctionArrayEnumerateUniqRanked;
//class FunctionArrayEnumerateDenseRanked;

template <typename Derived>
class FunctionArrayEnumerateRankedExtended : public IFunction
{
public:
    FunctionArrayEnumerateRankedExtended(const Context & context) : context(context) {}

    static FunctionPtr create(const Context & context) { return std::make_shared<Derived>(context); }

    String getName() const override { return Derived::name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() == 0)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be at least 1.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

/*
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].get());
            if (!array_type)
                throw Exception("All arguments for function " + getName() + " must be arrays but argument " +
                    toString(i + 1) + " has type " + arguments[i]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
*/

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;

private:
    const Context & context;
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
                           const NullMap * null_map, ColumnUInt32::Container & res_values);

    template <typename T>
    bool executeNumber(const ColumnArray::Offsets & offsets, const IColumn & data, const NullMap * null_map, ColumnUInt32::Container & res_values);
    bool executeString(const ColumnArray::Offsets & offsets, const IColumn & data, const NullMap * null_map, ColumnUInt32::Container & res_values);
    bool executeFixedString(const ColumnArray::Offsets & offsets, const IColumn & data, const NullMap * null_map, ColumnUInt32::Container & res_values);
    bool execute128bit(const ColumnArray::Offsets & offsets, const ColumnRawPtrs & columns, ColumnUInt32::Container & res_values);
    void executeHashed(const ColumnArray::Offsets & offsets, const ColumnRawPtrs & columns, ColumnUInt32::Container & res_values);
};


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



//ColumnPtr arrayElement(const ColumnWithTypeAndName & arg, const ColumnWithTypeAndName & n, const DataTypePtr & type, const Context & context)
ColumnPtr arrayElement(const ColumnWithTypeAndName & arg, const int64_t n, const Context & context)
{
    //const ColumnArray * array = checkAndGetColumn<ColumnArray>(arg.column.get());
    //const DataTypePtr & type = array->getNestedType();
    auto array_type = typeid_cast<const DataTypeArray *>(arg.type.get()); 
    const auto & type = array_type->getNestedType();
    //DataTypeUInt32().createColumnConst(arg.size(), n);

    //ColumnWithTypeAndName n_col { DataTypeUInt32().createColumnConst(arg.column->size(), n), std::make_shared<DataTypeUInt32>(), "" };

    Block temporary_block
    {
        arg,
        //{ DataTypeUInt32().createColumnConst(arg.column->size(), n), DataTypeUInt32(), "" },
        //n_col,
        { DataTypeUInt32().createColumnConst(arg.column->size(), n), std::make_shared<DataTypeUInt32>(), "" },
        {
            nullptr,
            type,
            ""
        }
    };

    FunctionBuilderPtr func_builder = FunctionFactory::instance().get("arrayElement", context);

    ColumnsWithTypeAndName arguments{ temporary_block.getByPosition(0), temporary_block.getByPosition(1) };
    auto func = func_builder->build(arguments);

    const size_t result_colum_num = 2;
    func->execute(temporary_block, {0, 1}, result_colum_num, arg.column->size());
    return temporary_block.getByPosition(result_colum_num).column;
}

ColumnPtr array(Block & block, const Context & context)
{
    //const ColumnArray * array = checkAndGetColumn<ColumnArray>(arg.column.get());
    //const DataTypePtr & type = array->getNestedType();
    //auto array_type = typeid_cast<const DataTypeArray *>(arg.type.get()); 
    //const auto & type = array_type->getNestedType();
    //DataTypeUInt32().createColumnConst(arg.size(), n);

    //ColumnWithTypeAndName n_col { DataTypeUInt32().createColumnConst(arg.column->size(), n), std::make_shared<DataTypeUInt32>(), "" };


    //auto type = func_builder->getReturnType(block);
    DataTypes types;
    for (size_t i = 0; i < block.columns(); ++i)
        types.emplace_back(block.getByPosition(i).type);

    auto type = std::make_shared<DataTypeArray>(getLeastSupertype(types));

    block.insert( 
        {
            nullptr,
            type,
            ""
        }
    );

    ColumnsWithTypeAndName arguments; //{ block.getByPosition(0), block.getByPosition(1) };

    ColumnNumbers arguments_nums(block.columns() - 1);

    for (size_t i = 0; i < block.columns() - 1; ++i) {
        arguments.emplace_back(block.getByPosition(i));
        arguments_nums[i] = i;
    }

    FunctionBuilderPtr func_builder = FunctionFactory::instance().get("array", context);
    auto func = func_builder->build(arguments);

    const size_t result_colum_num = block.columns() - 1;

    func->execute(block, arguments_nums, result_colum_num, block.rows());
    return block.getByPosition(result_colum_num).column;
}



template <typename Derived>
void FunctionArrayEnumerateRankedExtended<Derived>::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
{
    const ColumnArray::Offsets * offsets = nullptr;
    size_t num_arguments = arguments.size();
    ColumnRawPtrs data_columns; //(num_arguments);

    Columns array_holders;
    ColumnPtr offsets_column;




    ColumnsWithTypeAndName args;

    for (size_t i = 0; i < arguments.size(); ++i)
        args.emplace_back( block.getByPosition(arguments[i]) );

    DepthType depth; //1;
    DepthType max_array_depth; //1;
    //std::vector<DepthType> depths;
    DepthTypes depths;

    std::tie(depth, depths, max_array_depth) = getDepths(args);

DUMP(depth, depths, max_array_depth);


/*
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
    (void)get_array_column;
*/

    size_t array_num = 0;
    for (size_t i = 0; i < num_arguments; ++i)
    {
        const ColumnPtr & array_ptr = block.getByPosition(arguments[i]).column;
        const ColumnArray * array = checkAndGetColumn<ColumnArray>(array_ptr.get());
        if (!array)
        {
            const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(
                block.getByPosition(arguments[i]).column.get());
            if (!const_array)
                continue;

/*
                throw Exception("Illegal column " + block.getByPosition(arguments[i]).column->getName()
                    + " of " + toString(i + 1) + "-th argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
*/
            array_holders.emplace_back(const_array->convertToFullColumn());
            array = checkAndGetColumn<ColumnArray>(array_holders.back().get());
        }

        const ColumnArray::Offsets & offsets_i = array->getOffsets();
        //auto offsets_i = array->getOffsets();
        if (!offsets)
        {
            if (depths[array_num] == 2) { // TODO recurse here
                auto sub_array = checkAndGetColumn<ColumnArray>(&array->getData());
                DUMP("getdata1=", array_num, depths[array_num], sub_array);
                if(sub_array) {
                    //const ColumnArray::Offsets & sub_offsets_i = sub_array->getOffsets();
                    //offsets = &sub_offsets_i;
                    //offsets = &sub_array->getOffsets();
                    //offsets_column = sub_array->getOffsetsPtr();
DUMP(sub_array->getOffsets());
DUMP(sub_array->getOffsetsPtr());
                }
            }
            //if(!offsets) 
            {

            offsets = &offsets_i;
            offsets_column = array->getOffsetsPtr();
            }
        }
        else if (offsets_i != *offsets)
            throw Exception("Lengths of all arrays passed to " + getName() + " must be equal.",
                ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);

        auto * array_data = &array->getData();
        data_columns.emplace_back(array_data);
        ++array_num;
    }

    const NullMap * null_map = nullptr;

    for (size_t i = 0; i < data_columns.size(); ++i)
    {
        if (data_columns[i]->isColumnNullable())
        {
            const auto & nullable_col = static_cast<const ColumnNullable &>(*data_columns[i]);

            //if (num_arguments == 1)
            if (data_columns.size() == 1)
                data_columns[i] = &nullable_col.getNestedColumn();

            null_map = &nullable_col.getNullMapData();
            break;
        }
    }

    auto res_nested = ColumnUInt32::create();

    ColumnUInt32::Container & res_values = res_nested->getData();
    if (!offsets->empty())
        res_values.resize(offsets->back());
DUMP(offsets->back(), res_values.size());

DUMP(offsets);
DUMP(data_columns);

    //if (num_arguments == 1)
    if (data_columns.size() == 1)
    {
DUMP("ONE DATA");
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
DUMP("MULTI DATA");
        if (!execute128bit(*offsets, data_columns, res_values))
            executeHashed(*offsets, data_columns, res_values);
    }

DUMP(res_nested);
DUMP(offsets_column);
    block.getByPosition(result).column = ColumnArray::create(std::move(res_nested), offsets_column);
}

template <typename Derived>
template <typename Method, bool has_null_map>
void FunctionArrayEnumerateRankedExtended<Derived>::executeMethodImpl(
        const ColumnArray::Offsets & offsets,
        const ColumnRawPtrs & columns,
        const Sizes & key_sizes,
        [[maybe_unused]] const NullMap * null_map,
        ColumnUInt32::Container & res_values)
{
    typename Method::Set indices;
DUMP(columns);
DUMP(key_sizes);
    typename Method::Method method(columns, key_sizes, nullptr);
    Arena pool; /// Won't use it;

    ColumnArray::Offset prev_off = 0;

/*

(1, [[1,2,3],[2,2,1],[3]], 2)
 ; 1 2 3 2 2 1 3
(2, [[1,2,3],[2,2,1],[3]], 2)
 ; 1 2 3; 2 2 1; 3
(1, [[1,2,3],[2,2,1],[3]], 1)
 ; [1,2,3] [2,2,1] [3]

(1, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 1)
    ;.                         .                         .
(1, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 2)
    ; .       .       .         .       .       .         .
(2, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 2)
    ; .       .       .       ; .       .       .       ; .
(1, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 3)
    ;  . . .   . . .   . . .     . . .   . . .   . . .     . .
(2, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 3)
    ;  . . .   . . .   . . .  ;  . . .   . . .   . . .  ;  . .
(3, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 3)
    ;  . . . ; . . . ; . . .  ;  . . . ; . . . ; . . .  ;  . .

*/


    if constexpr (std::is_same_v<Derived, FunctionArrayEnumerateUniqRanked>)
    {
        // Unique
        for (size_t off : offsets)
        {
DUMP("UNIQ", off);

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
                emplace_result.setMapped(idx);

                res_values[j] = idx;
                DUMP(off, j, prev_off, res_values[j], idx);
            }
            prev_off = off;
        }
    }
    else
    {
        // Dense
        for (size_t off : offsets)
        {
DUMP("DENSE", off);
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

                auto emplace_result = method.emplaceKey(indices, j, pool);
                auto idx = emplace_result.getMapped();

                if (!idx)
                {
                    idx = ++rank;
                    emplace_result.setMapped(idx);
                }

DUMP(j, idx);
                res_values[j] = idx;
            }
            prev_off = off;
        }
    }
}

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

    executeMethod<MethodFixed>(offsets, columns, key_sizes, nullptr, res_values);
    return true;
}

template <typename Derived>
void FunctionArrayEnumerateRankedExtended<Derived>::executeHashed(
    const ColumnArray::Offsets & offsets,
    const ColumnRawPtrs & columns,
    ColumnUInt32::Container & res_values)
{
    executeMethod<MethodHashed>(offsets, columns, {}, nullptr, res_values);
}

}
