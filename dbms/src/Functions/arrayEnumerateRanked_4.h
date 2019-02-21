#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/AggregationCommon.h>
#include <Common/ColumnsHashing.h>
#include <Common/HashTable/ClearableHashMap.h>

#include <Core/iostream_debug_helpers.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>


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

using DepthType = uint32_t;
using DepthTypes = std::vector<DepthType>;
std::tuple<DepthType, DepthTypes, DepthType> getDepths(/*Block & block,*/ const ColumnsWithTypeAndName & arguments)
{
    const size_t num_arguments = arguments.size();
    //ColumnRawPtrs data_columns; //(num_arguments);

    //Columns array_holders;
    //ColumnPtr offsets_column;
    DepthType clear_depth = 0; //1;
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
        if (!arguments[i].column)
        {
            DUMP("No column at ", i, arguments[i]);
            continue;
        }
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

            if (depth_column // TODO
                //   && depth_column->isColumnConst()
            )
            {
                //DUMP(depth_column->size());
                auto value = depth_column->getUInt(0);
                //DUMP(value);
                if (i == 0)
                    clear_depth = value;
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
    if (clear_depth > max_array_depth)
        DUMP("IMPOSSIBLE"); // TODO THROW
    return std::make_tuple(clear_depth, depths, max_array_depth);
}


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

    //DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
#if 0
        if (arguments.size() == 0)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be at least 1.",
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
#endif

        DepthType clear_depth;
        DepthType max_array_depth;
        DepthTypes depths;

        std::tie(clear_depth, depths, max_array_depth) = getDepths(arguments);

        if (arguments.size() == 0)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be at least 1.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DataTypePtr type = std::make_shared<DataTypeUInt32>();
        for (DepthType i = 0; i < max_array_depth; ++i)
        {
            type = std::make_shared<DataTypeArray>(type);
        }

        DUMP("return type=", type);

        return type; //std::make_shared<DataTypeArray>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;

private:
    const Context & context;
    /// Initially allocate a piece of memory for 512 elements. NOTE: This is just a guess.
    static constexpr size_t INITIAL_SIZE_DEGREE = 9;

    /*
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

*/
    struct MethodHashed
    {
        using Set = ClearableHashMap<
            UInt128,
            UInt32,
            UInt128TrivialHash,
            HashTableGrower<INITIAL_SIZE_DEGREE>,
            HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(UInt128)>>;
        using Method = ColumnsHashing::HashMethodHashed<typename Set::value_type, UInt32, false>;
    };

    /*
    template <typename Method>
    void executeMethod(
        const ColumnArray::Offsets & offsets,
        const ColumnRawPtrs & columns,
        const Sizes & key_sizes,
        const NullMap * null_map,
        ColumnUInt32::Container & res_values);
*/


    template <typename Method, bool has_null_map>
    void executeMethodImpl(
        //const ColumnArray::Offsets & offsets,
        //const std::vector<ColumnPtr> & offsets_by_depth,
        //const std::vector<IColumn::Offsets*> & offsets_by_depth,
        const std::vector<const ColumnArray::Offsets *> & offsets_by_depth,
        const ColumnRawPtrs & columns,
        //const Sizes & key_sizes,
        //const NullMap * null_map,
        DepthType clear_depth,
        DepthType max_array_depth,
        DepthTypes depths,
        ColumnUInt32::Container & res_values);

    /*
    template <typename T>
    bool executeNumber(
        const ColumnArray::Offsets & offsets, const IColumn & data, const NullMap * null_map, ColumnUInt32::Container & res_values);
    bool executeString(
        const ColumnArray::Offsets & offsets, const IColumn & data, const NullMap * null_map, ColumnUInt32::Container & res_values);
    bool executeFixedString(
        const ColumnArray::Offsets & offsets, const IColumn & data, const NullMap * null_map, ColumnUInt32::Container & res_values);
    bool execute128bit(const ColumnArray::Offsets & offsets, const ColumnRawPtrs & columns, ColumnUInt32::Container & res_values);
    void executeHashed(const ColumnArray::Offsets & offsets, const ColumnRawPtrs & columns, ColumnUInt32::Container & res_values);
*/
};


//ColumnPtr arrayElement(const ColumnWithTypeAndName & arg, const ColumnWithTypeAndName & n, const DataTypePtr & type, const Context & context)
ColumnPtr arrayElement(const ColumnWithTypeAndName & arg, const int64_t n, const Context & context)
{
    //const ColumnArray * array = checkAndGetColumn<ColumnArray>(arg.column.get());
    //const DataTypePtr & type = array->getNestedType();
    auto array_type = typeid_cast<const DataTypeArray *>(arg.type.get());
    const auto & type = array_type->getNestedType();
    //DataTypeUInt32().createColumnConst(arg.size(), n);

    //ColumnWithTypeAndName n_col { DataTypeUInt32().createColumnConst(arg.column->size(), n), std::make_shared<DataTypeUInt32>(), "" };

    Block temporary_block{arg,
                          //{ DataTypeUInt32().createColumnConst(arg.column->size(), n), DataTypeUInt32(), "" },
                          //n_col,
                          {DataTypeUInt32().createColumnConst(arg.column->size(), n), std::make_shared<DataTypeUInt32>(), ""},
                          {nullptr, type, ""}};

    FunctionBuilderPtr func_builder = FunctionFactory::instance().get("arrayElement", context);

    ColumnsWithTypeAndName arguments{temporary_block.getByPosition(0), temporary_block.getByPosition(1)};
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

    block.insert({nullptr, type, ""});

    ColumnsWithTypeAndName arguments; //{ block.getByPosition(0), block.getByPosition(1) };

    ColumnNumbers arguments_nums(block.columns() - 1);

    for (size_t i = 0; i < block.columns() - 1; ++i)
    {
        arguments.emplace_back(block.getByPosition(i));
        arguments_nums[i] = i;
    }

    FunctionBuilderPtr func_builder = FunctionFactory::instance().get("array", context);
    auto func = func_builder->build(arguments);

    const size_t result_colum_num = block.columns() - 1;

    func->execute(block, arguments_nums, result_colum_num, block.rows());
    return block.getByPosition(result_colum_num).column;
}


/// Hash a set of keys into a UInt128 value.
static inline UInt128 ALWAYS_INLINE hash128depths(std::vector<size_t> indexes, /*size_t keys_size,*/ const ColumnRawPtrs & key_columns)
{
    UInt128 key;
    SipHash hash;

    for (size_t j = 0, keys_size = key_columns.size(); j < keys_size; ++j)
    {
        //DUMP(j, indexes[j], key_columns[j]->operator[](indexes[j]));
        key_columns[j]->updateHashWithValue(indexes[j], hash);
    }

    hash.get128(key.low, key.high);

    return key;
}


template <typename Derived>
void FunctionArrayEnumerateRankedExtended<Derived>::executeImpl(
    Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
{
    //const ColumnArray::Offsets * offsets = nullptr;
    size_t num_arguments = arguments.size();
    ColumnRawPtrs data_columns; //(num_arguments);

    Columns array_holders;
    ColumnPtr offsets_column;


    ColumnsWithTypeAndName args;

    for (size_t i = 0; i < arguments.size(); ++i)
        args.emplace_back(block.getByPosition(arguments[i]));

    DepthType clear_depth; //1;
    DepthType max_array_depth; //1;
    //std::vector<DepthType> depths;
    DepthTypes depths;

    std::tie(clear_depth, depths, max_array_depth) = getDepths(args);

    DUMP("GO FUNC!=======", clear_depth, depths, max_array_depth);


    // /*
    auto get_array_column = [&](const auto & column) -> const DB::ColumnArray * {
        //const ColumnArray * array = checkAndGetColumn<ColumnArray>(column.get());
        const ColumnArray * array = checkAndGetColumn<ColumnArray>(column);
        if (!array)
        {
            //const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(column.get());
            const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(column);
            if (!const_array)
                return nullptr;
            array_holders.emplace_back(const_array->convertToFullColumn());
            array = checkAndGetColumn<ColumnArray>(array_holders.back().get());
        }
        return array;
    };
    //(void)get_array_column;
    // */

    //std::vector<ColumnPtr> offsets_by_depth;
    //std::vector<IColumn::Offsets*> offsets_by_depth;
    std::vector<const ColumnArray::Offsets *> offsets_by_depth;
    std::vector<ColumnPtr> offsetsptr_by_depth; // TODO make one !

    size_t array_num = 0;
    for (size_t i = 0; i < num_arguments; ++i)
    {
        const ColumnPtr & array_ptr = block.getByPosition(arguments[i]).column;
        DUMP(array_ptr);
        const ColumnArray * array = checkAndGetColumn<ColumnArray>(array_ptr.get());
        DUMP(array);
        if (!array)
        {
            const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(block.getByPosition(arguments[i]).column.get());
            if (!const_array)
            {
                DUMP("Not array", i);
                continue;
            }
            /*
                throw Exception("Illegal column " + block.getByPosition(arguments[i]).column->getName()
                    + " of " + toString(i + 1) + "-th argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
*/
            array_holders.emplace_back(const_array->convertToFullColumn());
            array = checkAndGetColumn<ColumnArray>(array_holders.back().get());
        }

        DUMP(array);

        if (array_num == 0) // TODO check with prev
        {
            //offsets_by_depth.emplace_back(array->getOffsetsPtr());
            offsets_by_depth.emplace_back(&array->getOffsets());
            offsetsptr_by_depth.emplace_back(array->getOffsetsPtr());
        }

        for (DepthType col_depth = 1; col_depth <= depths[array_num]; ++col_depth)
        {
            //DUMP(array_num, col_depth, depths[array_num]);

            //DUMP("offsets was:", array->getOffsets());

            //DUMP("test offsets column:", col_depth, max_array_depth);
            if (col_depth == max_array_depth)
            {
                //DUMP("using offsets column:", col_depth, max_array_depth);
                offsets_column = array->getOffsetsPtr();
            }


            auto sub_array = get_array_column(&array->getData());
            //DUMP(sub_array);
            if (sub_array)
            {
                array = sub_array;
            }
            if (!sub_array)
            {
                //DUMP("Not subarray");
                break;
            }

            //if (array_num == 0) // TODO check with prev
            //offsets_by_depth.emplace_back(array->getOffsetsPtr());
            //DUMP(offsets_by_depth.size(), "<" ,col_depth);
            if (offsets_by_depth.size() <= col_depth)
            {
                offsets_by_depth.emplace_back(&array->getOffsets());
                offsetsptr_by_depth.emplace_back(array->getOffsetsPtr());
            }


            //array->getOffsets()
        }

        DUMP(offsets_by_depth);
        /*
            if (0 && depths[array_num] > 1) { // TODO recurse here
                //auto sub_array = checkAndGetColumn<ColumnArray>(&array->getData());

                auto sub_array = get_array_column(&array->getData());
                DUMP("getdata1=", array_num, depths[array_num], sub_array);
                if(sub_array) {
                    //const ColumnArray::Offsets & sub_offsets_i = sub_array->getOffsets();
                    //offsets = &sub_offsets_i;
                    //offsets = &sub_array->getOffsets();
                    //offsets_column = sub_array->getOffsetsPtr();

                    DUMP(1,sub_array);
                    DUMP(1,sub_array->getOffsets());
                    DUMP(1,sub_array->getOffsetsPtr());

                    //sub_array = checkAndGetColumn<ColumnArray>(&sub_array->getData());
                    DUMP(1, sub_array->getData());
                    sub_array = get_array_column(&sub_array->getData());
                    if (sub_array) {
                        DUMP(2,sub_array);
                        DUMP(2,sub_array->getOffsets());
                        DUMP(2,sub_array->getOffsetsPtr());

                        //sub_array = checkAndGetColumn<ColumnArray>(&sub_array->getData());
                        DUMP(2,sub_array->getData());
                        sub_array = get_array_column(&sub_array->getData());
                        if (sub_array) {
                            DUMP(3,sub_array);
                            DUMP(3,sub_array->getOffsets());
                            DUMP(3,sub_array->getOffsetsPtr());
                            DUMP(3,sub_array->getData());
                        //} else {
                        //    DUMP(sub_array->getData());
                        }
                    //} else {
                    //     DUMP(sub_array->getData());
                    }
                //} else {
                //   DUMP(sub_array->getData());
                }
            }
*/


        /*
        const ColumnArray::Offsets & offsets_i = array->getOffsets();
        //auto offsets_i = array->getOffsets();
        if (!offsets)
        {
            DUMP(array->getOffsets());
            //if(!offsets)
            {
                offsets = &offsets_i;
                //offsets_column = array->getOffsetsPtr();
            }
        }
        else if (offsets_i != *offsets)
//TODO this check!
//TODO this check!
            throw Exception("Lengths of all arrays passed to " + getName() + " must be equal.", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH); //TODO this check!
//TODO this check!
//TODO this check!
*/

        auto * array_data = &array->getData();
        data_columns.emplace_back(array_data);
        ++array_num;
    }


    //offsets_column = *offsets_by_depth[depth-1]; // TODO check

    /*
[[maybe_unused]]
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
*/

    auto res_nested = ColumnUInt32::create();

    ColumnUInt32::Container & res_values = res_nested->getData();
    /*
    if (!offsets->empty())
        res_values.resize(offsets->back());
    */

    //res_values.resize(offsets_by_depth[clear_depth-1]->back()); // todo size check?
    res_values.resize(offsets_by_depth[max_array_depth - 1]->back()); // todo size check?

    //DUMP(offsets->back(), res_values.size());
    DUMP("res total size=", res_values.size(), res_values);

    //DUMP(offsets);
    //DUMP(data_columns);

    //if (num_arguments == 1)
    /*
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
*/
    {
        //DUMP("MULTI DATA");
        //if (!execute128bit(*offsets, data_columns, res_values))
        //executeHashed(*offsets, data_columns, res_values);
        //executeHashed(offsets_by_depth, data_columns, res_values);
        //executeMethod<MethodHashed>(offsets_by_depth, data_columns, {}, nullptr, res_values);
        executeMethodImpl<MethodHashed, false>(offsets_by_depth, data_columns, clear_depth, max_array_depth, depths, res_values);
    }


    DUMP(res_nested);
    DUMP(offsets_column);
    //auto result = ColumnArray::create(std::move(res_nested), offsets_column);

    DUMP(max_array_depth, offsets_by_depth);

    //auto result_nested_array = ColumnArray::create(std::move(res_nested), *offsets_by_depth[max_array_depth-1]);
    //auto result_nested_array = ColumnArray::create(std::move(res_nested), offsetsptr_by_depth[max_array_depth-1]);
    //auto result_nested_array = ColumnArray::create(std::move(res_nested), offsetsptr_by_depth[max_array_depth-1]);
    //ColumnArray::Ptr result_nested_array = res_nested;
    ColumnPtr result_nested_array = std::move(res_nested);
    //DUMP(max_array_depth-1, result_nested_array, offsetsptr_by_depth[max_array_depth-1]);

    //if (max_array_depth >= 2)
    //for (auto depth = max_array_depth-2; depth>0; --depth) {
    //DUMP(max_array_depth);
    //if (max_array_depth >= 1)
    //auto depth = max_array_depth-1;
    //do {
    //for (auto depth = max_array_depth-1; depth>=0; --depth) {
    for (int depth = max_array_depth - 1; depth >= 0; --depth)
    {
        //DUMP(depth);
        //DUMP(offsets_by_depth[depth]);
        //ColumnArray::create(std::move(result_nested_array), *offsets_by_depth[depth-1]);
        result_nested_array = ColumnArray::create(std::move(result_nested_array), offsetsptr_by_depth[depth]);
        //DUMP(depth, result_nested_array, offsetsptr_by_depth[depth]);
    }
    //while(--depth>0)

    DUMP(result_nested_array);
    block.getByPosition(result).column = result_nested_array;

    //DUMP("complete=", block.getByPosition(result).column);
}


template <typename Derived>
template <typename Method, bool has_null_map>
void FunctionArrayEnumerateRankedExtended<Derived>::executeMethodImpl(
    //const ColumnArray::Offsets & offsets,
    //const std::vector<ColumnPtr> & offsets_by_depth,
    //const std::vector<IColumn::Offsets*> & offsets_by_depth,
    const std::vector<const ColumnArray::Offsets *> & offsets_by_depth,
    const ColumnRawPtrs & columns,
    //const Sizes & key_sizes,
    //[[maybe_unused]] const NullMap * null_map,

    [[maybe_unused]] DepthType clear_depth,
    [[maybe_unused]] DepthType max_array_depth,
    [[maybe_unused]] DepthTypes depths,

    ColumnUInt32::Container & res_values)
{
    //typename Method::Set indices;
    DUMP("========= gocalc");
    DUMP(offsets_by_depth);
    DUMP(columns);
    //DUMP(key_sizes);
    //typename Method::Method method(columns, key_sizes, nullptr);
    //Arena pool; /// Won't use it;

    //const auto & offsets = *offsets_by_depth[clear_depth]; //->getData(); //depth!
    const size_t current_offset_depth = max_array_depth;
    //const auto & offsets = *offsets_by_depth[max_array_depth-1]; //->getData(); //depth!
    const auto & offsets = *offsets_by_depth[current_offset_depth - 1]; //->getData(); //depth!
    //const auto & offsets = *offsets_by_depth[0];
    DUMP(max_array_depth, offsets, current_offset_depth);

    ColumnArray::Offset prev_off = 0;

    using Map = ClearableHashMap<
        UInt128,
        UInt32,
        UInt128TrivialHash,
        HashTableGrower<INITIAL_SIZE_DEGREE>,
        HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(UInt128)>>;
    Map indices;

    /*

(1, [[1,2,3],[2,2,1],[3]], 2)
    ; 1 2 3   2 2 1   3
(2, [[1,2,3],[2,2,1],[3]], 2)
    ; 1 2 3;  2 2 1;  3
(1, [[1,2,3],[2,2,1],[3]], 1)
    ;[1,2,3] [2,2,1] [3]


(1, [[1,2,3],[2,2,1],[3]], 2, [4,5,6], 1)
    ; 1 2 3   2 2 1   3        4 5 6
    ; 4 4 4   5 5 5   6      <-
 idx: 1 2 3   4 5 6   7
      1 1 1   2 2 2   3

dep: {2,1}       d2end:
     {1,1} {2,1} {3,1}

index to end op depth:
        3:2     6:2  7:2


 d:2   d:1    d2mark   d1mark
   1   4
   2   
   3   
               3
   2   5
   2
   1
               6
   3   6
               7       3

arrays:{1 2 3   2 2 1   3}, {4 5 6}
marks{d1:{}, d2:{3,6})
maxdepth: 2



(2, [[1,2,3],[2,2,1],[3]], 2, [4,5,6], 1)
    ; 1 2 3;  2 2 1;  3        4 5 6
    ; 4 4 4;  5 5 5;  6      <-

(1, [[1,2,3],[2,2,1],[3]], 1, [4,5,6], 1)
    ;[1,2,3] [2,2,1] [3]       4 5 6
    ;4       5       6       <-

(1, [[1,2,3],[2,2,1],[3]], 1, [4,5,6], 0)
    ;[1,2,3] [2,2,1] [3]       4 5 6
    ;[4,5,6] [4,5,6] [4,5,6] <-


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
              3       6


getArrayElement(array, depth, n)
maxdepth=2
[[1,2,3],[2,2,1],[3]], 0, 1 -> [[1,2,3],[2,2,1],[3]]  maxelements=1
[[1,2,3],[2,2,1],[3]], 0, 2 -> exception
[[1,2,3],[2,2,1],[3]], 1, 1 -> [1,2,3]                maxelements=3
[[1,2,3],[2,2,1],[3]], 1, 2 -> [2,2,1]
[[1,2,3],[2,2,1],[3]], 1, 3 -> [3]
[[1,2,3],[2,2,1],[3]], 1, 4 -> exception
[[1,2,3],[2,2,1],[3]], 2, 1 -> 1                      maxelements=7
[[1,2,3],[2,2,1],[3]], 2, 2 -> 2
[[1,2,3],[2,2,1],[3]], 2, 3 -> 3
[[1,2,3],[2,2,1],[3]], 2, 4 -> 2
[[1,2,3],[2,2,1],[3]], 2, 5 -> 2
[[1,2,3],[2,2,1],[3]], 2, 6 -> 1
[[1,2,3],[2,2,1],[3]], 2, 7 -> 3
[[1,2,3],[2,2,1],[3]], 2, 8 -> exception
[[1,2,3],[2,2,1],[3]], 3, 1 -> 1 ! depth>maxdepth     maxelements=7
[[1,2,3],[2,2,1],[3]], 3, 2 -> 2

maxdepth=1
[1,2,3], 0, 1 -> [1,2,3]                              maxelements=1
[1,2,3], 1, 1 -> 1                                    maxelements=3
[1,2,3], 1, 2 -> 2
[1,2,3], 1, 3 -> 3
[1,2,3], 1, 4 -> exception
[1,2,3], 2, 1 -> 1 ! depth>maxdepth                   maxelements=3
[1,2,3], 2, 2 -> 2
[1,2,3], 2, 3 -> 3
[1,2,3], 2, 4 -> exception




*/

    //std::vector<size_t> indexestest(columns.size());
    std::vector<size_t> indexes_by_depth(max_array_depth);
    //std::vector<size_t> prev_off_by_depth(max_array_depth);
    std::vector<size_t> current_offset_n_by_depth(max_array_depth);


    if constexpr (std::is_same_v<Derived, FunctionArrayEnumerateUniqRanked>)
    {
        // Unique
        for (size_t off : offsets)
        {
            DUMP("UNIQ", off);

            //indices.clear();

            std::vector<size_t> indexes(columns.size());

            /*
            for (size_t j = prev_off; j < off; ++j)
            {

                for (size_t col_n = 0; col_n < columns.size(); ++col_n ) {
DUMP("++", col_n, depths[col_n]);

                    if (depths[col_n] == current_offset_depth) {
                        ++indexestest[col_n];
                    }
                }           
DUMP("dosomething", j, indexestest);
            }

                for (size_t col_n = 0; col_n < columns.size(); ++col_n ) {
DUMP("levelup", col_n, depths[col_n]);

                    if (depths[col_n] == current_offset_depth-1) {
                        //++indexestest[col_n];
                        ++indexes_by_depth[]
                    }

                }           
*/

            bool want_clear = false;


            for (size_t j = prev_off; j < off; ++j)
            {
                for (size_t col_n = 0; col_n < columns.size(); ++col_n)
                {
                    //DUMP(col_n, depths[col_n], indexes_by_depth[depths[col_n] - 1]);
                    indexes[col_n] = indexes_by_depth[depths[col_n] - 1];
                }
                DUMP(indexes);

                auto hash = hash128depths(indexes, columns);
                auto idx = ++indices[hash];
                DUMP("res", j, hash, "=", idx);
                res_values[j] = idx;

                ++indexes_by_depth[current_offset_depth - 1];
                //DUMP("++", j, indexes_by_depth[current_offset_depth - 1], current_offset_depth);
                //DUMP("i", indexes_by_depth);
            }

            if (current_offset_depth >= 2)
            {
                for (int depth = current_offset_depth - 2; depth >= 0; --depth)
                { // TODO CHECK SIZE
                    ++indexes_by_depth[depth];
                    DUMP(
                        "dph",
                        depth,
                        indexes_by_depth[depth],
                        current_offset_n_by_depth[depth],
                        (*offsets_by_depth[depth])[current_offset_n_by_depth[depth]]);
                    //offsets_by_depth[


                    //current_offset_n_by_depth[depth];

                    DUMP("ctest", indexes_by_depth[depth], (*offsets_by_depth[depth])[current_offset_n_by_depth[depth]]);
                    if (indexes_by_depth[depth] == (*offsets_by_depth[depth])[current_offset_n_by_depth[depth]])
                    {
                        DUMP("cleartest", clear_depth - 1, depth);
                        if (clear_depth - 1 == depth)
                            want_clear = true;

                        ++current_offset_n_by_depth[depth];
                    }
                    else
                    {
                        DUMP("no levelup", depth);
                        break;
                    }
                }
            }
            else
            {
                DUMP("clearfirst test", current_offset_depth, indexes_by_depth[current_offset_depth - 1]);
            }
            //DUMP("nxt", indexes_by_depth);
            //prev_off_by_depth[current_offset_depth] = off;
            ++current_offset_n_by_depth[current_offset_depth - 1];
            DUMP(current_offset_depth, current_offset_n_by_depth);

            //DUMP(prev_off_by_depth);
            DUMP(want_clear);
            if (want_clear)
            {
                DUMP("Clear indx!");
                want_clear = false;
                indices.clear();
            }


#if 0
            //UInt32 null_count = 0;
            for (size_t j = prev_off; j < off; ++j)
            {
                DUMP(j, prev_off, off);

// offsets_by_depth[current_offset_depth]
                for (size_t col_n = 0; col_n < columns.size(); ++col_n ) {
                    indexes[col_n] = j;
                }           

/*
                if constexpr (has_null_map)
                {
                    if ((*null_map)[j])
                    {
                        res_values[j] = ++null_count;
                        continue;
                    }
                }
*/
                /*
                auto emplace_result = method.emplaceKey(indices, j, pool);
                auto idx = emplace_result.getMapped() + 1;
                emplace_result.setMapped(idx);
                */
                //auto hash = hash128depths({j, j}, /*columns.size(),*/ columns);
                auto hash = hash128depths(indexes, columns);
                auto idx = ++indices[hash];
                DUMP(j, idx, hash);


                res_values[j] = idx;
                DUMP(off, j, prev_off, res_values[j], idx);
            }
#endif
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
            //UInt32 rank = 0;
            //[[maybe_unused]] UInt32 null_index = 0;
            for (size_t j = prev_off; j < off; ++j)
            {
                DUMP(j, prev_off, off);

                /*
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
*/

                /*
                auto emplace_result = method.emplaceKey(indices, j, pool);
                auto idx = emplace_result.getMapped();
*/

                auto hash = hash128depths({j, j}, /*columns.size(),*/ columns); // TODO!
                auto idx = ++indices[hash];
                DUMP(j, idx, hash);

                /* TODO
                if (!idx)
                {
                    idx = ++rank;
                    emplace_result.setMapped(idx);
                }
*/

                res_values[j] = idx;
            }
            prev_off = off;
        }
    }
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
*/

}
