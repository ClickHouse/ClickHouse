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
//#include <Functions/FunctionFactory.h>


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
        throw Exception( "Arguments for function arrayEnumerateUniqRanked incorrect: clear_depth=" + std::to_string(clear_depth)+ " cant be larger than max_array_depth=" + std::to_string(max_array_depth) + ".", ErrorCodes::BAD_ARGUMENTS);

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

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() == 0)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be at least 1.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DepthType clear_depth;
        DepthType max_array_depth;
        DepthTypes depths;

        std::tie(clear_depth, depths, max_array_depth) = getDepths(arguments);

        DataTypePtr type = std::make_shared<DataTypeUInt32>();
        for (DepthType i = 0; i < max_array_depth; ++i)
            type = std::make_shared<DataTypeArray>(type);

        return type;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;

private:
    const Context & context;
    /// Initially allocate a piece of memory for 512 elements. NOTE: This is just a guess.
    static constexpr size_t INITIAL_SIZE_DEGREE = 9;

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

    template <typename Method, bool has_null_map>
    void executeMethodImpl(
        const std::vector<const ColumnArray::Offsets *> & offsets_by_depth,
        const ColumnRawPtrs & columns,
        DepthType clear_depth,
        DepthType max_array_depth,
        DepthTypes depths,
        ColumnUInt32::Container & res_values);

};


/// Hash a set of keys into a UInt128 value.
static inline UInt128 ALWAYS_INLINE hash128depths(std::vector<size_t> indexes, /*size_t keys_size,*/ const ColumnRawPtrs & key_columns)
{
    UInt128 key;
    SipHash hash;

    for (size_t j = 0, keys_size = key_columns.size(); j < keys_size; ++j)
    {
        //const auto & field = (*key_columns[j])[indexes[j]];
        //DUMP(j, indexes[j], field);
        key_columns[j]->updateHashWithValue(indexes[j], hash);
    }

    hash.get128(key.low, key.high);

    return key;
}


template <typename Derived>
void FunctionArrayEnumerateRankedExtended<Derived>::executeImpl(
    Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
{
    size_t num_arguments = arguments.size();
    ColumnRawPtrs data_columns;

    Columns array_holders;
    ColumnPtr offsets_column;

    ColumnsWithTypeAndName args;

    for (size_t i = 0; i < arguments.size(); ++i)
        args.emplace_back(block.getByPosition(arguments[i]));

    DepthType clear_depth;
    DepthType max_array_depth;
    DepthTypes depths;

    std::tie(clear_depth, depths, max_array_depth) = getDepths(args);

    auto get_array_column = [&](const auto & column) -> const DB::ColumnArray * {
        const ColumnArray * array = checkAndGetColumn<ColumnArray>(column);
        if (!array)
        {
            const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(column);
            if (!const_array)
                return nullptr;
            array_holders.emplace_back(const_array->convertToFullColumn());
            array = checkAndGetColumn<ColumnArray>(array_holders.back().get());
        }
        return array;
    };

    std::vector<const ColumnArray::Offsets *> offsets_by_depth;
    std::vector<ColumnPtr> offsetsptr_by_depth; // TODO make one !

    size_t array_num = 0;
    for (size_t i = 0; i < num_arguments; ++i)
    {
        const ColumnPtr & array_ptr = block.getByPosition(arguments[i]).column;
        const ColumnArray * array = checkAndGetColumn<ColumnArray>(array_ptr.get());
        if (!array)
        {
            const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(block.getByPosition(arguments[i]).column.get());
            if (!const_array)
                continue;
            array_holders.emplace_back(const_array->convertToFullColumn());
            array = checkAndGetColumn<ColumnArray>(array_holders.back().get());
        }

        if (array_num == 0) // TODO check with prev
        {
            offsets_by_depth.emplace_back(&array->getOffsets());
            offsetsptr_by_depth.emplace_back(array->getOffsetsPtr());
        }

        for (DepthType col_depth = 1; col_depth < depths[array_num]; ++col_depth)
        {

/*
            if (col_depth == max_array_depth)
            {
                offsets_column = array->getOffsetsPtr();
            }
*/

            auto sub_array = get_array_column(&array->getData());
            if (sub_array)
                array = sub_array;
            if (!sub_array)
                break;

            if (offsets_by_depth.size() <= col_depth)
            {
                offsets_by_depth.emplace_back(&array->getOffsets());
                offsetsptr_by_depth.emplace_back(array->getOffsetsPtr());
            }
        }

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

    auto res_nested = ColumnUInt32::create();

    ColumnUInt32::Container & res_values = res_nested->getData();
    res_values.resize(offsets_by_depth[max_array_depth - 1]->back()); // todo size check?

    executeMethodImpl<MethodHashed, false>(offsets_by_depth, data_columns, clear_depth, max_array_depth, depths, res_values);

    ColumnPtr result_nested_array = std::move(res_nested);
    for (int depth = max_array_depth - 1; depth >= 0; --depth)
        result_nested_array = ColumnArray::create(std::move(result_nested_array), offsetsptr_by_depth[depth]);

    block.getByPosition(result).column = result_nested_array;
}


template <typename Derived>
template <typename Method, bool has_null_map>
void FunctionArrayEnumerateRankedExtended<Derived>::executeMethodImpl(
    const std::vector<const ColumnArray::Offsets *> & offsets_by_depth,
    const ColumnRawPtrs & columns,
    DepthType clear_depth,
    DepthType max_array_depth,
     DepthTypes depths,
    ColumnUInt32::Container & res_values)
{
    const size_t current_offset_depth = max_array_depth;
    const auto & offsets = *offsets_by_depth[current_offset_depth - 1]; //->getData(); //depth!

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

. -  get data
; - clean index

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

    std::vector<size_t> indexes_by_depth(max_array_depth);
    std::vector<size_t> current_offset_n_by_depth(max_array_depth);


    if constexpr (std::is_same_v<Derived, FunctionArrayEnumerateUniqRanked>)
    {
        // Unique
        for (size_t off : offsets)
        {

            std::vector<size_t> indexes(columns.size());

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

                //++indexes_by_depth[current_offset_depth - 1];
                //DUMP("++", j, indexes_by_depth[current_offset_depth - 1], current_offset_depth);
                //DUMP("i", indexes_by_depth);
                //}

                //if (current_offset_depth >= 2)
                //DUMP("inc?", j, off);
                //if (j < off-1)
                {
                    //for (int depth = current_offset_depth - 2; depth >= 0; --depth)
                    for (int depth = current_offset_depth - 1; depth >= 0; --depth)
                    { // TODO CHECK SIZE
                        ++indexes_by_depth[depth];

                        if (indexes_by_depth[depth] == (*offsets_by_depth[depth])[current_offset_n_by_depth[depth]])
                        {
                            if (static_cast<int>(clear_depth - 1) == depth)
                                want_clear = true;
                            ++current_offset_n_by_depth[depth];
                        }
                        else
                        {
                            break;
                        }
                    }
                }
            }
            if (want_clear)
            {
                want_clear = false;
                indices.clear();
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

}
