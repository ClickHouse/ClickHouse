#pragma once
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>
#include <Interpreters/AggregationCommon.h>
#include <Common/ColumnsHashing.h>
#include <Common/HashTable/ClearableHashMap.h>

// for better debug: #include <Core/iostream_debug_helpers.h>

/** The function will enumerate distinct values of the passed multidimensional arrays looking inside at the specified depths.
  * This is very unusual function made as a special order for Yandex.Metrica.
  *
  * arrayEnumerateUniqRanked(['hello', 'world', 'hello']) = [1, 1, 2]
  * - it returns similar structured array containing number of occurrence of the corresponding value.
  *
  * arrayEnumerateUniqRanked([['hello', 'world'], ['hello'], ['hello']], 1) = [1, 1, 2]
  * - look at the depth 1 by default. Elements are ['hello', 'world'], ['hello'], ['hello'].
  *
  * arrayEnumerateUniqRanked([['hello', 'world'], ['hello'], ['hello']]) = [[1,1],[2],[3]]
  * - look at the depth 2. Return similar structured array.
  * arrayEnumerateUniqRanked([['hello', 'world'], ['hello'], ['hello']], 2) = [[1,1],[2],[3]]
  * - look at the maximum depth by default.
  *
  * We may pass multiple array arguments. Their elements will be processed as zipped to tuple.
  *
  * arrayEnumerateUniqRanked(['hello', 'hello', 'world', 'world'], ['a', 'b', 'b', 'b']) = [1, 1, 1, 2]
  *
  * We may provide arrays of different depths to look at different arguments.
  *
  * arrayEnumerateUniqRanked([['hello', 'world'], ['hello'], ['world'], ['world']], ['a', 'b', 'b', 'b']) = [[1,1],[1],[1],[2]]
  * arrayEnumerateUniqRanked([['hello', 'world'], ['hello'], ['world'], ['world']], 1, ['a', 'b', 'b', 'b'], 1) = [1, 1, 1, 2]
  *
  * When depths are different, we process less deep arrays as promoted to deeper arrays of similar structure by duplicating elements.
  *
  * arrayEnumerateUniqRanked(
  *     [['hello', 'world'], ['hello'], ['world'], ['world']],
  *     ['a', 'b', 'b', 'b'])
  * = arrayEnumerateUniqRanked(
  *     [['hello', 'world'], ['hello'], ['world'], ['world']],
  *     [['a', 'a'], ['b'], ['b'], ['b']])
  *
  * Finally, we can provide extra first argument named "clear_depth" (it can be considered as 1 by default).
  * Array elements at the clear_depth will be enumerated as separate elements (enumeration counter is reset for each new element).
  *
  * SELECT arrayEnumerateUniqRanked(1, [['hello', 'world'], ['hello'], ['world'], ['world']]) = [[1,1],[2],[2],[3]]
  * SELECT arrayEnumerateUniqRanked(2, [['hello', 'world'], ['hello'], ['world'], ['world']]) = [[1,1],[1],[1],[1]]
  * SELECT arrayEnumerateUniqRanked(1, [['hello', 'world', 'hello'], ['hello'], ['world'], ['world']]) = [[1,1,2],[3],[2],[3]]
  * SELECT arrayEnumerateUniqRanked(2, [['hello', 'world', 'hello'], ['hello'], ['world'], ['world']]) = [[1,1,2],[1],[1],[1]]
  */

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
}

class FunctionArrayEnumerateUniqRanked;
class FunctionArrayEnumerateDenseRanked;

using DepthType = uint32_t;
using DepthTypes = std::vector<DepthType>;

struct ArraysDepths
{
    /// Enumerate elements at the specified level separately.
    DepthType clear_depth;

    /// Effective depth is the array depth by default or lower value, specified as a constant argument following the array.
    /// f([[1, 2], [3]]) - effective depth is 2.
    /// f([[1, 2], [3]], 1) - effective depth is 1.
    DepthTypes depths;

    /// Maximum effective depth.
    DepthType max_array_depth;
};

/// Return depth info about passed arrays
ArraysDepths getArraysDepths(const ColumnsWithTypeAndName & arguments);

template <typename Derived>
class FunctionArrayEnumerateRankedExtended : public IFunction
{
public:
    static FunctionPtr create(const Context & /* context */) { return std::make_shared<Derived>(); }

    String getName() const override { return Derived::name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty())
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + std::to_string(arguments.size())
                    + ", should be at least 1.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const ArraysDepths arrays_depths = getArraysDepths(arguments);

        /// Return type is the array of the depth as the maximum effective depth of arguments, containing UInt32.

        DataTypePtr type = std::make_shared<DataTypeUInt32>();
        for (DepthType i = 0; i < arrays_depths.max_array_depth; ++i)
            type = std::make_shared<DataTypeArray>(type);

        return type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

private:
    /// Initially allocate a piece of memory for 64 elements. NOTE: This is just a guess.
    static constexpr size_t INITIAL_SIZE_DEGREE = 6;

    void executeMethodImpl(
        const std::vector<const ColumnArray::Offsets *> & offsets_by_depth,
        const ColumnRawPtrs & columns,
        const ArraysDepths & arrays_depths,
        ColumnUInt32::Container & res_values) const;
};


/// Hash a set of keys into a UInt128 value.
static inline UInt128 ALWAYS_INLINE hash128depths(const std::vector<size_t> & indices, const ColumnRawPtrs & key_columns)
{
    UInt128 key;
    SipHash hash;

    for (size_t j = 0, keys_size = key_columns.size(); j < keys_size; ++j)
    {
        // Debug: const auto & field = (*key_columns[j])[indices[j]]; DUMP(j, indices[j], field);
        key_columns[j]->updateHashWithValue(indices[j], hash);
    }

    hash.get128(key.low, key.high);

    return key;
}


template <typename Derived>
ColumnPtr FunctionArrayEnumerateRankedExtended<Derived>::executeImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const
{
    size_t num_arguments = arguments.size();
    ColumnRawPtrs data_columns;

    Columns array_holders;
    ColumnPtr offsets_column;

    const ArraysDepths arrays_depths = getArraysDepths(arguments);

    /// If the column is Array - return it. If the const Array - materialize it, keep ownership and return.
    auto get_array_column = [&](const auto & column) -> const DB::ColumnArray *
    {
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
    std::vector<ColumnPtr> offsetsptr_by_depth;

    size_t array_num = 0;
    for (size_t i = 0; i < num_arguments; ++i)
    {
        const auto * array = get_array_column(arguments[i].column.get());
        if (!array)
            continue;

        if (array_num == 0) // TODO check with prev
        {
            offsets_by_depth.emplace_back(&array->getOffsets());
            offsetsptr_by_depth.emplace_back(array->getOffsetsPtr());
        }
        else
        {
            if (*offsets_by_depth[0] != array->getOffsets())
            {
                throw Exception(
                    "Lengths and effective depths of all arrays passed to " + getName() + " must be equal.",
                    ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
            }
        }

        DepthType col_depth = 1;
        for (; col_depth < arrays_depths.depths[array_num]; ++col_depth)
        {
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
            else
            {
                if (*offsets_by_depth[col_depth] != array->getOffsets())
                {
                    throw Exception(
                        "Lengths and effective depths of all arrays passed to " + getName() + " must be equal.",
                        ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
                }
            }
        }

        if (col_depth < arrays_depths.depths[array_num])
        {
            throw Exception(
                getName() + ": Passed array number " + std::to_string(array_num) + " depth ("
                    + std::to_string(arrays_depths.depths[array_num]) + ") is more than the actual array depth ("
                    + std::to_string(col_depth) + ").",
                ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
        }

        auto * array_data = &array->getData();
        data_columns.emplace_back(array_data);
        ++array_num;
    }

    if (offsets_by_depth.empty())
        throw Exception("No arrays passed to function " + getName(), ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    auto res_nested = ColumnUInt32::create();

    ColumnUInt32::Container & res_values = res_nested->getData();
    res_values.resize(offsets_by_depth[arrays_depths.max_array_depth - 1]->back());

    executeMethodImpl(offsets_by_depth, data_columns, arrays_depths, res_values);

    ColumnPtr result_nested_array = std::move(res_nested);
    for (ssize_t depth = arrays_depths.max_array_depth - 1; depth >= 0; --depth)
        result_nested_array = ColumnArray::create(std::move(result_nested_array), offsetsptr_by_depth[depth]);

    return result_nested_array;
}

/*

(2, [[1,2,3],[2,2,1],[3]], 2, [4,5,6], 1)
    ; 1 2 3;  2 2 1;  3        4 5 6
    ; 4 4 4;  5 5 5;  6      <-

(1, [[1,2,3],[2,2,1],[3]], 1, [4,5,6], 1)
    ;[1,2,3] [2,2,1] [3]       4 5 6
    ;4       5       6       <-

(1, [[1,2,3],[2,2,1],[3]], 1, [4,5,6], 0)
    ;[1,2,3] [2,2,1] [3]       4 5 6
    ;[4,5,6] [4,5,6] [4,5,6] <-

. - get data
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

template <typename Derived>
void FunctionArrayEnumerateRankedExtended<Derived>::executeMethodImpl(
    const std::vector<const ColumnArray::Offsets *> & offsets_by_depth,
    const ColumnRawPtrs & columns,
    const ArraysDepths & arrays_depths,
    ColumnUInt32::Container & res_values) const
{
    /// Offsets at the depth we want to look.
    const size_t depth_to_look = arrays_depths.max_array_depth;
    const auto & offsets = *offsets_by_depth[depth_to_look - 1];

    using Map = ClearableHashMapWithStackMemory<UInt128, UInt32,
        UInt128TrivialHash, INITIAL_SIZE_DEGREE>;

    Map indices;

    std::vector<size_t> indices_by_depth(depth_to_look);
    std::vector<size_t> current_offset_n_by_depth(depth_to_look);
    std::vector<size_t> last_offset_by_depth(depth_to_look, 0); // For skipping empty arrays

    /// For arrayEnumerateDense variant: to calculate every distinct value.
    UInt32 rank = 0;

    std::vector<size_t> columns_indices(columns.size());

    /// For each array at the depth we want to look.
    ColumnArray::Offset prev_off = 0;
    for (size_t off : offsets)
    {
        bool want_clear = false;

        /// Skipping offsets if no data in this array
        if (prev_off == off)
        {
            if (depth_to_look >= 2)
            {
                /// Advance to the next element of the parent array.
                for (ssize_t depth = depth_to_look - 2; depth >= 0; --depth)
                {
                    /// Skipping offsets for empty arrays
                    while (last_offset_by_depth[depth] == (*offsets_by_depth[depth])[current_offset_n_by_depth[depth]])
                    {
                        ++current_offset_n_by_depth[depth];
                    }

                    ++indices_by_depth[depth];

                    if (indices_by_depth[depth] == (*offsets_by_depth[depth])[current_offset_n_by_depth[depth]])
                    {
                        last_offset_by_depth[depth] = (*offsets_by_depth[depth])[current_offset_n_by_depth[depth]];
                        ++current_offset_n_by_depth[depth];
                        want_clear = true;
                    }
                    else
                    {
                        break;
                    }
                }
            }
        }

        /// For each element at the depth we want to look.
        for (size_t j = prev_off; j < off; ++j)
        {
            for (size_t col_n = 0; col_n < columns.size(); ++col_n)
                columns_indices[col_n] = indices_by_depth[arrays_depths.depths[col_n] - 1];

            auto hash = hash128depths(columns_indices, columns);

            if constexpr (std::is_same_v<Derived, FunctionArrayEnumerateUniqRanked>)
            {
                auto idx = ++indices[hash];
                res_values[j] = idx;
            }
            else // FunctionArrayEnumerateDenseRanked
            {
                auto idx = indices[hash];
                if (!idx)
                {
                    idx = ++rank;
                    indices[hash] = idx;
                }
                res_values[j] = idx;
            }

            // Debug: DUMP(off, prev_off, j, columns_indices, res_values[j], columns);

            for (ssize_t depth = depth_to_look - 1; depth >= 0; --depth)
            {
                /// Skipping offsets for empty arrays
                while (last_offset_by_depth[depth] == (*offsets_by_depth[depth])[current_offset_n_by_depth[depth]])
                {
                    ++current_offset_n_by_depth[depth];
                }

                ++indices_by_depth[depth];

                if (indices_by_depth[depth] == (*offsets_by_depth[depth])[current_offset_n_by_depth[depth]])
                {
                    if (static_cast<int>(arrays_depths.clear_depth) == depth + 1)
                        want_clear = true;
                    last_offset_by_depth[depth] = (*offsets_by_depth[depth])[current_offset_n_by_depth[depth]];
                    ++current_offset_n_by_depth[depth];
                }
                else
                {
                    break;
                }
            }
        }

        if (want_clear)
        {
            want_clear = false;
            indices.clear();
            rank = 0;
        }

        prev_off = off;
    }
}

}
