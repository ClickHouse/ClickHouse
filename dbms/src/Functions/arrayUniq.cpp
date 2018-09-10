#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Common/HashTable/ClearableHashSet.h>
#include <Interpreters/AggregationCommon.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// Counts the number of different elements in the array, or the number of different tuples from the elements at the corresponding positions in several arrays.
/// NOTE The implementation partially matches arrayEnumerateUniq.
class FunctionArrayUniq : public IFunction
{
public:
    static constexpr auto name = "arrayUniq";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionArrayUniq>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }

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

        return std::make_shared<DataTypeUInt32>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;

private:
    /// Initially allocate a piece of memory for 512 elements. NOTE: This is just a guess.
    static constexpr size_t INITIAL_SIZE_DEGREE = 9;

    template <typename T>
    bool executeNumber(const ColumnArray * array,  const IColumn * null_map, ColumnUInt32::Container & res_values);

    bool executeString(const ColumnArray * array,  const IColumn * null_map, ColumnUInt32::Container & res_values);

    bool execute128bit(
        const ColumnArray::Offsets & offsets,
        const ColumnRawPtrs & columns,
        const ColumnRawPtrs & null_maps,
        ColumnUInt32::Container & res_values,
        bool has_nullable_columns);

    void executeHashed(
        const ColumnArray::Offsets & offsets,
        const ColumnRawPtrs & columns,
        ColumnUInt32::Container & res_values);
};


void FunctionArrayUniq::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
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
            executeHashed(*offsets, original_data_columns, res_values);
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
    ColumnArray::Offset prev_off = 0;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        set.clear();
        bool found_null = false;
        ColumnArray::Offset off = offsets[i];
        for (ColumnArray::Offset j = prev_off; j < off; ++j)
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
    ColumnArray::Offset prev_off = 0;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        set.clear();
        bool found_null = false;
        ColumnArray::Offset off = offsets[i];
        for (ColumnArray::Offset j = prev_off; j < off; ++j)
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
    ColumnArray::Offset prev_off = 0;
    for (ColumnArray::Offset i = 0; i < offsets.size(); ++i)
    {
        set.clear();
        ColumnArray::Offset off = offsets[i];
        for (ColumnArray::Offset j = prev_off; j < off; ++j)
        {
            if (has_nullable_columns)
            {
                KeysNullMap<UInt128> bitmap{};

                for (ColumnArray::Offset i = 0; i < columns.size(); ++i)
                {
                    if (null_maps[i])
                    {
                        const auto & null_map = static_cast<const ColumnUInt8 &>(*null_maps[i]).getData();
                        if (null_map[j] == 1)
                        {
                            ColumnArray::Offset bucket = i / 8;
                            ColumnArray::Offset offset = i % 8;
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
    ColumnArray::Offset prev_off = 0;
    for (ColumnArray::Offset i = 0; i < offsets.size(); ++i)
    {
        set.clear();
        ColumnArray::Offset off = offsets[i];
        for (ColumnArray::Offset j = prev_off; j < off; ++j)
            set.insert(hash128(j, count, columns));

        res_values[i] = set.size();
        prev_off = off;
    }
}


void registerFunctionArrayUniq(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayUniq>();
}

}
