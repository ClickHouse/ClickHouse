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
    bool executeNumber(const ColumnArray::Offsets & offsets, const IColumn & data, const NullMap * null_map, ColumnUInt32::Container & res_values);
    bool executeString(const ColumnArray::Offsets & offsets, const IColumn & data, const NullMap * null_map, ColumnUInt32::Container & res_values);
    bool execute128bit(const ColumnArray::Offsets & offsets, const ColumnRawPtrs & columns, ColumnUInt32::Container & res_values);
    bool executeHashed(const ColumnArray::Offsets & offsets, const ColumnRawPtrs & columns, ColumnUInt32::Container & res_values);
};


void FunctionArrayUniq::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
{
    const ColumnArray::Offsets * offsets = nullptr;
    size_t num_arguments = arguments.size();
    ColumnRawPtrs data_columns(num_arguments);

    Columns array_holders;
    for (size_t i = 0; i < num_arguments; ++i)
    {
        const ColumnPtr & array_ptr = block.getByPosition(arguments[i]).column;
        const ColumnArray * array = checkAndGetColumn<ColumnArray>(array_ptr.get());
        if (!array)
        {
            const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(
                block.getByPosition(arguments[i]).column.get());
            if (!const_array)
                throw Exception("Illegal column " + block.getByPosition(arguments[i]).column->getName()
                    + " of " + toString(i + 1) + "-th argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
            array_holders.emplace_back(const_array->convertToFullColumn());
            array = checkAndGetColumn<ColumnArray>(array_holders.back().get());
        }

        const ColumnArray::Offsets & offsets_i = array->getOffsets();
        if (i == 0)
            offsets = &offsets_i;
        else if (offsets_i != *offsets)
            throw Exception("Lengths of all arrays passed to " + getName() + " must be equal.",
                ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);

        auto * array_data = &array->getData();
        data_columns[i] = array_data;
    }

    const NullMap * null_map = nullptr;

    for (size_t i = 0; i < num_arguments; ++i)
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

    auto res = ColumnUInt32::create();
    ColumnUInt32::Container & res_values = res->getData();
    res_values.resize(offsets->size());

    if (num_arguments == 1)
    {
        executeNumber<UInt8>(*offsets, *data_columns[0], null_map, res_values)
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
            || executeHashed(*offsets, data_columns, res_values);
    }
    else
    {
        execute128bit(*offsets, data_columns, res_values)
            || executeHashed(*offsets, data_columns, res_values);
    }

    block.getByPosition(result).column = std::move(res);
}

template <typename T>
bool FunctionArrayUniq::executeNumber(const ColumnArray::Offsets & offsets, const IColumn & data, const NullMap * null_map, ColumnUInt32::Container & res_values)
{
    const ColumnVector<T> * nested = checkAndGetColumn<ColumnVector<T>>(&data);
    if (!nested)
        return false;
    const auto & values = nested->getData();

    using Set = ClearableHashSet<T, DefaultHash<T>, HashTableGrower<INITIAL_SIZE_DEGREE>,
        HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(T)>>;

    Set set;
    ColumnArray::Offset prev_off = 0;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        set.clear();
        bool found_null = false;
        ColumnArray::Offset off = offsets[i];
        for (ColumnArray::Offset j = prev_off; j < off; ++j)
        {
            if (null_map && (*null_map)[j])
                found_null = true;
            else
                set.insert(values[j]);
        }

        res_values[i] = set.size() + found_null;
        prev_off = off;
    }
    return true;
}

bool FunctionArrayUniq::executeString(const ColumnArray::Offsets & offsets, const IColumn & data, const NullMap * null_map, ColumnUInt32::Container & res_values)
{
    const ColumnString * nested = checkAndGetColumn<ColumnString>(&data);
    if (!nested)
        return false;

    using Set = ClearableHashSet<StringRef, StringRefHash, HashTableGrower<INITIAL_SIZE_DEGREE>,
        HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(StringRef)>>;

    Set set;
    ColumnArray::Offset prev_off = 0;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        set.clear();
        bool found_null = false;
        ColumnArray::Offset off = offsets[i];
        for (ColumnArray::Offset j = prev_off; j < off; ++j)
        {
            if (null_map && (*null_map)[j])
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

    using Set = ClearableHashSet<UInt128, UInt128HashCRC32, HashTableGrower<INITIAL_SIZE_DEGREE>,
        HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(UInt128)>>;

    Set set;
    ColumnArray::Offset prev_off = 0;
    for (ColumnArray::Offset i = 0; i < offsets.size(); ++i)
    {
        set.clear();
        ColumnArray::Offset off = offsets[i];
        for (ColumnArray::Offset j = prev_off; j < off; ++j)
            set.insert(packFixed<UInt128>(j, count, columns, key_sizes));

        res_values[i] = set.size();
        prev_off = off;
    }

    return true;
}

bool FunctionArrayUniq::executeHashed(
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

    return true;
}


void registerFunctionArrayUniq(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayUniq>();
}

}
