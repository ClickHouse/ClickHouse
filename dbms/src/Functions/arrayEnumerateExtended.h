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

template <typename Derived>
class FunctionArrayEnumerateExtended : public IFunction
{
public:
    static FunctionPtr create(const Context &) { return std::make_shared<Derived>(); }

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

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].get());
            if (!array_type)
                throw Exception("All arguments for function " + getName() + " must be arrays but argument " +
                    toString(i + 1) + " has type " + arguments[i]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;

private:
    /// Initially allocate a piece of memory for 512 elements. NOTE: This is just a guess.
    static constexpr size_t INITIAL_SIZE_DEGREE = 9;

    template <typename T>
    bool executeNumber(const ColumnArray * array, const IColumn * null_map, ColumnUInt32::Container & res_values);

    bool executeString(const ColumnArray * array, const IColumn * null_map, ColumnUInt32::Container & res_values);

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


template <typename Derived>
void FunctionArrayEnumerateExtended<Derived>::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
{
    const ColumnArray::Offsets * offsets = nullptr;
    ColumnRawPtrs data_columns;
    data_columns.reserve(arguments.size());

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

        const ColumnArray::Offsets & offsets_i = array->getOffsets();
        if (i == 0)
            offsets = &offsets_i;
        else if (offsets_i != *offsets)
            throw Exception("Lengths of all arrays passed to " + getName() + " must be equal.",
                ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);

        auto * array_data = &array->getData();
        data_columns.push_back(array_data);
    }

    size_t num_columns = data_columns.size();
    ColumnRawPtrs original_data_columns(num_columns);
    ColumnRawPtrs null_maps(num_columns);

    for (size_t i = 0; i < num_columns; ++i)
    {
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

    const ColumnArray * first_array = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments.at(0)).column.get());
    const IColumn * first_null_map = null_maps[0];
    auto res_nested = ColumnUInt32::create();

    ColumnUInt32::Container & res_values = res_nested->getData();
    if (!offsets->empty())
        res_values.resize(offsets->back());

    if (num_columns == 1)
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
            executeHashed(*offsets, original_data_columns, res_values);
    }
    else
    {
        if (!execute128bit(*offsets, data_columns, null_maps, res_values, has_nullable_columns))
            executeHashed(*offsets, original_data_columns, res_values);
    }

    block.getByPosition(result).column = ColumnArray::create(std::move(res_nested), first_array->getOffsetsPtr());
}


template <typename Derived>
template <typename T>
bool FunctionArrayEnumerateExtended<Derived>::executeNumber(const ColumnArray * array, const IColumn * null_map, ColumnUInt32::Container & res_values)
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
                if (null_map_data && ((*null_map_data)[j] == 1))
                    res_values[j] = ++null_count;
                else
                    res_values[j] = ++indices[values[j]];
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
                if (null_map_data && ((*null_map_data)[j] == 1))
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
    return true;
}

template <typename Derived>
bool FunctionArrayEnumerateExtended<Derived>::executeString(const ColumnArray * array, const IColumn * null_map, ColumnUInt32::Container & res_values)
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
                if (null_map_data && ((*null_map_data)[j] == 1))
                    res_values[j] = ++null_count;
                else
                    res_values[j] = ++indices[nested->getDataAt(j)];
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
                if (null_map_data && ((*null_map_data)[j] == 1))
                {
                    if (!null_index)
                        null_index = ++rank;
                    res_values[j] = null_index;
                }
                else
                {
                    auto & idx = indices[nested->getDataAt(j)];
                    if (!idx)
                        idx = ++rank;
                    res_values[j] = idx;
                }
            }
            prev_off = off;
        }
    }
    return true;
}

template <typename Derived>
bool FunctionArrayEnumerateExtended<Derived>::execute128bit(
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
    if constexpr (std::is_same_v<Derived, FunctionArrayEnumerateUniq>)
    {
        // Unique
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
                    auto &idx = indices[packFixed<UInt128>(j, count, columns, key_sizes, bitmap)];
                    if (!idx)
                        idx = ++rank;
                    res_values[j] = idx;
                }
                else
                {
                    auto &idx = indices[packFixed<UInt128>(j, count, columns, key_sizes)];;
                    if (!idx)
                        idx = ++rank;
                    res_values[j] = idx;
                }
            }
            prev_off = off;
        }
    }

    return true;
}

template <typename Derived>
void FunctionArrayEnumerateExtended<Derived>::executeHashed(
    const ColumnArray::Offsets & offsets,
    const ColumnRawPtrs & columns,
    ColumnUInt32::Container & res_values)
{
    size_t count = columns.size();

    using ValuesToIndices = ClearableHashMap<UInt128, UInt32, UInt128TrivialHash, HashTableGrower<INITIAL_SIZE_DEGREE>,
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
            {
                res_values[j] = ++indices[hash128(j, count, columns)];
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
            size_t off = offsets[i];
            size_t rank = 0;
            for (size_t j = prev_off; j < off; ++j)
            {
                auto & idx = indices[hash128(j, count, columns)];
                if (!idx)
                    idx = ++rank;
                res_values[j] = idx;
            }
            prev_off = off;
        }
    }
}

}
