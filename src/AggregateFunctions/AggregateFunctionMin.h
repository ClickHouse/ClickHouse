#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsCommon.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>

#include <base/StringRef.h>
#include <base/defines.h>
#include <Common/TargetSpecific.h>
#include <Common/assert_cast.h>
#include <Common/config.h>

#include <algorithm>
#include <cstdint>
#include <cstddef>
#include <limits>
#include <type_traits>

#if USE_EMBEDDED_COMPILER
#    include <llvm/IR/IRBuilder.h>
#    include <DataTypes/Native.h>
#endif

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename T>
struct AggregateFunctionStandaloneMinData
{
    bool has_value = false;
    /// Using max as the initial value allows us to avoid checking if any value has been set before (has_value). Note that this doesn't
    /// work with floating point since NaN will compare incorrectly, so we deal with those manually
    T min{ std::numeric_limits<T>::max() };

    void ALWAYS_INLINE inline add(T value)
    {
        if constexpr (std::is_floating_point_v<T>)
        {
            if (!has_value)
            {
                has_value = true;
                min = value;
                return;
            }
        }
        has_value = true;
        min = std::min(min, value);
    }

    void ALWAYS_INLINE inline addManyDefaults(T value, size_t /*length*/) { add(value); }

    MULTITARGET_FUNCTION_AVX2_SSE42(
        MULTITARGET_FUNCTION_HEADER(void),
        addManyImpl,
        MULTITARGET_FUNCTION_BODY((const T * __restrict ptr, size_t row_begin, size_t row_end)
        {
            const auto * last_element = ptr + row_end;
            ptr += row_begin;

            if constexpr (std::is_floating_point_v<T>)
            {
                constexpr size_t unroll_block = 64 / sizeof(T);
                const auto * unrolled_end = ptr + ((row_end - row_begin) / unroll_block * unroll_block);

                /// Force initialization to an existing value
                chassert(row_end - row_begin);
                if (!has_value)
                    min = *ptr;

                if (ptr != unrolled_end)
                {
                    std::vector<T> partial_min(ptr, ptr + unroll_block);
                    ptr += unroll_block;

                    while (ptr < unrolled_end)
                    {
                        for (size_t i = 0; i < unroll_block; i++)
                        {
                            partial_min[i] = std::min(partial_min[i], ptr[i]);
                        }
                        ptr += unroll_block;
                    }
                    min = std::min(min, *std::min_element(partial_min.begin(), partial_min.end()));
                }
            }
            while (ptr < last_element)
            {
                min = std::min(min, *ptr);
                ptr++;
            }

            has_value = true;
        })
    )

    void addManyNotNullImpl(const T * __restrict ptr, const UInt8 * __restrict null_map, size_t row_begin, size_t row_end)
    {
        T aux{min}; /// Need an auxiliary variable for "compiler reasons", otherwise it won't use SIMD
        size_t count = row_end - row_begin;
        ptr += row_begin;
        null_map += row_begin;

        for (size_t i = 0; i < count; i++)
            if (!null_map[i])
                aux = std::min(aux, ptr[i]);

        if (!has_value)
            min = aux;
        else
            min = std::min(min, aux);
        has_value = true;
    }

    void addManyConditionalImpl(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t row_begin, size_t row_end)
    {
        T aux{min};
        size_t count = row_end - row_begin;
        ptr += row_begin;
        condition_map += row_begin;

        for (size_t i = 0; i < count; i++)
            if (condition_map[i])
                aux = std::min(aux, ptr[i]);

        if (!has_value)
            min = aux;
        else
            min = std::min(min, aux);
        has_value = true;
    }

    /// Vectorized version
    template <typename Value>
    void NO_INLINE addMany(const Value * __restrict ptr, size_t start, size_t end)
    {
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::AVX2))
        {
            addManyImplAVX2(ptr, start, end);
            return;
        }
        else if (isArchSupported(TargetArch::SSE42))
        {
            addManyImplSSE42(ptr, start, end);
            return;
        }
#endif
        addManyImpl(ptr, start, end);
    }

    template <typename Value>
    void NO_INLINE addManyNotNull(const Value * __restrict ptr, const UInt8 * __restrict null_map, size_t start, size_t end)
    {
        size_t null_count = countBytesInFilter(null_map, start, end);
        if (null_count == (end - start))
            return;

        if (null_count != 0)
            addManyNotNullImpl(ptr, null_map, start, end);
        else
            addMany(ptr, start, end);
    }

    template <typename Value>
    void NO_INLINE addManyConditional(const Value * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end)
    {
        size_t included_count = countBytesInFilter(condition_map, start, end);
        if (!included_count)
            return;

        if (included_count != (end - start))
            addManyConditionalImpl(ptr, condition_map, start, end);
        else
            addMany(ptr, start, end);
    }

    void merge(const AggregateFunctionStandaloneMinData & rhs)
    {
        if (rhs.has_value)
            add(rhs.min);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(has_value, buf);
        if (has_value)
            writeBinary(min, buf);
    }

    void read(ReadBuffer & buf)
    {
        readBinary(has_value, buf);
        if (has_value)
            readBinary(min, buf);
    }

    T get() const
    {
        return has_value ? min : T{};
    }
};


template <typename T>
class AggregateFunctionMin final : public IAggregateFunctionDataHelper<AggregateFunctionStandaloneMinData<T>, AggregateFunctionMin<T>>
{
    using ColVecType = ColumnVectorOrDecimal<T>;
public:
    explicit AggregateFunctionMin(const DataTypePtr & type)
        : IAggregateFunctionDataHelper<AggregateFunctionStandaloneMinData<T>, AggregateFunctionMin<T>>({type}, {})
    {
        if (!type->isComparable())
            throw Exception("Illegal type " + type->getName() + " of argument of aggregate function " + getName()
                + " because the values of that data type are not comparable", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    String getName() const override { return "min"; }

    DataTypePtr getReturnType() const override { return this->argument_types.at(0); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        this->data(place).add(assert_cast<const T &>(column.getData()[row_num]));
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            this->data(place).addManyConditional(column.getData().data(), flags.data(), row_begin, row_end);
        }
        else
        {
            this->data(place).addMany(column.getData().data(), row_begin, row_end);
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos = -1) const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);

        if (if_argument_pos >= 0)
        {
            const auto * if_flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();
            auto final_null_flags = std::make_unique<UInt8[]>(row_end);
            for (size_t i = row_begin; i < row_end; ++i)
                final_null_flags[i] = null_map[i] & !if_flags[i];

            this->data(place).addManyNotNull(column.getData().data(), final_null_flags.get(), row_begin, row_end);
        }
        else
        {
            this->data(place).addManyNotNull(column.getData().data(), null_map, row_begin, row_end);
        }
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t length, Arena *) const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        this->data(place).addManyDefaults(assert_cast<const T &>(column.getData()[0]), length);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).read(buf);
    }

    bool allocatesMemoryInArena() const override
    {
        return false;
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColVecType &>(to).getData().push_back(this->data(place).get());
    }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override
    {
        return false;
    }
#endif
};

}
