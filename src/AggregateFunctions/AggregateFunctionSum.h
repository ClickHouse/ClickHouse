#pragma once

#include <type_traits>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{

template <typename T>
struct AggregateFunctionSumData
{
    T sum{};

    void ALWAYS_INLINE add(T value)
    {
        sum += value;
    }

    /// Vectorized version
    template <typename Value>
    void NO_INLINE addMany(const Value * __restrict ptr, size_t count)
    {
        /// Compiler cannot unroll this loop, do it manually.
        /// (at least for floats, most likely due to the lack of -fassociative-math)

        /// Something around the number of SSE registers * the number of elements fit in register.
        constexpr size_t unroll_count = 128 / sizeof(T);
        T partial_sums[unroll_count]{};

        const auto * end = ptr + count;
        const auto * unrolled_end = ptr + (count / unroll_count * unroll_count);

        while (ptr < unrolled_end)
        {
            for (size_t i = 0; i < unroll_count; ++i)
                partial_sums[i] += ptr[i];
            ptr += unroll_count;
        }

        for (size_t i = 0; i < unroll_count; ++i)
            sum += partial_sums[i];

        while (ptr < end)
        {
            sum += *ptr;
            ++ptr;
        }
    }

    template <typename Value>
    void NO_INLINE addManyNotNull(const Value * __restrict ptr, const UInt8 * __restrict null_map, size_t count)
    {
        constexpr size_t unroll_count = 128 / sizeof(T);
        T partial_sums[unroll_count]{};

        const auto * end = ptr + count;
        const auto * unrolled_end = ptr + (count / unroll_count * unroll_count);

        while (ptr < unrolled_end)
        {
            for (size_t i = 0; i < unroll_count; ++i)
                if (!null_map[i])
                    partial_sums[i] += ptr[i];
            ptr += unroll_count;
            null_map += unroll_count;
        }

        for (size_t i = 0; i < unroll_count; ++i)
            sum += partial_sums[i];

        while (ptr < end)
        {
            if (!*null_map)
                sum += *ptr;
            ++ptr;
            ++null_map;
        }
    }

    void merge(const AggregateFunctionSumData & rhs)
    {
        sum += rhs.sum;
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(sum, buf);
    }

    void read(ReadBuffer & buf)
    {
        readBinary(sum, buf);
    }

    T get() const
    {
        return sum;
    }
};

template <typename T>
struct AggregateFunctionSumKahanData
{
    static_assert(std::is_floating_point_v<T>,
        "It doesn't make sense to use Kahan Summation algorithm for non floating point types");

    T sum{};
    T compensation{};

    template <typename Value>
    void ALWAYS_INLINE addImpl(Value value, T & out_sum, T & out_compensation)
    {
        auto compensated_value = static_cast<T>(value) - out_compensation;
        auto new_sum = out_sum + compensated_value;
        out_compensation = (new_sum - out_sum) - compensated_value;
        out_sum = new_sum;
    }

    void ALWAYS_INLINE add(T value)
    {
        addImpl(value, sum, compensation);
    }

    /// Vectorized version
    template <typename Value>
    void NO_INLINE addMany(const Value * __restrict ptr, size_t count)
    {
        /// Less than in ordinary sum, because the algorithm is more complicated and too large loop unrolling is questionable.
        /// But this is just a guess.
        constexpr size_t unroll_count = 4;
        T partial_sums[unroll_count]{};
        T partial_compensations[unroll_count]{};

        const auto * end = ptr + count;
        const auto * unrolled_end = ptr + (count / unroll_count * unroll_count);

        while (ptr < unrolled_end)
        {
            for (size_t i = 0; i < unroll_count; ++i)
                addImpl(ptr[i], partial_sums[i], partial_compensations[i]);
            ptr += unroll_count;
        }

        for (size_t i = 0; i < unroll_count; ++i)
            mergeImpl(sum, compensation, partial_sums[i], partial_compensations[i]);

        while (ptr < end)
        {
            addImpl(*ptr, sum, compensation);
            ++ptr;
        }
    }

    template <typename Value>
    void NO_INLINE addManyNotNull(const Value * __restrict ptr, const UInt8 * __restrict null_map, size_t count)
    {
        constexpr size_t unroll_count = 4;
        T partial_sums[unroll_count]{};
        T partial_compensations[unroll_count]{};

        const auto * end = ptr + count;
        const auto * unrolled_end = ptr + (count / unroll_count * unroll_count);

        while (ptr < unrolled_end)
        {
            for (size_t i = 0; i < unroll_count; ++i)
                if (!null_map[i])
                    addImpl(ptr[i], partial_sums[i], partial_compensations[i]);
            ptr += unroll_count;
            null_map += unroll_count;
        }

        for (size_t i = 0; i < unroll_count; ++i)
            mergeImpl(sum, compensation, partial_sums[i], partial_compensations[i]);

        while (ptr < end)
        {
            if (!*null_map)
                addImpl(*ptr, sum, compensation);
            ++ptr;
            ++null_map;
        }
    }

    void ALWAYS_INLINE mergeImpl(T & to_sum, T & to_compensation, T from_sum, T from_compensation)
    {
        auto raw_sum = to_sum + from_sum;
        auto rhs_compensated = raw_sum - to_sum;
        /// Kahan summation is tricky because it depends on non-associativity of float arithmetic.
        /// Do not simplify this expression if you are not sure.
        auto compensations = ((from_sum - rhs_compensated) + (to_sum - (raw_sum - rhs_compensated))) + compensation + from_compensation;
        to_sum = raw_sum + compensations;
        to_compensation = compensations - (to_sum - raw_sum);
    }

    void merge(const AggregateFunctionSumKahanData & rhs)
    {
        mergeImpl(sum, compensation, rhs.sum, rhs.compensation);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(sum, buf);
        writeBinary(compensation, buf);
    }

    void read(ReadBuffer & buf)
    {
        readBinary(sum, buf);
        readBinary(compensation, buf);
    }

    T get() const
    {
        return sum;
    }
};


enum AggregateFunctionSumType
{
    AggregateFunctionTypeSum,
    AggregateFunctionTypeSumWithOverflow,
    AggregateFunctionTypeSumKahan,
};
/// Counts the sum of the numbers.
template <typename T, typename TResult, typename Data, AggregateFunctionSumType Type>
class AggregateFunctionSum final : public IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data, Type>>
{
public:
    using ResultDataType = std::conditional_t<IsDecimalNumber<T>, DataTypeDecimal<TResult>, DataTypeNumber<TResult>>;
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    using ColVecResult = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<TResult>, ColumnVector<TResult>>;

    String getName() const override
    {
        if constexpr (Type == AggregateFunctionTypeSum)
            return "sum";
        else if constexpr (Type == AggregateFunctionTypeSumWithOverflow)
            return "sumWithOverflow";
        else if constexpr (Type == AggregateFunctionTypeSumKahan)
            return "sumKahan";
        __builtin_unreachable();
    }

    AggregateFunctionSum(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data, Type>>(argument_types_, {})
        , scale(0)
    {}

    AggregateFunctionSum(const IDataType & data_type, const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data, Type>>(argument_types_, {})
        , scale(getDecimalScale(data_type))
    {}

    DataTypePtr getReturnType() const override
    {
        if constexpr (IsDecimalNumber<T>)
            return std::make_shared<ResultDataType>(ResultDataType::maxPrecision(), scale);
        else
            return std::make_shared<ResultDataType>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = static_cast<const ColVecType &>(*columns[0]);
        if constexpr (is_big_int_v<T>)
            this->data(place).add(static_cast<TResult>(column.getData()[row_num]));
        else
            this->data(place).add(column.getData()[row_num]);
    }

    /// Vectorized version when there is no GROUP BY keys.
    void addBatchSinglePlace(size_t batch_size, AggregateDataPtr place, const IColumn ** columns, Arena *) const override
    {
        const auto & column = static_cast<const ColVecType &>(*columns[0]);
        this->data(place).addMany(column.getData().data(), batch_size);
    }

    void addBatchSinglePlaceNotNull(
        size_t batch_size, AggregateDataPtr place, const IColumn ** columns, const UInt8 * null_map, Arena *) const override
    {
        const auto & column = static_cast<const ColVecType &>(*columns[0]);
        this->data(place).addManyNotNull(column.getData().data(), null_map, batch_size);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        auto & column = static_cast<ColVecResult &>(to);
        column.getData().push_back(this->data(place).get());
    }

private:
    UInt32 scale;
};

}
