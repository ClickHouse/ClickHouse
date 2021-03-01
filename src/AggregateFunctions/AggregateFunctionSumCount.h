#pragma once

#include <experimental/type_traits>
#include <type_traits>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{

/// Uses addOverflow method (if available) to avoid UB for sumWithOverflow()
///
/// Since NO_SANITIZE_UNDEFINED works only for the function itself, without
/// callers, and in case of non-POD type (i.e. Decimal) you have overwritten
/// operator+=(), which will have UB.
template <typename T>
struct AggregateFunctionSumCountCountAddOverflowImpl
{
    static void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(T & lhs, const T & rhs)
    {
        lhs += rhs;
    }
};
template <typename DecimalNativeType>
struct AggregateFunctionSumCountCountAddOverflowImpl<Decimal<DecimalNativeType>>
{
    static void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(Decimal<DecimalNativeType> & lhs, const Decimal<DecimalNativeType> & rhs)
    {
        lhs.addOverflow(rhs);
    }
};

template <typename T>
struct AggregateFunctionSumCountData
{
    using Impl = AggregateFunctionSumCountCountAddOverflowImpl<T>;

    T sum{};
    UInt64 count = 0;

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(T value)
    {
        Impl::add(sum, value);
        ++count;
    }

    /// Vectorized version
    template <typename Value>
    void NO_SANITIZE_UNDEFINED NO_INLINE addMany(const Value * __restrict ptr, size_t tmp_count)
    {
        const auto * end = ptr + tmp_count;

        if constexpr (std::is_floating_point_v<T>)
        {
            /// Compiler cannot unroll this loop, do it manually.
            /// (at least for floats, most likely due to the lack of -fassociative-math)

            /// Something around the number of SSE registers * the number of elements fit in register.
            constexpr size_t unroll_count = 128 / sizeof(T);
            T partial_sums[unroll_count]{};

            const auto * unrolled_end = ptr + (tmp_count / unroll_count * unroll_count);

            while (ptr < unrolled_end)
            {
                for (size_t i = 0; i < unroll_count; ++i)
                    Impl::add(partial_sums[i], ptr[i]);
                ptr += unroll_count;
            }

            for (size_t i = 0; i < unroll_count; ++i)
                Impl::add(sum, partial_sums[i]);
        }

        /// clang cannot vectorize the loop if accumulator is class member instead of local variable.
        T local_sum{};
        while (ptr < end)
        {
            Impl::add(local_sum, *ptr);
            ++ptr;
        }
        Impl::add(sum, local_sum);
        count += tmp_count; 
    }

    template <typename Value>
    void NO_SANITIZE_UNDEFINED NO_INLINE addManyNotNull(const Value * __restrict ptr, const UInt8 * __restrict null_map, size_t tmp_count)
    {
        const auto * end = ptr + tmp_count;

        if constexpr (std::is_floating_point_v<T>)
        {
            constexpr size_t unroll_count = 128 / sizeof(T);
            T partial_sums[unroll_count]{};

            const auto * unrolled_end = ptr + (tmp_count / unroll_count * unroll_count);

            while (ptr < unrolled_end)
            {
                for (size_t i = 0; i < unroll_count; ++i)
                {
                    if (!null_map[i])
                    {
                        Impl::add(partial_sums[i], ptr[i]);
                    }
                }
                ptr += unroll_count;
                null_map += unroll_count;
            }

            for (size_t i = 0; i < unroll_count; ++i)
                Impl::add(sum, partial_sums[i]);
        }

        T local_sum{};
        while (ptr < end)
        {
            if (!*null_map)
                Impl::add(local_sum, *ptr);
            ++ptr;
            ++null_map;
        }
        Impl::add(sum, local_sum);
        count += tmp_count;
    }

    void NO_SANITIZE_UNDEFINED merge(const AggregateFunctionSumCountData & rhs)
    {
        Impl::add(sum, rhs.sum);
        count += rhs.count;
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(sum, buf);
        writeBinary(count, buf);
    }

    void read(ReadBuffer & buf)
    {
        readBinary(sum, buf);
        readBinary(count, buf);
    }

    std::pair<T, UInt64> get() const
    {
        return {sum, count};
    }

};


/// Counts the sum of the numbers.
template <typename T, typename TResult, typename Data>
class AggregateFunctionSumCount final : public IAggregateFunctionDataHelper<Data, AggregateFunctionSumCount<T, TResult, Data>>
{
public:
    static constexpr bool DateTime64Supported = false;

    using ResultDataType = std::conditional_t<IsDecimalNumber<T>, DataTypeDecimal<TResult>, DataTypeNumber<TResult>>;
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    using ColVecResult = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<TResult>, ColumnVector<TResult>>;

    String getName() const override
    {
        return "sumCount";
    }

    AggregateFunctionSumCount(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionSumCount<T, TResult, Data>>(argument_types_, {})
        , scale(0)
    {}

    AggregateFunctionSumCount(const IDataType & data_type, const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionSumCount<T, TResult, Data>>(argument_types_, {})
        , scale(getDecimalScale(data_type))
    {}

    DataTypePtr getReturnType() const override
    {
        DataTypes types;
        if constexpr (IsDecimalNumber<T>)
        {
            types.emplace_back(std::make_shared<ResultDataType>(ResultDataType::maxPrecision(), scale));
        }
        else
        {
            types.emplace_back(std::make_shared<ResultDataType>());
        }
 
        types.emplace_back(std::make_shared<DataTypeUInt64>());

        return std::make_shared<DataTypeTuple>(types);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        if constexpr (is_big_int_v<T>)
            this->data(place).add(static_cast<TResult>(column.getData()[row_num]));
        else
            this->data(place).add(column.getData()[row_num]);
    }

    /// Vectorized version when there is no GROUP BY keys.
    void addBatchSinglePlace(
        size_t batch_size, AggregateDataPtr place, const IColumn ** columns, Arena * arena, ssize_t if_argument_pos) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = 0; i < batch_size; ++i)
            {
                if (flags[i])
                    add(place, columns, i, arena);
            }
        }
        else
        {
            const auto & column = assert_cast<const ColVecType &>(*columns[0]);
            this->data(place).addMany(column.getData().data(), batch_size);
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t batch_size, AggregateDataPtr place, const IColumn ** columns, const UInt8 * null_map, Arena * arena, ssize_t if_argument_pos)
        const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = 0; i < batch_size; ++i)
                if (!null_map[i] && flags[i])
                    add(place, columns, i, arena);
        }
        else
        {
            const auto & column = assert_cast<const ColVecType &>(*columns[0]);
            this->data(place).addManyNotNull(column.getData().data(), null_map, batch_size);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & to_tuple = assert_cast<ColumnTuple &>(to);
        auto & column_sum = assert_cast<ColVecResult &>(to_tuple.getColumn(0));
        auto & column_count = assert_cast<ColumnUInt64 &>(to_tuple.getColumn(1));
        auto res = this->data(place).get();
        column_sum.getData().push_back(res.first);
        column_count.getData().push_back(res.second);
    }

private:
    UInt32 scale;
};

}
