#pragma once

#include <cstring>
#include <memory>
#include <type_traits>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <Common/config.h>
#include <Common/TargetSpecific.h>

#if USE_EMBEDDED_COMPILER
#    include <llvm/IR/IRBuilder.h>
#    include <DataTypes/Native.h>
#endif

namespace DB
{
struct Settings;

/// Uses addOverflow method (if available) to avoid UB for sumWithOverflow()
///
/// Since NO_SANITIZE_UNDEFINED works only for the function itself, without
/// callers, and in case of non-POD type (i.e. Decimal) you have overwritten
/// operator+=(), which will have UB.
template <typename T>
struct AggregateFunctionSumAddOverflowImpl
{
    static void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(T & lhs, const T & rhs)
    {
        lhs += rhs;
    }
};
template <typename DecimalNativeType>
struct AggregateFunctionSumAddOverflowImpl<Decimal<DecimalNativeType>>
{
    static void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(Decimal<DecimalNativeType> & lhs, const Decimal<DecimalNativeType> & rhs)
    {
        lhs.addOverflow(rhs);
    }
};

template <typename T>
struct AggregateFunctionSumData
{
    using Impl = AggregateFunctionSumAddOverflowImpl<T>;
    T sum{};

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(T value)
    {
        Impl::add(sum, value);
    }

    /// Vectorized version
    MULTITARGET_FUNCTION_AVX2_SSE42(
    MULTITARGET_FUNCTION_HEADER(
    template <typename Value>
    void NO_SANITIZE_UNDEFINED NO_INLINE
    ), addManyImpl, MULTITARGET_FUNCTION_BODY((const Value * __restrict ptr, size_t start, size_t end) /// NOLINT
    {
        ptr += start;
        size_t count = end - start;
        const auto * end_ptr = ptr + count;

        if constexpr (std::is_floating_point_v<T>)
        {
            /// Compiler cannot unroll this loop, do it manually.
            /// (at least for floats, most likely due to the lack of -fassociative-math)

            /// Something around the number of SSE registers * the number of elements fit in register.
            constexpr size_t unroll_count = 128 / sizeof(T);
            T partial_sums[unroll_count]{};

            const auto * unrolled_end = ptr + (count / unroll_count * unroll_count);

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
        while (ptr < end_ptr)
        {
            Impl::add(local_sum, *ptr);
            ++ptr;
        }
        Impl::add(sum, local_sum);
    })
    )

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

    MULTITARGET_FUNCTION_AVX2_SSE42(
    MULTITARGET_FUNCTION_HEADER(
    template <typename Value, bool add_if_zero>
    void NO_SANITIZE_UNDEFINED NO_INLINE
    ), addManyConditionalInternalImpl, MULTITARGET_FUNCTION_BODY((const Value * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end) /// NOLINT
    {
        ptr += start;
        size_t count = end - start;
        const auto * end_ptr = ptr + count;

        if constexpr (
            (is_integer<T> && !is_big_int_v<T>)
            || (is_decimal<T> && !std::is_same_v<T, Decimal256> && !std::is_same_v<T, Decimal128>))
        {
            /// For integers we can vectorize the operation if we replace the null check using a multiplication (by 0 for null, 1 for not null)
            /// https://quick-bench.com/q/MLTnfTvwC2qZFVeWHfOBR3U7a8I
            T local_sum{};
            while (ptr < end_ptr)
            {
                T multiplier = !*condition_map == add_if_zero;
                Impl::add(local_sum, *ptr * multiplier);
                ++ptr;
                ++condition_map;
            }
            Impl::add(sum, local_sum);
            return;
        }

        if constexpr (std::is_floating_point_v<T>)
        {
            /// For floating point we use a similar trick as above, except that now we  reinterpret the floating point number as an unsigned
            /// integer of the same size and use a mask instead (0 to discard, 0xFF..FF to keep)
            static_assert(sizeof(Value) == 4 || sizeof(Value) == 8);
            using equivalent_integer = typename std::conditional_t<sizeof(Value) == 4, UInt32, UInt64>;

            constexpr size_t unroll_count = 128 / sizeof(T);
            T partial_sums[unroll_count]{};

            const auto * unrolled_end = ptr + (count / unroll_count * unroll_count);

            while (ptr < unrolled_end)
            {
                for (size_t i = 0; i < unroll_count; ++i)
                {
                    equivalent_integer value;
                    std::memcpy(&value, &ptr[i], sizeof(Value));
                    value &= (!condition_map[i] != add_if_zero) - 1;
                    Value d;
                    std::memcpy(&d, &value, sizeof(Value));
                    Impl::add(partial_sums[i], d);
                }
                ptr += unroll_count;
                condition_map += unroll_count;
            }

            for (size_t i = 0; i < unroll_count; ++i)
                Impl::add(sum, partial_sums[i]);
        }

        T local_sum{};
        while (ptr < end_ptr)
        {
            if (!*condition_map == add_if_zero)
                Impl::add(local_sum, *ptr);
            ++ptr;
            ++condition_map;
        }
        Impl::add(sum, local_sum);
    })
    )

    /// Vectorized version
    template <typename Value, bool add_if_zero>
    void NO_INLINE addManyConditionalInternal(const Value * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end)
    {
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::AVX2))
        {
            addManyConditionalInternalImplAVX2<Value, add_if_zero>(ptr, condition_map, start, end);
            return;
        }
        else if (isArchSupported(TargetArch::SSE42))
        {
            addManyConditionalInternalImplSSE42<Value, add_if_zero>(ptr, condition_map, start, end);
            return;
        }
#endif

        addManyConditionalInternalImpl<Value, add_if_zero>(ptr, condition_map, start, end);
    }

    template <typename Value>
    void ALWAYS_INLINE addManyNotNull(const Value * __restrict ptr, const UInt8 * __restrict null_map, size_t start, size_t end)
    {
        return addManyConditionalInternal<Value, true>(ptr, null_map, start, end);
    }

    template <typename Value>
    void ALWAYS_INLINE addManyConditional(const Value * __restrict ptr, const UInt8 * __restrict cond_map, size_t start, size_t end)
    {
        return addManyConditionalInternal<Value, false>(ptr, cond_map, start, end);
    }

    void NO_SANITIZE_UNDEFINED merge(const AggregateFunctionSumData & rhs)
    {
        Impl::add(sum, rhs.sum);
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
    void NO_INLINE addMany(const Value * __restrict ptr, size_t start, size_t end)
    {
        /// Less than in ordinary sum, because the algorithm is more complicated and too large loop unrolling is questionable.
        /// But this is just a guess.
        constexpr size_t unroll_count = 4;
        T partial_sums[unroll_count]{};
        T partial_compensations[unroll_count]{};

        ptr += start;
        size_t count = end - start;

        const auto * end_ptr = ptr + count;
        const auto * unrolled_end = ptr + (count / unroll_count * unroll_count);

        while (ptr < unrolled_end)
        {
            for (size_t i = 0; i < unroll_count; ++i)
                addImpl(ptr[i], partial_sums[i], partial_compensations[i]);
            ptr += unroll_count;
        }

        for (size_t i = 0; i < unroll_count; ++i)
            mergeImpl(sum, compensation, partial_sums[i], partial_compensations[i]);

        while (ptr < end_ptr)
        {
            addImpl(*ptr, sum, compensation);
            ++ptr;
        }
    }

    template <typename Value, bool add_if_zero>
    void NO_INLINE addManyConditionalInternal(const Value * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end)
    {
        constexpr size_t unroll_count = 4;
        T partial_sums[unroll_count]{};
        T partial_compensations[unroll_count]{};

        ptr += start;
        size_t count = end - start;

        const auto * end_ptr = ptr + count;
        const auto * unrolled_end = ptr + (count / unroll_count * unroll_count);

        while (ptr < unrolled_end)
        {
            for (size_t i = 0; i < unroll_count; ++i)
                if ((!condition_map[i]) == add_if_zero)
                    addImpl(ptr[i], partial_sums[i], partial_compensations[i]);
            ptr += unroll_count;
            condition_map += unroll_count;
        }

        for (size_t i = 0; i < unroll_count; ++i)
            mergeImpl(sum, compensation, partial_sums[i], partial_compensations[i]);

        while (ptr < end_ptr)
        {
            if ((!*condition_map) == add_if_zero)
                addImpl(*ptr, sum, compensation);
            ++ptr;
            ++condition_map;
        }
    }

    template <typename Value>
    void ALWAYS_INLINE addManyNotNull(const Value * __restrict ptr, const UInt8 * __restrict null_map, size_t start, size_t end)
    {
        return addManyConditionalInternal<Value, true>(ptr, null_map, start, end);
    }

    template <typename Value>
    void ALWAYS_INLINE addManyConditional(const Value * __restrict ptr, const UInt8 * __restrict cond_map, size_t start, size_t end)
    {
        return addManyConditionalInternal<Value, false>(ptr, cond_map, start, end);
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
    static constexpr bool DateTime64Supported = false;

    using ColVecType = ColumnVectorOrDecimal<T>;

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

    explicit AggregateFunctionSum(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data, Type>>(argument_types_, {})
        , scale(0)
    {}

    AggregateFunctionSum(const IDataType & data_type, const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data, Type>>(argument_types_, {})
        , scale(getDecimalScale(data_type))
    {}

    DataTypePtr getReturnType() const override
    {
        if constexpr (!is_decimal<T>)
            return std::make_shared<DataTypeNumber<TResult>>();
        else
        {
            using DataType = DataTypeDecimal<TResult>;
            return std::make_shared<DataType>(DataType::maxPrecision(), scale);
        }
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        if constexpr (is_big_int_v<T>)
            this->data(place).add(static_cast<TResult>(column.getData()[row_num]));
        else
            this->data(place).add(column.getData()[row_num]);
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
        ssize_t if_argument_pos)
        const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        if (if_argument_pos >= 0)
        {
            /// Merge the 2 sets of flags (null and if) into a single one. This allows us to use parallelizable sums when available
            const auto * if_flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();
            auto final_flags = std::make_unique<UInt8[]>(row_end);
            for (size_t i = row_begin; i < row_end; ++i)
                final_flags[i] = (!null_map[i]) & if_flags[i];

            this->data(place).addManyConditional(column.getData().data(), final_flags.get(), row_begin, row_end);
        }
        else
        {
            this->data(place).addManyNotNull(column.getData().data(), null_map, row_begin, row_end);
        }
    }

    void addManyDefaults(
        AggregateDataPtr __restrict /*place*/,
        const IColumn ** /*columns*/,
        size_t /*length*/,
        Arena * /*arena*/) const override
    {
    }

    void addBatchSparse(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena) const override
    {
        const auto & column_sparse = assert_cast<const ColumnSparse &>(*columns[0]);
        const auto * values = &column_sparse.getValuesColumn();
        const auto & offsets = column_sparse.getOffsetsData();

        size_t from = std::lower_bound(offsets.begin(), offsets.end(), row_begin) - offsets.begin();
        size_t to = std::lower_bound(offsets.begin(), offsets.end(), row_end) - offsets.begin();

        for (size_t i = from; i < to; ++i)
            add(places[offsets[i]] + place_offset, &values, i + 1, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        castColumnToResult(to).getData().push_back(this->data(place).get());
    }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override
    {
        if constexpr (Type == AggregateFunctionTypeSumKahan)
            return false;

        bool can_be_compiled = true;

        for (const auto & argument_type : this->argument_types)
            can_be_compiled &= canBeNativeType(*argument_type);

        auto return_type = getReturnType();
        can_be_compiled &= canBeNativeType(*return_type);

        return can_be_compiled;
    }

    void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = toNativeType(b, getReturnType());
        auto * aggregate_sum_ptr = b.CreatePointerCast(aggregate_data_ptr, return_type->getPointerTo());

        b.CreateStore(llvm::Constant::getNullValue(return_type), aggregate_sum_ptr);
    }

    void compileAdd(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const DataTypes & arguments_types, const std::vector<llvm::Value *> & argument_values) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = toNativeType(b, getReturnType());

        auto * sum_value_ptr = b.CreatePointerCast(aggregate_data_ptr, return_type->getPointerTo());
        auto * sum_value = b.CreateLoad(return_type, sum_value_ptr);

        const auto & argument_type = arguments_types[0];
        const auto & argument_value = argument_values[0];

        auto * value_cast_to_result = nativeCast(b, argument_type, argument_value, return_type);
        auto * sum_result_value = sum_value->getType()->isIntegerTy() ? b.CreateAdd(sum_value, value_cast_to_result) : b.CreateFAdd(sum_value, value_cast_to_result);

        b.CreateStore(sum_result_value, sum_value_ptr);
    }

    void compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = toNativeType(b, getReturnType());

        auto * sum_value_dst_ptr = b.CreatePointerCast(aggregate_data_dst_ptr, return_type->getPointerTo());
        auto * sum_value_dst = b.CreateLoad(return_type, sum_value_dst_ptr);

        auto * sum_value_src_ptr = b.CreatePointerCast(aggregate_data_src_ptr, return_type->getPointerTo());
        auto * sum_value_src = b.CreateLoad(return_type, sum_value_src_ptr);

        auto * sum_return_value = sum_value_dst->getType()->isIntegerTy() ? b.CreateAdd(sum_value_dst, sum_value_src) : b.CreateFAdd(sum_value_dst, sum_value_src);
        b.CreateStore(sum_return_value, sum_value_dst_ptr);
    }

    llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = toNativeType(b, getReturnType());
        auto * sum_value_ptr = b.CreatePointerCast(aggregate_data_ptr, return_type->getPointerTo());

        return b.CreateLoad(return_type, sum_value_ptr);
    }

#endif

private:
    UInt32 scale;

    static constexpr auto & castColumnToResult(IColumn & to)
    {
        if constexpr (is_decimal<T>)
            return assert_cast<ColumnDecimal<TResult> &>(to);
        else
            return assert_cast<ColumnVector<TResult> &>(to);
    }
};

}
