#pragma once

#include <cmath>
#include <type_traits>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnDecimal.h>
#include <Core/DecimalFunctions.h>
#include <Core/callOnTypeIndex.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionsRound.h>
#include <DataTypes/DataTypeNullable.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionSum.h>

#include "config.h"

#if USE_EMBEDDED_COMPILER
#    include <llvm/IR/IRBuilder.h>
#    include <DataTypes/Native.h>
#endif

namespace DB
{

/// Convert Float64 avg result to target type with rounding
/// Returns 0 for NaN (empty set case)
template <typename ValueType>
ValueType avgResultToValue(Float64 v, UInt32 scale = 0)
{
    if (std::isnan(v))
        return ValueType(0);

    if constexpr (is_decimal<ValueType>)
    {
        const auto mult = DecimalUtils::scaleMultiplier<ValueType>(scale);
        const Float64 scaled = v * static_cast<Float64>(mult);
        const auto rounded = roundWithMode(scaled, RoundingMode::Round);
        return ValueType(static_cast<typename ValueType::NativeType>(rounded));
    }
    else
    {
        const auto rounded = roundWithMode(v, RoundingMode::Round);
        return static_cast<ValueType>(rounded);
    }
}

struct Settings;

template <typename T> constexpr bool DecimalOrExtendedInt =
    is_decimal<T>
    || std::is_same_v<T, Int128>
    || std::is_same_v<T, Int256>
    || std::is_same_v<T, UInt128>
    || std::is_same_v<T, UInt256>;

/**
 * Helper class to encapsulate values conversion for avg and avgWeighted.
 */
template <typename Numerator, typename Denominator>
struct AvgFraction
{
    Numerator numerator{0};
    Denominator denominator{0};

    /// Allow division by zero as sometimes we need to return NaN.
    /// Invoked only is either Numerator or Denominator are Decimal.
    Float64 NO_SANITIZE_UNDEFINED divideIfAnyDecimal(UInt32 num_scale, UInt32 denom_scale [[maybe_unused]]) const
    {
        Float64 numerator_float;
        if constexpr (is_decimal<Numerator>)
            numerator_float = DecimalUtils::convertTo<Float64>(numerator, num_scale);
        else
            numerator_float = numerator;

        Float64 denominator_float;
        if constexpr (is_decimal<Denominator>)
            denominator_float = DecimalUtils::convertTo<Float64>(denominator, denom_scale);
        else
            denominator_float = static_cast<Float64>(denominator);

        return numerator_float / denominator_float;
    }

    Float64 NO_SANITIZE_UNDEFINED divide() const
    {
        if constexpr (DecimalOrExtendedInt<Denominator>) /// if extended int
            return static_cast<Float64>(numerator) / static_cast<Float64>(denominator);
        else
            return static_cast<Float64>(numerator) / static_cast<Float64>(denominator);
    }
};


/**
 * @tparam Derived When deriving from this class, use the child class name as in CRTP, e.g.
 *         class Self : Agg<char, bool, bool, Self>.
 */
template <typename TNumerator, typename TDenominator, typename Derived>
class AggregateFunctionAvgBase : public
        IAggregateFunctionDataHelper<AvgFraction<TNumerator, TDenominator>, Derived>
{
public:
    using Base = IAggregateFunctionDataHelper<AvgFraction<TNumerator, TDenominator>, Derived>;
    using Numerator = TNumerator;
    using Denominator = TDenominator;
    using Fraction = AvgFraction<Numerator, Denominator>;

    explicit AggregateFunctionAvgBase(const DataTypes & argument_types_,
                                      UInt32 num_scale_ = 0, UInt32 denom_scale_ = 0)
        : Base(argument_types_, {}, createResultType())
        , num_scale(num_scale_)
        , denom_scale(denom_scale_)
    {}

    AggregateFunctionAvgBase(const DataTypes & argument_types_, const DataTypePtr & result_type_,
                             UInt32 num_scale_ = 0, UInt32 denom_scale_ = 0)
        : Base(argument_types_, {}, result_type_)
        , num_scale(num_scale_)
        , denom_scale(denom_scale_)
    {}

    DataTypePtr createResultType() const { return std::make_shared<DataTypeNumber<Float64>>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void NO_SANITIZE_UNDEFINED merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).numerator += this->data(rhs).numerator;
        this->data(place).denominator += this->data(rhs).denominator;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinaryLittleEndian(this->data(place).numerator, buf);

        if constexpr (std::is_unsigned_v<Denominator>)
            writeVarUInt(this->data(place).denominator, buf);
        else /// Floating point denominator type can be used
            writeBinary(this->data(place).denominator, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        readBinaryLittleEndian(this->data(place).numerator, buf);

        if constexpr (std::is_unsigned_v<Denominator>)
            readVarUInt(this->data(place).denominator, buf);
        else /// Floating point denominator type can be used
            readBinary(this->data(place).denominator, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto compute_avg = [&]() -> Float64
        {
            if constexpr (is_decimal<Numerator> || is_decimal<Denominator>)
                return this->data(place).divideIfAnyDecimal(num_scale, denom_scale);
            else
                return this->data(place).divide();
        };

        const auto & res_type = this->getResultType();
        WhichDataType result_which(res_type);

        Float64 v = compute_avg();

        /// Processing of results with Date/Time types
        if (callOnBasicType<void, false, false, false, true>(result_which.idx, [&](auto types) -> bool
        {
            using ValueType = typename decltype(types)::RightType;
            auto & col = assert_cast<ColumnVectorOrDecimal<ValueType> &>(to);
            if constexpr (is_decimal<ValueType>)
                col.getData().push_back(avgResultToValue<ValueType>(v, col.getScale()));
            else
                col.getData().push_back(avgResultToValue<ValueType>(v));
            return true;
        }))
            return;

        assert_cast<ColumnVector<Float64> &>(to).getData().push_back(v);
    }


#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override
    {
        if constexpr (!canBeNativeType<Numerator>() || !canBeNativeType<Denominator>())
            return false;

        bool can_be_compiled = true;

        for (const auto & argument : this->argument_types)
            can_be_compiled &= canBeNativeType(*argument);

        const auto & result_type = this->getResultType();
        can_be_compiled &= canBeNativeType(*result_type);

        /// JIT compilation does not support non-float result types (like DateTime[64]/Time[64])
        can_be_compiled &= WhichDataType(result_type).isFloat64();

        return can_be_compiled;
    }

    void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);
        b.CreateMemSet(aggregate_data_ptr, llvm::ConstantInt::get(b.getInt8Ty(), 0), sizeof(Fraction), llvm::assumeAligned(this->alignOfData()));
    }

    void compileMergeImpl(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const
    requires(canBeNativeType<Numerator>() && canBeNativeType<Denominator>())
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * numerator_type = toNativeType<Numerator>(b);

        auto * numerator_dst_ptr = aggregate_data_dst_ptr;
        auto * numerator_dst_value = b.CreateLoad(numerator_type, numerator_dst_ptr);
        numerator_dst_value->setAlignment(llvm::Align(alignof(TNumerator)));

        auto * numerator_src_ptr = aggregate_data_src_ptr;
        auto * numerator_src_value = b.CreateLoad(numerator_type, numerator_src_ptr);
        numerator_src_value->setAlignment(llvm::Align(alignof(TNumerator)));

        auto * numerator_result_value = numerator_type->isIntegerTy() ? b.CreateAdd(numerator_dst_value, numerator_src_value) : b.CreateFAdd(numerator_dst_value, numerator_src_value);
        b.CreateStore(numerator_result_value, numerator_dst_ptr)->setAlignment(llvm::Align(alignof(TNumerator)));

        auto * denominator_type = toNativeType<Denominator>(b);
        static constexpr size_t denominator_offset = offsetof(Fraction, denominator);
        auto * denominator_dst_ptr = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_dst_ptr, denominator_offset);
        auto * denominator_src_ptr = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_src_ptr, denominator_offset);

        auto * denominator_dst_value = b.CreateLoad(denominator_type, denominator_dst_ptr);
        denominator_dst_value->setAlignment(llvm::Align(alignof(TDenominator)));
        auto * denominator_src_value = b.CreateLoad(denominator_type, denominator_src_ptr);
        denominator_src_value->setAlignment(llvm::Align(alignof(TDenominator)));

        auto * denominator_result_value = denominator_type->isIntegerTy() ? b.CreateAdd(denominator_src_value, denominator_dst_value) : b.CreateFAdd(denominator_src_value, denominator_dst_value);
        b.CreateStore(denominator_result_value, denominator_dst_ptr)->setAlignment(llvm::Align(alignof(TDenominator)));
    }

    void
    compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override
    {
        if constexpr (canBeNativeType<Numerator>() && canBeNativeType<Denominator>())
            compileMergeImpl(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
    }

    llvm::Value * compileGetResultImpl(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const
    requires(canBeNativeType<Numerator>() && canBeNativeType<Denominator>())
    {
        const auto & result_type = this->getResultType();
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * numerator_type = toNativeType<Numerator>(b);
        auto * numerator_ptr = aggregate_data_ptr;
        auto * numerator_value = b.CreateLoad(numerator_type, numerator_ptr);
        numerator_value->setAlignment(llvm::Align(alignof(TNumerator)));

        auto * denominator_type = toNativeType<Denominator>(b);
        static constexpr size_t denominator_offset = offsetof(Fraction, denominator);
        auto * denominator_ptr = b.CreateConstGEP1_32(b.getInt8Ty(), aggregate_data_ptr, denominator_offset);
        auto * denominator_value = b.CreateLoad(denominator_type, denominator_ptr);
        denominator_value->setAlignment(llvm::Align(alignof(TDenominator)));

        auto * double_numerator = nativeCast<Numerator>(b, numerator_value, result_type);
        auto * double_denominator = nativeCast<Denominator>(b, denominator_value, result_type);

        /// If numerator is decimal, we need to scale it to the result type
        if constexpr (is_decimal<Numerator>)
        {
            auto scale = getDecimalScale(*removeNullable(this->argument_types[0]));
            auto multiplier = DecimalUtils::scaleMultiplier<NativeType<Numerator>>(scale);

            llvm::Value * multiplier_value = nullptr;
            if constexpr (!is_over_big_decimal<Numerator>)
            {
                multiplier_value = llvm::ConstantInt::get(numerator_type, static_cast<uint64_t>(multiplier), true);
            }
            else
            {
                llvm::APInt value(numerator_type->getIntegerBitWidth(), multiplier.items);
                multiplier_value = llvm::ConstantInt::get(numerator_type, value);
            }

            auto double_multiplier = nativeCast<Numerator>(b, multiplier_value, result_type);
            double_numerator = b.CreateFDiv(double_numerator, double_multiplier);
        }

        return b.CreateFDiv(double_numerator, double_denominator);
    }

    llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        if constexpr (canBeNativeType<Numerator>() && canBeNativeType<Denominator>())
            return compileGetResultImpl(builder, aggregate_data_ptr);
        return nullptr;
    }

#endif

private:
    UInt32 num_scale;
    UInt32 denom_scale;
};

template <typename T>
using AvgFieldType = std::conditional_t<is_decimal<T>,
    std::conditional_t<std::is_same_v<T, Decimal256>, Decimal256, Decimal128>,
    NearestFieldType<T>>;

template <typename T>
class AggregateFunctionAvg : public AggregateFunctionAvgBase<AvgFieldType<T>, UInt64, AggregateFunctionAvg<T>>
{
public:
    using Base = AggregateFunctionAvgBase<AvgFieldType<T>, UInt64, AggregateFunctionAvg<T>>;
    using Base::Base;

    using Numerator = typename Base::Numerator;
    using Denominator = typename Base::Denominator;
    using Fraction = typename Base::Fraction;
    using ColVecType = ColumnVectorOrDecimal<T>;


    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const final
    {
        increment(place, Numerator(static_cast<const ColVecType &>(*columns[0]).getData()[row_num]));
        ++this->data(place).denominator;
    }

    void addManyDefaults(
        AggregateDataPtr __restrict place,
        const IColumn ** /*columns*/,
        size_t length,
        Arena * /*arena*/) const override
    {
        this->data(place).denominator += length;
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos) const final
    {
        AggregateFunctionSumData<Numerator> sum_data;
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            sum_data.addManyConditional(column.getData().data(), flags.data(), row_begin, row_end);
            this->data(place).denominator += countBytesInFilter(flags.data(), row_begin, row_end);
        }
        else
        {
            sum_data.addMany(column.getData().data(), row_begin, row_end);
            this->data(place).denominator += (row_end - row_begin);
        }
        increment(place, sum_data.get());
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos)
        const final
    {
        AggregateFunctionSumData<Numerator> sum_data;
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        if (if_argument_pos >= 0)
        {
            /// Merge the 2 sets of flags (null and if) into a single one. This allows us to use parallelizable sums when available
            const auto * if_flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();
            auto final_flags = std::make_unique<UInt8[]>(row_end);
            size_t used_value = 0;
            for (size_t i = row_begin; i < row_end; ++i)
            {
                UInt8 kept = (!null_map[i]) & !!if_flags[i];
                final_flags[i] = kept;
                used_value += kept;
            }

            sum_data.addManyConditional(column.getData().data(), final_flags.get(), row_begin, row_end);
            this->data(place).denominator += used_value;
        }
        else
        {
            sum_data.addManyNotNull(column.getData().data(), null_map, row_begin, row_end);
            this->data(place).denominator += (row_end - row_begin) - countBytesInFilter(null_map, row_begin, row_end);
        }
        increment(place, sum_data.get());
    }

    String getName() const override { return "avg"; }

#if USE_EMBEDDED_COMPILER

    void compileAddImpl(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const ValuesWithType & arguments) const
    requires(canBeNativeType<Numerator>() && canBeNativeType<Denominator>())
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * numerator_type = toNativeType<Numerator>(b);

        auto * numerator_ptr = aggregate_data_ptr;
        auto * numerator_value = b.CreateLoad(numerator_type, numerator_ptr);
        numerator_value->setAlignment(llvm::Align(alignof(Numerator)));
        auto * value_cast_to_numerator = nativeCast(b, arguments[0], toNativeDataType<Numerator>());
        auto * numerator_result_value = numerator_type->isIntegerTy() ? b.CreateAdd(numerator_value, value_cast_to_numerator) : b.CreateFAdd(numerator_value, value_cast_to_numerator);
        b.CreateStore(numerator_result_value, numerator_ptr)->setAlignment(llvm::Align(alignof(Numerator)));

        auto * denominator_type = toNativeType<Denominator>(b);
        static constexpr size_t denominator_offset = offsetof(Fraction, denominator);
        auto * denominator_ptr = b.CreateConstGEP1_32(b.getInt8Ty(), aggregate_data_ptr, denominator_offset);
        auto * denominator_value_loaded = b.CreateLoad(denominator_type, denominator_ptr);
        denominator_value_loaded->setAlignment(llvm::Align(alignof(Denominator)));
        auto * denominator_value_updated = b.CreateAdd(denominator_value_loaded, llvm::ConstantInt::get(denominator_type, 1));
        b.CreateStore(denominator_value_updated, denominator_ptr)->setAlignment(llvm::Align(alignof(Denominator)));
    }

    void compileAdd(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const ValuesWithType & arguments) const override
    {
        if constexpr (canBeNativeType<Numerator>() && canBeNativeType<Denominator>())
            compileAddImpl(builder, aggregate_data_ptr, arguments);
    }

#endif

private:
    void NO_SANITIZE_UNDEFINED increment(AggregateDataPtr __restrict place, Numerator inc) const
    {
        this->data(place).numerator += inc;
    }
};
}
