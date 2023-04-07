#pragma once

#include <type_traits>
#include <AggregateFunctions/AggregateFunctionSum.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Core/DecimalFunctions.h>
#include <Core/IResolvedFunction.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/extended_types.h>

#include "config.h"

#if USE_EMBEDDED_COMPILER
#    include <DataTypes/Native.h>
#    include <llvm/IR/IRBuilder.h>
#endif

namespace DB
{
struct Settings;

/**
 * Helper class to encapsulate values conversion for avg and avgWeighted.
 */
template <typename Numerator, typename Denominator>
struct AvgFraction
{
    Numerator numerator{0};
    Denominator denominator{0};

    /// Allow division by zero as sometimes we need to return NaN.
    Float64 NO_SANITIZE_UNDEFINED divideIfAnyDecimal(UInt32 num_scale, UInt32 denom_scale [[maybe_unused]]) const
    {
        // num_scale is actually a sum of numerator and denominator scales if both numerator and
        // denominator are decimal types, see AggregateFunctionAvgWeighted.cpp::create()
        if constexpr (is_decimal<Numerator> && is_decimal<Denominator>)
            return DecimalUtils::convertTo<Float64>(numerator / denominator, num_scale);

        // The only way to divide correctly if denominator is not Float64 is to cast numerator to Float64
        // for all types
        Float64 casted_numerator;
        if constexpr (is_decimal<Numerator>)
            casted_numerator = DecimalUtils::convertTo<Float64>(numerator, num_scale);
        else
            casted_numerator = numerator;

        std::conditional_t< //
            is_decimal<Denominator> || is_extended_int<Denominator>,
            Float64,
            Denominator>
            casted_denominator;

        if constexpr (is_decimal<Denominator>)
            casted_denominator = DecimalUtils::convertTo<Float64>(denominator, denom_scale);
        else
            casted_denominator = denominator;

        return casted_numerator / casted_denominator;
    }

    Float64 NO_SANITIZE_UNDEFINED divide() const
        requires(!is_decimal<Numerator> && !is_decimal<Denominator>)
    {
        if constexpr (is_extended_int<Denominator>)
            return static_cast<Float64>(numerator) / static_cast<Float64>(denominator);
        else
            return static_cast<Float64>(numerator) / denominator;
    }
};


/**
 * @tparam Derived When deriving from this class, use the child class name as in CRTP, e.g.
 *         class Self : Agg<char, bool, bool, Self>.
 */
template <typename TNumerator, typename TDenominator, typename Derived>
class AggregateFunctionAvgBase : public IAggregateFunctionDataHelper<AvgFraction<TNumerator, TDenominator>, Derived>
{
public:
    using Base = IAggregateFunctionDataHelper<AvgFraction<TNumerator, TDenominator>, Derived>;
    using Numerator = TNumerator;
    using Denominator = TDenominator;
    using Fraction = AvgFraction<Numerator, Denominator>;

    explicit AggregateFunctionAvgBase(const DataTypes & argument_types_, UInt32 num_scale_ = 0, UInt32 denom_scale_ = 0)
        : Base(argument_types_, {}, createResultType()), num_scale(num_scale_), denom_scale(denom_scale_)
    {
    }

    AggregateFunctionAvgBase(
        const DataTypes & argument_types_, const DataTypePtr & result_type_, UInt32 num_scale_ = 0, UInt32 denom_scale_ = 0)
        : Base(argument_types_, {}, result_type_), num_scale(num_scale_), denom_scale(denom_scale_)
    {
    }

    DataTypePtr createResultType() const { return std::make_shared<DataTypeNumber<Float64>>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void NO_SANITIZE_UNDEFINED merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).numerator += this->data(rhs).numerator;
        this->data(place).denominator += this->data(rhs).denominator;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinary(this->data(place).numerator, buf);

        // intentional use of std:: as writeVarUInt does not operate on extended types
        if constexpr (std::is_unsigned_v<Denominator>)
            writeVarUInt(this->data(place).denominator, buf);
        else
            writeBinary(this->data(place).denominator, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        readBinary(this->data(place).numerator, buf);

        // intentional use of std:: as writeVarUInt does not operate on extended types
        if constexpr (std::is_unsigned_v<Denominator>)
            readVarUInt(this->data(place).denominator, buf);
        else
            readBinary(this->data(place).denominator, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & to_array = assert_cast<ColumnVector<Float64> &>(to).getData();

        if constexpr (is_decimal<Numerator> || is_decimal<Denominator>)
            to_array.push_back(this->data(place).divideIfAnyDecimal(num_scale, denom_scale));
        else
            to_array.push_back(this->data(place).divide());
    }


#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override
    {
        bool can_be_compiled = true;

        for (const auto & argument : this->argument_types)
            can_be_compiled &= canBeNativeType(*argument);

        auto return_type = this->getResultType();
        can_be_compiled &= canBeNativeType(*return_type);

        return can_be_compiled;
    }

    void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);
        b.CreateMemSet(
            aggregate_data_ptr, llvm::ConstantInt::get(b.getInt8Ty(), 0), sizeof(Fraction), llvm::assumeAligned(this->alignOfData()));
    }

    void
    compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * numerator_type = toNativeType<Numerator>(b);

        auto * numerator_dst_ptr = aggregate_data_dst_ptr;
        auto * numerator_dst_value = b.CreateLoad(numerator_type, numerator_dst_ptr);

        auto * numerator_src_ptr = aggregate_data_src_ptr;
        auto * numerator_src_value = b.CreateLoad(numerator_type, numerator_src_ptr);

        auto * numerator_result_value = numerator_type->isIntegerTy() ? b.CreateAdd(numerator_dst_value, numerator_src_value)
                                                                      : b.CreateFAdd(numerator_dst_value, numerator_src_value);
        b.CreateStore(numerator_result_value, numerator_dst_ptr);

        auto * denominator_type = toNativeType<Denominator>(b);
        static constexpr size_t denominator_offset = offsetof(Fraction, denominator);
        auto * denominator_dst_ptr = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_dst_ptr, denominator_offset);
        auto * denominator_src_ptr = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_src_ptr, denominator_offset);

        auto * denominator_dst_value = b.CreateLoad(denominator_type, denominator_dst_ptr);
        auto * denominator_src_value = b.CreateLoad(denominator_type, denominator_src_ptr);

        auto * denominator_result_value = denominator_type->isIntegerTy() ? b.CreateAdd(denominator_src_value, denominator_dst_value)
                                                                          : b.CreateFAdd(denominator_src_value, denominator_dst_value);
        b.CreateStore(denominator_result_value, denominator_dst_ptr);
    }

    llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * numerator_type = toNativeType<Numerator>(b);
        auto * numerator_ptr = aggregate_data_ptr;
        auto * numerator_value = b.CreateLoad(numerator_type, numerator_ptr);

        auto * denominator_type = toNativeType<Denominator>(b);
        static constexpr size_t denominator_offset = offsetof(Fraction, denominator);
        auto * denominator_ptr = b.CreateConstGEP1_32(b.getInt8Ty(), aggregate_data_ptr, denominator_offset);
        auto * denominator_value = b.CreateLoad(denominator_type, denominator_ptr);

        auto * double_numerator = nativeCast<Numerator>(b, numerator_value, b.getDoubleTy());
        auto * double_denominator = nativeCast<Denominator>(b, denominator_value, b.getDoubleTy());

        return b.CreateFDiv(double_numerator, double_denominator);
    }

#endif

private:
    UInt32 num_scale;
    UInt32 denom_scale;
};

template <typename T>
using AvgFieldType = std::conditional_t< //
    is_decimal<T>,
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
        incrementNumerator(place, static_cast<const ColVecType &>(*columns[0]).getData()[row_num]);
        ++this->data(place).denominator;
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** /*columns*/, size_t length, Arena * /*arena*/) const override
    {
        this->data(place).denominator += length;
    }

    void addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena *, ssize_t if_argument_pos)
        const final
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
        incrementNumerator(place, sum_data.sum);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos) const final
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
        incrementNumerator(place, sum_data.sum);
    }

    String getName() const override { return "avg"; }

#if USE_EMBEDDED_COMPILER

    void compileAdd(
        llvm::IRBuilderBase & builder,
        llvm::Value * aggregate_data_ptr,
        const DataTypes & arguments_types,
        const std::vector<llvm::Value *> & argument_values) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * numerator_type = toNativeType<Numerator>(b);

        auto * numerator_ptr = aggregate_data_ptr;
        auto * numerator_value = b.CreateLoad(numerator_type, numerator_ptr);
        auto * value_cast_to_numerator = nativeCast(b, arguments_types[0], argument_values[0], numerator_type);
        auto * numerator_result_value = numerator_type->isIntegerTy() ? b.CreateAdd(numerator_value, value_cast_to_numerator)
                                                                      : b.CreateFAdd(numerator_value, value_cast_to_numerator);
        b.CreateStore(numerator_result_value, numerator_ptr);

        auto * denominator_type = toNativeType<Denominator>(b);
        static constexpr size_t denominator_offset = offsetof(Fraction, denominator);
        auto * denominator_ptr = b.CreateConstGEP1_32(b.getInt8Ty(), aggregate_data_ptr, denominator_offset);
        auto * denominator_value_updated
            = b.CreateAdd(b.CreateLoad(denominator_type, denominator_ptr), llvm::ConstantInt::get(denominator_type, 1));
        b.CreateStore(denominator_value_updated, denominator_ptr);
    }

#endif

private:
    void NO_SANITIZE_UNDEFINED incrementNumerator(AggregateDataPtr __restrict place, Numerator count) const
    {
        this->data(place).numerator += count;
    }
};
}
