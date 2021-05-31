#pragma once

#include <type_traits>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/DecimalFunctions.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

#if USE_EMBEDDED_COMPILER
#    include <llvm/IR/IRBuilder.h>
#    include <DataTypes/Native.h>
#endif

namespace DB
{
struct Settings;
template <typename T>
using DecimalOrVectorCol = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;

template <typename T> constexpr bool DecimalOrExtendedInt =
    IsDecimalNumber<T>
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
        if constexpr (IsDecimalNumber<Numerator> && IsDecimalNumber<Denominator>)
        {
            // According to the docs, num(S1) / denom(S2) would have scale S1

            if constexpr (std::is_same_v<Numerator, Decimal256> && std::is_same_v<Denominator, Decimal128>)
                ///Special case as Decimal256 / Decimal128 = compile error (as Decimal128 is not parametrized by a wide
                ///int), but an __int128 instead
                return DecimalUtils::convertTo<Float64>(
                    numerator / (denominator.template convertTo<Decimal256>()), num_scale);
            else
                return DecimalUtils::convertTo<Float64>(numerator / denominator, num_scale);
        }

        /// Numerator is always casted to Float64 to divide correctly if the denominator is not Float64.
        Float64 num_converted;

        if constexpr (IsDecimalNumber<Numerator>)
            num_converted = DecimalUtils::convertTo<Float64>(numerator, num_scale);
        else
            num_converted = static_cast<Float64>(numerator); /// all other types, including extended integral.

        std::conditional_t<DecimalOrExtendedInt<Denominator>,
            Float64, Denominator> denom_converted;

        if constexpr (IsDecimalNumber<Denominator>)
            denom_converted = DecimalUtils::convertTo<Float64>(denominator, denom_scale);
        else if constexpr (DecimalOrExtendedInt<Denominator>)
            /// no way to divide Float64 and extended integral type without an explicit cast.
            denom_converted = static_cast<Float64>(denominator);
        else
            denom_converted = denominator; /// can divide on float, no cast required.

        return num_converted / denom_converted;
    }

    Float64 NO_SANITIZE_UNDEFINED divide() const
    {
        if constexpr (DecimalOrExtendedInt<Denominator>) /// if extended int
            return static_cast<Float64>(numerator) / static_cast<Float64>(denominator);
        else
            return static_cast<Float64>(numerator) / denominator;
    }
};


/**
 * @tparam Derived When deriving from this class, use the child class name as in CRTP, e.g.
 *         class Self : Agg<char, bool, bool, Self>.
 */
template <typename Numerator, typename Denominator, typename Derived>
class AggregateFunctionAvgBase : public
        IAggregateFunctionDataHelper<AvgFraction<Numerator, Denominator>, Derived>
{
public:
    using Fraction = AvgFraction<Numerator, Denominator>;
    using Base = IAggregateFunctionDataHelper<Fraction, Derived>;

    explicit AggregateFunctionAvgBase(const DataTypes & argument_types_,
        UInt32 num_scale_ = 0, UInt32 denom_scale_ = 0)
        : Base(argument_types_, {}), num_scale(num_scale_), denom_scale(denom_scale_) {}

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeNumber<Float64>>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void NO_SANITIZE_UNDEFINED merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).numerator += this->data(rhs).numerator;
        this->data(place).denominator += this->data(rhs).denominator;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        writeBinary(this->data(place).numerator, buf);

        if constexpr (std::is_unsigned_v<Denominator>)
            writeVarUInt(this->data(place).denominator, buf);
        else /// Floating point denominator type can be used
            writeBinary(this->data(place).denominator, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        readBinary(this->data(place).numerator, buf);

        if constexpr (std::is_unsigned_v<Denominator>)
            readVarUInt(this->data(place).denominator, buf);
        else /// Floating point denominator type can be used
            readBinary(this->data(place).denominator, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        if constexpr (IsDecimalNumber<Numerator> || IsDecimalNumber<Denominator>)
            assert_cast<ColumnVector<Float64> &>(to).getData().push_back(
                this->data(place).divideIfAnyDecimal(num_scale, denom_scale));
        else
            assert_cast<ColumnVector<Float64> &>(to).getData().push_back(this->data(place).divide());
    }
private:
    UInt32 num_scale;
    UInt32 denom_scale;
};

template <typename T>
using AvgFieldType = std::conditional_t<IsDecimalNumber<T>,
    std::conditional_t<std::is_same_v<T, Decimal256>, Decimal256, Decimal128>,
    NearestFieldType<T>>;

template <typename T>
class AggregateFunctionAvg final : public AggregateFunctionAvgBase<AvgFieldType<T>, UInt64, AggregateFunctionAvg<T>>
{
public:
    using AggregateFunctionAvgBase<AvgFieldType<T>, UInt64, AggregateFunctionAvg<T>>::AggregateFunctionAvgBase;

    void NO_SANITIZE_UNDEFINED add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const final
    {
        this->data(place).numerator += static_cast<const DecimalOrVectorCol<T> &>(*columns[0]).getData()[row_num];
        ++this->data(place).denominator;
    }

#if USE_EMBEDDED_COMPILER

    virtual bool isCompilable() const override
    {
        using AverageFieldType = AvgFieldType<T>;
        return std::is_same_v<AverageFieldType, UInt64> || std::is_same_v<AverageFieldType, Int64>;
    }

    virtual void compile(llvm::IRBuilderBase & builder, llvm::Value * aggregate_function_place, const DataTypePtr & value_type, llvm::Value * value) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        llvm::Type * numerator_type = b.getInt64Ty();
        llvm::Type * denominator_type = b.getInt64Ty();

        auto * numerator_value_ptr = b.CreatePointerCast(aggregate_function_place, numerator_type->getPointerTo());
        auto * numerator_value = b.CreateLoad(numerator_type, numerator_value_ptr);
        auto * value_cast_to_result = nativeCast(b, value_type, value, numerator_type);
        auto * sum_result_value = numerator_value->getType()->isIntegerTy() ? b.CreateAdd(numerator_value, value_cast_to_result) : b.CreateFAdd(numerator_value, value_cast_to_result);
        b.CreateStore(sum_result_value, numerator_value_ptr);

        auto * denominator_place_ptr_untyped = b.CreateConstInBoundsGEP1_32(nullptr, aggregate_function_place, 8);
        auto * denominator_place_ptr = b.CreatePointerCast(denominator_place_ptr_untyped, denominator_type->getPointerTo());
        auto * denominator_value = b.CreateLoad(denominator_place_ptr, numerator_value_ptr);
        auto * increate_denominator_value = b.CreateAdd(denominator_value, llvm::ConstantInt::get(denominator_type, 1));
        b.CreateStore(increate_denominator_value, denominator_place_ptr);
    }

#endif


    String getName() const final { return "avg"; }
};
}
