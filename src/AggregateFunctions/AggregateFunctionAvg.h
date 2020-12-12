#pragma once

#include <type_traits>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include "Core/DecimalFunctions.h"


namespace DB
{
template <class T>
using DecimalOrVectorCol = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;

template <class T> constexpr bool DecimalOrExtendedInt =
    IsDecimalNumber<T>
    || std::is_same_v<T, Int128>
    || std::is_same_v<T, Int256>
    || std::is_same_v<T, UInt128>
    || std::is_same_v<T, UInt256>;

/**
 * Helper class to encapsulate values conversion for avg and avgWeighted.
 */
template <class Numerator, class Denominator>
struct AvgFraction
{
    Numerator numerator{0};
    Denominator denominator{0};

    /// Allow division by zero as sometimes we need to return NaN.
    /// Invoked only is either Numerator or Denominator are Decimal.
    Float64 NO_SANITIZE_UNDEFINED divideIfAnyDecimal(UInt32 num_scale, UInt32 denom_scale) const
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
template <class Numerator, class Denominator, class Derived>
class AggregateFunctionAvgBase : public
        IAggregateFunctionDataHelper<AvgFraction<Numerator, Denominator>, Derived>
{
public:
    using Fraction = AvgFraction<Numerator, Denominator>;
    using Base = IAggregateFunctionDataHelper<Fraction, Derived>;

    explicit AggregateFunctionAvgBase(const DataTypes & argument_types_,
        UInt32 num_scale_ = 0, UInt32 denom_scale_ = 0)
        : Base(argument_types_, {}), num_scale(num_scale_), denom_scale(denom_scale_) {}

    DataTypePtr getReturnType() const final { return std::make_shared<DataTypeNumber<Float64>>(); }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).numerator += this->data(rhs).numerator;
        this->data(place).denominator += this->data(rhs).denominator;
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        writeBinary(this->data(place).numerator, buf);

        if constexpr (std::is_unsigned_v<Denominator>)
            writeVarUInt(this->data(place).denominator, buf);
        else /// Floating point denominator type can be used
            writeBinary(this->data(place).denominator, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        readBinary(this->data(place).numerator, buf);

        if constexpr (std::is_unsigned_v<Denominator>)
            readVarUInt(this->data(place).denominator, buf);
        else /// Floating point denominator type can be used
            readBinary(this->data(place).denominator, buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        if constexpr (IsDecimalNumber<Numerator> || IsDecimalNumber<Denominator>)
            static_cast<ColumnVector<Float64> &>(to).getData().push_back(
                this->data(place).divideIfAnyDecimal(num_scale, denom_scale));
        else
            static_cast<ColumnVector<Float64> &>(to).getData().push_back(this->data(place).divide());
    }
private:
    UInt32 num_scale;
    UInt32 denom_scale;
};

template <class T>
using AvgFieldType = std::conditional_t<IsDecimalNumber<T>,
    std::conditional_t<std::is_same_v<T, Decimal256>, Decimal256, Decimal128>,
    NearestFieldType<T>>;

template <class T>
class AggregateFunctionAvg final : public AggregateFunctionAvgBase<AvgFieldType<T>, UInt64, AggregateFunctionAvg<T>>
{
public:
    using AggregateFunctionAvgBase<AvgFieldType<T>, UInt64, AggregateFunctionAvg<T>>::AggregateFunctionAvgBase;

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const final
    {
        this->data(place).numerator += static_cast<const DecimalOrVectorCol<T> &>(*columns[0]).getData()[row_num];
        ++this->data(place).denominator;
    }

    String getName() const final { return "avg"; }
};
}
