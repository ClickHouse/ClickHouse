#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{
template <class T>
using DecimalOrVectorCol = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;

/// A type-fixed rational fraction represented by a pair of #Numerator and #Denominator.
template <class Numerator, class Denominator>
struct RationalFraction
{
    Numerator numerator{0};
    Denominator denominator{0};

    /// Calculate the fraction as a #Result.
    template <class Result>
    Result NO_SANITIZE_UNDEFINED result() const
    {
        if constexpr (std::is_floating_point_v<Result>)
            if constexpr (std::numeric_limits<Result>::is_iec559)
            {
                if constexpr (is_big_int_v<Denominator>)
                    return static_cast<Result>(numerator) / static_cast<Result>(denominator);
                else
                    return static_cast<Result>(numerator) / denominator; /// allow division by zero
            }

        if (denominator == static_cast<Denominator>(0))
            return static_cast<Result>(0);

        if constexpr (std::is_same_v<Numerator, Decimal256>)
            return static_cast<Result>(numerator / static_cast<Numerator>(denominator));
        else
            return static_cast<Result>(numerator / denominator);
    }
};

/**
 * @tparam Numerator The type that the initial numerator column would have (needed to cast the input IColumn to
 *                   appropriate type).
 * @tparam Denominator The type that the initial denominator column would have.
 *
 * @tparam Derived When deriving from this class, use the child class name as in CRTP, e.g.
 *         class Self : Agg<char, bool, bool, Self>.
 */
template <class Numerator, class Denominator, class Derived>
class AggregateFunctionAvgBase : public IAggregateFunctionDataHelper<RationalFraction<Numerator, Denominator>, Derived>
{
public:
    using Base = IAggregateFunctionDataHelper<RationalFraction<Numerator, Denominator>, Derived>;

    /// ctor for native types
    explicit AggregateFunctionAvgBase(const DataTypes & argument_types_): Base(argument_types_, {}), scale(0) {}

    /// ctor for Decimals
    AggregateFunctionAvgBase(const IDataType & data_type, const DataTypes & argument_types_)
        : Base(argument_types_, {}), scale(getDecimalScale(data_type)) {}

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeNumber<Float64>>();
    }

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
        static_cast<ColumnVector<Float64> &>(to).getData().push_back(this->data(place).template result<Float64>());
    }

protected:
    UInt32 scale;
};

template <class InputColumn, class Numerator, class Denominator>
class AggregateFunctionAvg final :
    public AggregateFunctionAvgBase<Numerator, Denominator, AggregateFunctionAvg<InputColumn, Numerator, Denominator>>
{
public:
    using AggregateFunctionAvgBase<Numerator, Denominator,
        AggregateFunctionAvg<InputColumn, Numerator, Denominator>>::AggregateFunctionAvgBase;

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const final
    {
        const auto & column = static_cast<const DecimalOrVectorCol<InputColumn> &>(*columns[0]);
        this->data(place).numerator += column.getData()[row_num];
        this->data(place).denominator += 1;
    }

    String getName() const final { return "avg"; }
};
}
