#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{

/// @tparam BothZeroMeansNaN If false, the pair 0 / 0 = 0, nan otherwise.
template <class Denominator, bool BothZeroMeansNaN = true>
struct RationalFraction
{
    Float64 numerator{0};
    Denominator denominator{0};

    Float64 NO_SANITIZE_UNDEFINED result() const
    {
        if constexpr (BothZeroMeansNaN && std::numeric_limits<Float64>::is_iec559)
            return static_cast<Float64>(numerator) / denominator; /// allow division by zero

        if (denominator == static_cast<Denominator>(0))
            return static_cast<Float64>(0);

        return static_cast<Float64>(numerator / denominator);
    }
};

/**
 * The discussion showed that the easiest (and simplest) way is to cast both the columns of numerator and denominator
 * to Float64. Another way would be to write some template magic that figures out the appropriate numerator
 * and denominator (and the resulting type) in favour of extended integral types (UInt128 e.g.) and Decimals (
 * which are a mess themselves). The second way is also a bit useless because now Decimals are not used in functions
 * like avg.
 *
 * The ability to explicitly specify the denominator is made for avg (it uses the integral value as the denominator is
 * simply the length of the supplied list).
 *
 * @tparam Derived When deriving from this class, use the child class name as in CRTP, e.g.
 *         class Self : Agg<char, bool, bool, Self>.
 */
template <class Denominator, bool BothZeroMeansNaN, class Derived>
class AggregateFunctionAvgBase : public
        IAggregateFunctionDataHelper<RationalFraction<Denominator, BothZeroMeansNaN>, Derived>
{
public:
    using Fraction = RationalFraction<Denominator, BothZeroMeansNaN>;
    using Base = IAggregateFunctionDataHelper<Fraction, Derived>;

    explicit AggregateFunctionAvgBase(const DataTypes & argument_types_): Base(argument_types_, {}) {}

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeNumber<Float64>>(); }

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
        static_cast<ColumnVector<Float64> &>(to).getData().push_back(this->data(place).result());
    }
};

class AggregateFunctionAvg final : public AggregateFunctionAvgBase<UInt64, false, AggregateFunctionAvg>
{
public:
    using AggregateFunctionAvgBase<UInt64, false, AggregateFunctionAvg>::AggregateFunctionAvgBase;

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const final
    {
        this->data(place).numerator += columns[0]->getFloat64(row_num);
        ++this->data(place).denominator;
    }

    String getName() const final { return "avg"; }
};
}
