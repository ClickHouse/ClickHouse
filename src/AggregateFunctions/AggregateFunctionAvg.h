#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{
/// A type-fixed fraction represented by a pair of #Numerator and #Denominator.
template <class Numerator, class Denominator>
struct RationalFraction
{
    constexpr RationalFraction(): numerator(0), denominator(0) {}

    Numerator numerator;
    Denominator denominator;

    /// Calculate the fraction as a #Result.
    template <class Result>
    Result NO_SANITIZE_UNDEFINED result() const
    {
        if constexpr (std::is_floating_point_v<Result> && std::numeric_limits<Result>::is_iec559)
            return static_cast<Result>(numerator) / denominator; /// allow division by zero

        if (denominator == static_cast<Denominator>(0))
            return static_cast<Result>(0);

        return static_cast<Result>(numerator / denominator);
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
template <class Denominator, class Derived>
class AggregateFunctionAvgBase : public
        IAggregateFunctionDataHelper<RationalFraction<Float64, Denominator>, Derived>
{
public:
    using Numerator = Float64;
    using Fraction = RationalFraction<Numerator, Denominator>;

    using ResultType       = Float64;
    using ResultDataType   = DataTypeNumber<Float64>;
    using ResultVectorType = ColumnVector<Float64>;

    using Base = IAggregateFunctionDataHelper<Fraction, Derived>;

    /// ctor for native types
    explicit AggregateFunctionAvgBase(const DataTypes & argument_types_): Base(argument_types_, {}), scale(0) {}

    /// ctor for Decimals
    AggregateFunctionAvgBase(const IDataType & data_type, const DataTypes & argument_types_)
        : Base(argument_types_, {}), scale(getDecimalScale(data_type)) {}

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<ResultDataType>();
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
        static_cast<ResultVectorType &>(to).getData().push_back(this->data(place).template result<ResultType>());
    }

protected:
    UInt32 scale;
};

class AggregateFunctionAvg final : public AggregateFunctionAvgBase<UInt64, AggregateFunctionAvg>
{
public:
    using AggregateFunctionAvgBase<UInt64, AggregateFunctionAvg>::AggregateFunctionAvgBase;

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const final
    {
        const auto & column = static_cast<const ColumnVector<Float64> &>(*columns[0]);
        this->data(place).numerator += column.getData()[row_num];
        ++this->data(place).denominator;
    }

    String getName() const final { return "avg"; }
};
}
