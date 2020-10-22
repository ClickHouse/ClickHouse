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
    constexpr RationalFraction(): numerator(0), denominator(0) {}

    Numerator numerator;
    Denominator denominator;

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
 * Motivation: ClickHouse has added the Decimal data type, which basically represents a fraction that stores
 * the precise (unlike floating-point) result with respect to some scale.
 *
 * These decimal types can't be divided by floating point data types, so functions like avg or avgWeighted
 * can't return the Floa64 column as a result when of the input columns is Decimal (because that would, in case of
 * avgWeighted, involve division numerator (Decimal) / denominator (Float64)).
 *
 * The rules for determining the output and intermediate storage types for these functions are different, so
 * the struct representing the deduction guide is presented.
 *
 * Given the initial Columns types (e.g. values and weights for avgWeighted, values for avg),
 * the struct calculated the output type and the intermediate storage type (that's used by the RationalFraction).
 */
template <class Column1, class Column2>
struct AvgFunctionTypesDeductionTemplate
{
    using Numerator = int;
    using Denominator = int;
    using Fraction = RationalFraction<Numerator, Denominator>;

    using ResultType = bool;
    using ResultDataType = bool;
    using ResultVectorType = bool;
};

/**
 * @tparam InitialNumerator The type that the initial numerator column would have (needed to cast the input IColumn to
 *                   appropriate type).
 * @tparam InitialDenominator The type that the initial denominator column would have.
 *
 * @tparam Deduction Function template that, given the numerator and the denominator, finds the actual
 *         suitable storage and the resulting column type.
 *
 * @tparam Derived When deriving from this class, use the child class name as in CRTP, e.g.
 *         class Self : Agg<char, bool, bool, Self>.
 */
template <class InitialNumerator, class InitialDenominator, template <class, class> class Deduction, class Derived>
class AggregateFunctionAvgBase : public
        IAggregateFunctionDataHelper<typename Deduction<InitialNumerator, InitialDenominator>::Fraction, Derived>
{
public:
    using Deducted = Deduction<InitialNumerator, InitialDenominator>;

    using ResultType =       typename Deducted::ResultType;
    using ResultDataType =   typename Deducted::ResultDataType;
    using ResultVectorType = typename Deducted::ResultVectorType;

    using Numerator =   typename Deducted::Numerator;
    using Denominator = typename Deducted::Denominator;

    using Base = IAggregateFunctionDataHelper<typename Deducted::Fraction, Derived>;

    /// ctor for native types
    explicit AggregateFunctionAvgBase(const DataTypes & argument_types_): Base(argument_types_, {}), scale(0) {}

    /// ctor for Decimals
    AggregateFunctionAvgBase(const IDataType & data_type, const DataTypes & argument_types_)
        : Base(argument_types_, {}), scale(getDecimalScale(data_type)) {}

    DataTypePtr getReturnType() const override
    {
        if constexpr (IsDecimalNumber<ResultType>)
            return std::make_shared<ResultDataType>(ResultDataType::maxPrecision(), scale);
        else
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

template <class T, class V>
struct AvgFunctionTypesDeduction
{
    using Numerator = std::conditional_t<IsDecimalNumber<T>,
        std::conditional_t<std::is_same_v<T, Decimal256>,
            Decimal256,
            Decimal128>,
        NearestFieldType<T>>;

    using Denominator = V;
    using Fraction = RationalFraction<Numerator, Denominator>;

    using ResultType =       std::conditional_t<IsDecimalNumber<T>, T, Float64>;
    using ResultDataType =   std::conditional_t<IsDecimalNumber<T>, DataTypeDecimal<T>, DataTypeNumber<Float64>>;
    using ResultVectorType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<Float64>>;
};

template <class InputColumn>
class AggregateFunctionAvg final :
    public AggregateFunctionAvgBase<InputColumn, UInt64, AvgFunctionTypesDeduction, AggregateFunctionAvg<InputColumn>>
{
public:
    using Base =
        AggregateFunctionAvgBase<InputColumn, UInt64, AvgFunctionTypesDeduction, AggregateFunctionAvg<InputColumn>>;

    using Base::Base;

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const final
    {
        const auto & column = static_cast<const DecimalOrVectorCol<InputColumn> &>(*columns[0]);
        this->data(place).numerator += column.getData()[row_num];
        this->data(place).denominator += 1;
    }

    String getName() const final { return "avg"; }
};
}
