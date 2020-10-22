#pragma once

#include <type_traits>
#include <AggregateFunctions/AggregateFunctionAvg.h>

namespace DB
{
template <class Values, class Weights>
struct AvgWeightedFunctionTypesDeduction
{
    template <class T> struct NextAvgType  { using Type = T; };
    template <> struct NextAvgType<Int8>   { using Type = Int16; };
    template <> struct NextAvgType<Int16>  { using Type = Int32; };
    template <> struct NextAvgType<Int32>  { using Type = Int64; };
    template <> struct NextAvgType<Int64>  { using Type = Int128; };
    template <> struct NextAvgType<Int128> { using Type = Int256; };
    template <> struct NextAvgType<Int256> { using Type = Int256; };

    template <> struct NextAvgType<UInt8>   { using Type = UInt16; };
    template <> struct NextAvgType<UInt16>  { using Type = UInt32; };
    template <> struct NextAvgType<UInt32>  { using Type = UInt64; };
    template <> struct NextAvgType<UInt64>  { using Type = UInt128; };
    template <> struct NextAvgType<UInt128> { using Type = UInt256; };
    template <> struct NextAvgType<UInt256> { using Type = UInt256; };

    template <> struct NextAvgType<Decimal32> { using Type = Decimal128; };
    template <> struct NextAvgType<Decimal64> { using Type = Decimal128; };
    template <> struct NextAvgType<Decimal128> { using Type = Decimal256; };
    template <> struct NextAvgType<Decimal256> { using Type = Decimal256; };

    template <> struct NextAvgType<Float32> { using Type = Float64; };
    template <> struct NextAvgType<Float64> { using Type = Float64; };

    template <class T> using NextAvgTypeT = typename NextAvgType<T>::Type;
    template <class T, class U> using Largest = std::conditional_t<(sizeof(T) > sizeof(U)), T, U>;

    struct GetNumDenom
    {
        using U = Values;
        using V = Weights;
        static constexpr bool UDecimal = IsDecimalNumber<U>;
        static constexpr bool VDecimal = IsDecimalNumber<V>;
        static constexpr bool BothDecimal = UDecimal && VDecimal;
        static constexpr bool NoneDecimal = !UDecimal && !VDecimal;

        template <class T>
        static constexpr bool IsIntegral = std::is_integral_v<T>
            || std::is_same_v<T, Int128> || std::is_same_v<T, Int256>
            || std::is_same_v<T, UInt128> || std::is_same_v<T, UInt256>;

        static constexpr bool BothOrNoneDecimal = BothDecimal || NoneDecimal;

        using Num = std::conditional<BothOrNoneDecimal,
            /// When both types are Decimal, we can perform computations in the Decimals only.
            /// When none of the types is Decimal, the result is always correct, the numerator is the next largest type up
            /// to Float64.
            NextAvgTypeT<Largest<U, V>>,

            std::conditional_t<UDecimal,
                /// When the numerator only is Decimal, we have to check the denominator:
                /// - If it's non-floating point, then we can set the numerator as the next Largest decimal.
                /// - Otherwise we won't be able to divide Decimal by double, so we leave the numerator as Float64.
                std::conditional_t<IsIntegral<V>,
                    NextAvgTypeT<U>,
                    Float64>,
                /// When the denominator only is Decimal, we check the numerator (as the above case).
                std::conditional_t<IsIntegral<U>,
                    NextAvgTypeT<U>,
                    Float64>>>;

        /**
         * When both types are Decimal, we can perform computations in the Decimals only.
         * When none of the types is Decimal, the result is always correct, the numerator is the next largest type up to
         * Float64.
         * We use #V only as the denominator accumulates the sum of the weights.
         *
         * When the numerator only is Decimal, we set the denominator to next Largest type.
         * - If the denominator was floating-point, the numerator would be Float64.
         * - If not, the numerator would be Decimal (as the denominator is integral).
         *
         * When the denominator only is Decimal, the numerator is either integral (so we leave the Decimal), or Float64,
         * so we set the denominator to Float64;
         */
        using Denom = std::conditional<VDecimal && !UDecimal && !IsIntegral<U>,
            Float64,
            NextAvgTypeT<V>>;
    };

    using Numerator = typename GetNumDenom::Num;
    using Denominator = typename GetNumDenom::Denom;
    using Fraction = RationalFraction<Numerator, Denominator>;

    /// If either Numerator or Denominator are Decimal, the result is also Decimal as everything was checked in
    /// GetNumDenom.
    using T = std::conditional_t<IsDecimalNumber<Numerator> && IsDecimalNumber<Denominator>,
        Largest<Numerator, Denominator>,
        std::conditional_t<IsDecimalNumber<Numerator>,
            Numerator,
            std::conditional_t<IsDecimalNumber<Denominator>,
                Denominator,
                bool>>>; // both numerator and denominator are non-decimal.

    using ResultType =       std::conditional_t<IsDecimalNumber<T>, T, Float64>;
    using ResultDataType =   std::conditional_t<IsDecimalNumber<T>, DataTypeDecimal<T>, DataTypeNumber<Float64>>;
    using ResultVectorType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<Float64>>;
};

/**
 * @tparam Values The values column type.
 * @tparam Weights The weights column type.
 */
template <class Values, class Weights>
class AggregateFunctionAvgWeighted final : public AggregateFunctionAvgBase<
    Values, Weights, AvgWeightedFunctionTypesDeduction, AggregateFunctionAvgWeighted<Values, Weights>>
{
public:
    using Base = AggregateFunctionAvgBase<
        Values, Weights, AvgWeightedFunctionTypesDeduction, AggregateFunctionAvgWeighted<Values, Weights>>;
    using Base::Base;

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & values = static_cast<const DecimalOrVectorCol<Values> &>(*columns[0]);
        const auto & weights = static_cast<const DecimalOrVectorCol<Weights> &>(*columns[1]);

        const auto value = values.getData()[row_num];
        const auto weight = weights.getData()[row_num];

        this->data(place).numerator += static_cast<typename Base::Numerator>(value) * weight;
        this->data(place).denominator += weight;
    }

    String getName() const override { return "avgWeighted"; }
};
}
