#pragma once

namespace DB
{

/// These classes should be present in DB namespace (cannot place them into namelesspace)
template <typename> struct AbsImpl;
template <typename> struct NegateImpl;
template <typename, typename> struct PlusImpl;
template <typename, typename> struct MinusImpl;
template <typename, typename> struct MultiplyImpl;
template <typename, typename> struct DivideFloatingImpl;
template <typename, typename> struct DivideIntegralImpl;
template <typename, typename> struct DivideIntegralOrZeroImpl;
template <typename, typename> struct LeastBaseImpl;
template <typename, typename> struct GreatestBaseImpl;
template <typename, typename> struct ModuloImpl;
template <typename, typename> struct EqualsOp;
template <typename, typename> struct NotEqualsOp;
template <typename, typename> struct LessOrEqualsOp;
template <typename, typename> struct GreaterOrEqualsOp;
template <typename, typename> struct BitHammingDistanceImpl;

template <typename>
struct SignImpl;

template <template <typename, typename> typename Op1, template <typename, typename> typename Op2>
struct IsSameOperation
{
    static constexpr bool value = std::is_same_v<Op1<UInt8, UInt8>, Op2<UInt8, UInt8>>;
};

template <template <typename> typename Op>
struct IsUnaryOperation
{
    static constexpr bool abs = std::is_same_v<Op<Int8>, AbsImpl<Int8>>;
    static constexpr bool negate = std::is_same_v<Op<Int8>, NegateImpl<Int8>>;
    static constexpr bool sign = std::is_same_v<Op<Int8>, SignImpl<Int8>>;
};

template <template <typename, typename> typename Op>
struct IsOperation
{
    static constexpr bool equals = IsSameOperation<Op, EqualsOp>::value;
    static constexpr bool not_equals = IsSameOperation<Op, NotEqualsOp>::value;
    static constexpr bool less_or_equals = IsSameOperation<Op, LessOrEqualsOp>::value;
    static constexpr bool greater_or_equals = IsSameOperation<Op, GreaterOrEqualsOp>::value;

    static constexpr bool plus = IsSameOperation<Op, PlusImpl>::value;
    static constexpr bool minus = IsSameOperation<Op, MinusImpl>::value;
    static constexpr bool multiply = IsSameOperation<Op, MultiplyImpl>::value;
    static constexpr bool div_floating = IsSameOperation<Op, DivideFloatingImpl>::value;
    static constexpr bool div_int = IsSameOperation<Op, DivideIntegralImpl>::value;
    static constexpr bool div_int_or_zero = IsSameOperation<Op, DivideIntegralOrZeroImpl>::value;
    static constexpr bool modulo = IsSameOperation<Op, ModuloImpl>::value;
    static constexpr bool least = IsSameOperation<Op, LeastBaseImpl>::value;
    static constexpr bool greatest = IsSameOperation<Op, GreatestBaseImpl>::value;

    static constexpr bool bit_hamming_distance = IsSameOperation<Op, BitHammingDistanceImpl>::value;

    static constexpr bool division = div_floating || div_int || div_int_or_zero;

    static constexpr bool allow_decimal = plus || minus || multiply || division || least || greatest;
};

}
