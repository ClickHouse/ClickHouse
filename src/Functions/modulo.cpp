#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

#if defined(__SSE2__)
#    define LIBDIVIDE_SSE2 1
#endif

#include <libdivide.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_DIVISION;
}

namespace
{

/// Optimizations for integer modulo by a constant.

template <typename A, typename B>
struct ModuloByConstantImpl
    : BinaryOperation<A, B, ModuloImpl<A, B>>
{
    using Op = ModuloImpl<A, B>;
    using ResultType = typename Op::ResultType;
    static const constexpr bool allow_fixed_string = false;

    template <OpCase op_case>
    static void NO_INLINE process(const A * __restrict a, const B * __restrict b, ResultType * __restrict c, size_t size)
    {
        if constexpr (op_case == OpCase::Vector)
            for (size_t i = 0; i < size; ++i)
                c[i] = Op::template apply<ResultType>(a[i], b[i]);
        else if constexpr (op_case == OpCase::LeftConstant)
            for (size_t i = 0; i < size; ++i)
                c[i] = Op::template apply<ResultType>(*a, b[i]);
        else
            vectorConstant(a, *b, c, size);
    }

    static ResultType process(A a, B b) { return Op::template apply<ResultType>(a, b); }

    static void NO_INLINE NO_SANITIZE_UNDEFINED vectorConstant(const A * __restrict src, B b, ResultType * __restrict dst, size_t size)
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

        /// Modulo with too small divisor.
        if (unlikely((std::is_signed_v<B> && b == -1) || b == 1))
        {
            for (size_t i = 0; i < size; ++i)
                dst[i] = 0;
            return;
        }

        /// Modulo with too large divisor.
        if (unlikely(b > std::numeric_limits<A>::max()
            || (std::is_signed_v<A> && std::is_signed_v<B> && b < std::numeric_limits<A>::lowest())))
        {
            for (size_t i = 0; i < size; ++i)
                dst[i] = src[i];
            return;
        }

#pragma GCC diagnostic pop

        if (unlikely(static_cast<A>(b) == 0))
            throw Exception("Division by zero", ErrorCodes::ILLEGAL_DIVISION);

        /// Division by min negative value.
        if (std::is_signed_v<B> && b == std::numeric_limits<B>::lowest())
            throw Exception("Division by the most negative number", ErrorCodes::ILLEGAL_DIVISION);

        /// Modulo of division by negative number is the same as the positive number.
        if (b < 0)
            b = -b;

        /// Here we failed to make the SSE variant from libdivide give an advantage.

        if (b & (b - 1))
        {
            libdivide::divider<A> divider(b);
            for (size_t i = 0; i < size; ++i)
                dst[i] = src[i] - (src[i] / divider) * b; /// NOTE: perhaps, the division semantics with the remainder of negative numbers is not preserved.
        }
        else
        {
            // gcc libdivide doesn't work well for pow2 division
            auto mask = b - 1;
            for (size_t i = 0; i < size; ++i)
                dst[i] = src[i] & mask;
        }
    }
};

template <typename A, typename B>
struct ModuloLegacyByConstantImpl : ModuloByConstantImpl<A, B>
{
    using Op = ModuloLegacyImpl<A, B>;
};
}

/** Specializations are specified for dividing numbers of the type UInt64 and UInt32 by the numbers of the same sign.
  * Can be expanded to all possible combinations, but more code is needed.
  */

namespace impl_
{
template <> struct BinaryOperationImpl<UInt64, UInt8, ModuloImpl<UInt64, UInt8>> : ModuloByConstantImpl<UInt64, UInt8> {};
template <> struct BinaryOperationImpl<UInt64, UInt16, ModuloImpl<UInt64, UInt16>> : ModuloByConstantImpl<UInt64, UInt16> {};
template <> struct BinaryOperationImpl<UInt64, UInt32, ModuloImpl<UInt64, UInt32>> : ModuloByConstantImpl<UInt64, UInt32> {};
template <> struct BinaryOperationImpl<UInt64, UInt64, ModuloImpl<UInt64, UInt64>> : ModuloByConstantImpl<UInt64, UInt64> {};

template <> struct BinaryOperationImpl<UInt32, UInt8, ModuloImpl<UInt32, UInt8>> : ModuloByConstantImpl<UInt32, UInt8> {};
template <> struct BinaryOperationImpl<UInt32, UInt16, ModuloImpl<UInt32, UInt16>> : ModuloByConstantImpl<UInt32, UInt16> {};
template <> struct BinaryOperationImpl<UInt32, UInt32, ModuloImpl<UInt32, UInt32>> : ModuloByConstantImpl<UInt32, UInt32> {};
template <> struct BinaryOperationImpl<UInt32, UInt64, ModuloImpl<UInt32, UInt64>> : ModuloByConstantImpl<UInt32, UInt64> {};

template <> struct BinaryOperationImpl<Int64, Int8, ModuloImpl<Int64, Int8>> : ModuloByConstantImpl<Int64, Int8> {};
template <> struct BinaryOperationImpl<Int64, Int16, ModuloImpl<Int64, Int16>> : ModuloByConstantImpl<Int64, Int16> {};
template <> struct BinaryOperationImpl<Int64, Int32, ModuloImpl<Int64, Int32>> : ModuloByConstantImpl<Int64, Int32> {};
template <> struct BinaryOperationImpl<Int64, Int64, ModuloImpl<Int64, Int64>> : ModuloByConstantImpl<Int64, Int64> {};

template <> struct BinaryOperationImpl<Int32, Int8, ModuloImpl<Int32, Int8>> : ModuloByConstantImpl<Int32, Int8> {};
template <> struct BinaryOperationImpl<Int32, Int16, ModuloImpl<Int32, Int16>> : ModuloByConstantImpl<Int32, Int16> {};
template <> struct BinaryOperationImpl<Int32, Int32, ModuloImpl<Int32, Int32>> : ModuloByConstantImpl<Int32, Int32> {};
template <> struct BinaryOperationImpl<Int32, Int64, ModuloImpl<Int32, Int64>> : ModuloByConstantImpl<Int32, Int64> {};
}

struct NameModulo { static constexpr auto name = "modulo"; };
using FunctionModulo = BinaryArithmeticOverloadResolver<ModuloImpl, NameModulo, false>;

void registerFunctionModulo(FunctionFactory & factory)
{
    factory.registerFunction<FunctionModulo>();
    factory.registerAlias("mod", "modulo", FunctionFactory::CaseInsensitive);
}

struct NameModuloLegacy { static constexpr auto name = "moduloLegacy"; };
using FunctionModuloLegacy = BinaryArithmeticOverloadResolver<ModuloLegacyImpl, NameModuloLegacy, false>;

void registerFunctionModuloLegacy(FunctionFactory & factory)
{
    factory.registerFunction<FunctionModuloLegacy>();
}

}
