#pragma once

#include <cmath>
#include <type_traits>
#include <Common/Exception.h>
#include <Common/NaNUtils.h>
#include <DataTypes/NumberTraits.h>

#include "DataTypes/Native.h"
#include "config.h"

#if USE_EMBEDDED_COMPILER
#    include <Core/ValuesWithType.h>
#    include <llvm/IR/IRBuilder.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_DIVISION;
    extern const int LOGICAL_ERROR;
}

#if USE_EMBEDDED_COMPILER

template <typename F>
static llvm::Value * compileWithNullableValues(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool is_signed, F && compile_func)
{
    auto * left_type = left->getType();
    auto * right_type = right->getType();

    if (!left_type->isStructTy() && !right_type->isStructTy())
    {
        // Both arguments are not nullable.
        return compile_func(b, left, right, is_signed);
    }

    auto * denull_left = left_type->isStructTy() ? b.CreateExtractValue(left, {1}) : left;
    auto * denull_right = right_type->isStructTy() ? b.CreateExtractValue(right, {1}) : right;
    auto * denull_result = compile_func(b, denull_left, denull_right, is_signed);

    auto * nullable_result_type = toNullableType(b, denull_result->getType());
    llvm::Value * nullable_result = llvm::Constant::getNullValue(nullable_result_type);
    nullable_result = b.CreateInsertValue(nullable_result, denull_result, {0});

    auto * result_is_null = b.CreateExtractValue(nullable_result, {1});
    if (left_type->isStructTy())
        result_is_null = b.CreateOr(result_is_null, b.CreateExtractValue(left, {1}));
    if (right_type->isStructTy())
        result_is_null = b.CreateOr(result_is_null, b.CreateExtractValue(right, {1}));

    return b.CreateInsertValue(nullable_result, result_is_null, {1});
}

#endif

template <typename A, typename B>
inline void throwIfDivisionLeadsToFPE(A a, B b)
{
    /// Is it better to use siglongjmp instead of checks?

    if (unlikely(b == 0))
        throw Exception(ErrorCodes::ILLEGAL_DIVISION, "Division by zero");

    /// http://avva.livejournal.com/2548306.html
    if constexpr (is_signed_v<A> && is_signed_v<B>)
        if (unlikely(a == std::numeric_limits<A>::min() && b == -1))
            throw Exception(ErrorCodes::ILLEGAL_DIVISION, "Division of minimal signed number by minus one");
}

template <typename A, typename B>
inline bool divisionLeadsToFPE(A a, B b)
{
    if (unlikely(b == 0))
        return true;

    if constexpr (is_signed_v<A> && is_signed_v<B>)
        if (unlikely(a == std::numeric_limits<A>::min() && b == -1))
            return true;

    return false;
}

template <typename A, typename B>
inline auto checkedDivision(A a, B b)
{
    throwIfDivisionLeadsToFPE(a, b);

    if constexpr (is_big_int_v<A> && is_floating_point<B>)
        return static_cast<B>(a) / b;
    else if constexpr (is_big_int_v<B> && is_floating_point<A>)
        return a / static_cast<A>(b);
    else if constexpr (is_big_int_v<A> && is_big_int_v<B>)
        return static_cast<A>(a / b);
    else if constexpr (!is_big_int_v<A> && is_big_int_v<B>)
        return static_cast<A>(B(a) / b);
    else
        return a / b;
}


template <typename A, typename B>
struct DivideIntegralImpl
{
    using ResultType = typename NumberTraits::ResultOfIntegerDivision<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static Result apply(A a, B b, NullMap::value_type * m [[maybe_unused]] = nullptr)
    {
        using CastA = std::conditional_t<is_big_int_v<B> && std::is_same_v<A, UInt8>, uint8_t, A>;
        using CastB = std::conditional_t<is_big_int_v<A> && std::is_same_v<B, UInt8>, uint8_t, B>;

        /// Otherwise overflow may occur due to integer promotion. Example: int8_t(-1) / uint64_t(2).
        /// NOTE: overflow is still possible when dividing large signed number to large unsigned number or vice-versa. But it's less harmful.
        if constexpr (is_integer<A> && is_integer<B> && (is_signed_v<A> || is_signed_v<B>))
        {
            using SignedCastA = make_signed_t<CastA>;
            using SignedCastB = std::conditional_t<sizeof(A) <= sizeof(B), make_signed_t<CastB>, SignedCastA>;

            return static_cast<Result>(checkedDivision(static_cast<SignedCastA>(a), static_cast<SignedCastB>(b)));
        }
        else
        {
            /// Comparisons are not strict to avoid rounding issues when operand is implicitly cast to float.

            if constexpr (is_floating_point<A>)
                if (isNaN(a) || a >= std::numeric_limits<CastA>::max() || a <= std::numeric_limits<CastA>::lowest())
                    throw Exception(ErrorCodes::ILLEGAL_DIVISION, "Cannot perform integer division on infinite or too large floating point numbers");

            if constexpr (is_floating_point<B>)
                if (isNaN(b) || b >= std::numeric_limits<CastB>::max() || b <= std::numeric_limits<CastB>::lowest())
                    throw Exception(ErrorCodes::ILLEGAL_DIVISION, "Cannot perform integer division on infinite or too large floating point numbers");

            auto res = checkedDivision(CastA(a), CastB(b));

            if constexpr (is_floating_point<decltype(res)>)
                if (isNaN(res) || res >= static_cast<double>(std::numeric_limits<Result>::max()) || res <= std::numeric_limits<Result>::lowest())
                    throw Exception(ErrorCodes::ILLEGAL_DIVISION, "Cannot perform integer division, because it will produce infinite or too large number");

            return static_cast<Result>(res);
        }
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// don't know how to throw from LLVM IR
#endif
};

template <typename A, typename B>
struct ModuloImpl
{
    using ResultType = typename NumberTraits::ResultOfModulo<A, B>::Type;
    using IntegerAType = typename NumberTraits::ToInteger<A>::Type;
    using IntegerBType = typename NumberTraits::ToInteger<B>::Type;

    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static Result apply(A a, B b, NullMap::value_type * m [[maybe_unused]] = nullptr)
    {
        if constexpr (is_floating_point<ResultType>)
        {
            /// This computation is similar to `fmod` but the latter is not inlined and has 40 times worse performance.
            return static_cast<ResultType>(a) - trunc(static_cast<ResultType>(a) / static_cast<ResultType>(b)) * static_cast<ResultType>(b);
        }
        else
        {
            if constexpr (is_floating_point<A>)
                if (isNaN(a) || a > std::numeric_limits<IntegerAType>::max() || a < std::numeric_limits<IntegerAType>::lowest())
                    throw Exception(ErrorCodes::ILLEGAL_DIVISION, "Cannot perform integer division on infinite or too large floating point numbers");

            if constexpr (is_floating_point<B>)
                if (isNaN(b) || b > std::numeric_limits<IntegerBType>::max() || b < std::numeric_limits<IntegerBType>::lowest())
                    throw Exception(ErrorCodes::ILLEGAL_DIVISION, "Cannot perform integer division on infinite or too large floating point numbers");

            throwIfDivisionLeadsToFPE(IntegerAType(a), IntegerBType(b));

            if constexpr (is_big_int_v<IntegerAType> || is_big_int_v<IntegerBType>)
            {
                using CastA = std::conditional_t<std::is_same_v<IntegerAType, UInt8>, uint8_t, IntegerAType>;
                using CastB = std::conditional_t<std::is_same_v<IntegerBType, UInt8>, uint8_t, IntegerBType>;

                CastA int_a(a);
                CastB int_b(b);

                if constexpr (is_big_int_v<IntegerBType> && sizeof(IntegerAType) <= sizeof(IntegerBType))
                    return static_cast<Result>(static_cast<CastB>(int_a) % int_b);
                else
                    return static_cast<Result>(int_a % static_cast<CastA>(int_b));
            }
            else
                return static_cast<Result>(IntegerAType(a) % IntegerBType(b));
        }
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true; /// Ignore exceptions in LLVM IR

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool is_signed)
    {
        return compileWithNullableValues(
            b,
            left,
            right,
            is_signed,
            [](auto & b_, auto * left_, auto * right_, auto is_signed_) { return compileImpl(b_, left_, right_, is_signed_); });
    }

    static llvm::Value * compileImpl(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool is_signed)
    {
        if (left->getType()->isFloatingPointTy())
            return b.CreateFRem(left, right);
        else if (left->getType()->isIntegerTy())
            return is_signed ? b.CreateSRem(left, right) : b.CreateURem(left, right);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ModuloImpl compilation expected native integer or floating point type");
    }

    #endif
};

template <typename A, typename B>
struct ModuloLegacyImpl : ModuloImpl<A, B>
{
    using ResultType = typename NumberTraits::ResultOfModuloLegacy<A, B>::Type;

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// moduloLegacy is only used in partition key expression
#endif
};

template <typename A, typename B>
struct PositiveModuloImpl : ModuloImpl<A, B>
{
    using OriginResultType = typename ModuloImpl<A, B>::ResultType;
    using ResultType = typename NumberTraits::ResultOfPositiveModulo<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b, NullMap::value_type * m [[maybe_unused]] = nullptr)
    {
        auto res = ModuloImpl<A, B>::template apply<OriginResultType>(a, b);
        if constexpr (is_signed_v<A>)
        {
            if (res < 0)
            {
                if constexpr (is_unsigned_v<B>)
                    res += static_cast<OriginResultType>(b);
                else
                {
                    if (b == std::numeric_limits<B>::lowest())
                        throw Exception(ErrorCodes::ILLEGAL_DIVISION, "Division by the most negative number");
                    res += b >= 0 ? static_cast<OriginResultType>(b) : static_cast<OriginResultType>(-b);
                }
            }
        }
        return static_cast<ResultType>(res);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true; /// Ignore exceptions in LLVM IR

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool is_signed)
    {
        return compileWithNullableValues(
            b,
            left,
            right,
            is_signed,
            [](auto & b_, auto * left_, auto * right_, auto is_signed_) { return compileImpl(b_, left_, right_, is_signed_); });
    }

    static llvm::Value * compileImpl(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool is_signed)
    {
        auto * result = ModuloImpl<A, B>::compileImpl(b, left, right, is_signed);
        if (is_signed)
        {
            /// If result is negative, result += abs(right).
            auto * zero = llvm::Constant::getNullValue(result->getType());
            auto * is_negative = b.CreateICmpSLT(result, zero);
            auto * abs_right = b.CreateSelect(b.CreateICmpSLT(right, zero), b.CreateNeg(right), right);
            return b.CreateSelect(is_negative, b.CreateAdd(result, abs_right), result);
        }
        else
            return result;
    }
#endif

};

template <typename A, typename B>
struct DivideFloatingImpl
{
    using ResultType = typename NumberTraits::ResultOfFloatingPointDivision<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static NO_SANITIZE_UNDEFINED Result apply(A a [[maybe_unused]], B b [[maybe_unused]], NullMap::value_type * m [[maybe_unused]] = nullptr)
    {
        return static_cast<Result>(a) / static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (left->getType()->isIntegerTy())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "DivideFloatingImpl expected a floating-point type");
        return b.CreateFDiv(left, right);
    }
#endif
};

}
