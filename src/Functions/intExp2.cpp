#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/intExp2.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
}

template <typename A>
struct IntExp2Impl
{
    using ResultType = UInt64;
    static constexpr bool allow_string_or_fixed_string = false;

    static ResultType apply([[maybe_unused]] A a)
    {
        if constexpr (is_big_int_v<A>)
        {
            throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "intExp2 not implemented for big integers");
        }
        else
        {
            if constexpr (is_floating_point<A>)
            {
                /// Self-inequality is the canonical NaN check — works for `Float32`/`Float64`
                /// and for `BFloat16` (which has no `std::isnan` overload).
                if (a != a)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "intExp2 must not be called with nan");
            }

            /// Range-check before the narrowing `static_cast<int>` below. Without this, integer
            /// inputs whose magnitude exceeds `INT_MAX` (e.g. `UInt32` near `2^32`, `Int64`
            /// near `INT64_MAX`) get implementation-defined truncation and return nonsense.
            if constexpr (is_signed_v<A>)
                if (a < A{0})
                    return 0;
            if (a >= A{64})
                return std::numeric_limits<UInt64>::max();

            return intExp2(static_cast<int>(a));
        }
    }

#if USE_EMBEDDED_COMPILER
    /// JIT only handles native integer inputs. Floats fall through to `apply` so NaN throws
    /// (we can't throw from JIT-compiled code), and big ints fall through to `apply` so the
    /// `NOT_IMPLEMENTED` exception is preserved.
    static constexpr bool compilable = is_integer<A> && !is_big_int_v<A>;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * arg, bool is_signed)
    {
        if (!arg->getType()->isIntegerTy())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "IntExp2Impl expected an integral type");

        /// Widen the input to `i64`. For signed inputs, sign-extension preserves the sign bit
        /// so that originally-negative values can be detected with `ICmpSLT(.., 0)`.
        auto * result_type = b.getInt64Ty();
        auto * arg_i64 = b.CreateIntCast(arg, result_type, is_signed);

        auto * one = llvm::ConstantInt::get(result_type, 1);
        auto * sixty_three = llvm::ConstantInt::get(result_type, 63);
        auto * zero64 = llvm::ConstantInt::get(result_type, 0);
        auto * uint_max = llvm::ConstantInt::getAllOnesValue(result_type);

        /// Negative inputs return 0 (matches `intExp2(int x)` for `x < 0`).
        /// For unsigned inputs the branch folds away because `is_negative` is a constant `false`.
        llvm::Value * is_negative = is_signed
            ? static_cast<llvm::Value *>(b.CreateICmpSLT(arg_i64, zero64))
            : static_cast<llvm::Value *>(b.getFalse());

        /// Inputs `> 63` saturate to `UINT64_MAX`. The mask makes the `shl` defined when
        /// `is_too_large` is true (the result of `shl` is then discarded by the outer select).
        auto * is_too_large = b.CreateICmpUGT(arg_i64, sixty_three);
        auto * masked = b.CreateAnd(arg_i64, sixty_three);
        auto * shifted = b.CreateShl(one, masked);
        auto * with_overflow = b.CreateSelect(is_too_large, uint_max, shifted);

        return b.CreateSelect(is_negative, zero64, with_overflow);
    }
#endif
};

namespace
{

struct NameIntExp2 { static constexpr auto name = "intExp2"; };
using FunctionIntExp2 = FunctionUnaryArithmetic<IntExp2Impl, NameIntExp2, false>;

}

template <> struct FunctionUnaryArithmeticMonotonicity<NameIntExp2>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const IDataType & type, const Field & left, const Field & right)
    {
        Float64 left_float = left.isNull() ? -std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), left);
        Float64 right_float = right.isNull() ? std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), right);

        if (left_float < 0 || right_float > 63)
            return {};

        /// For floating-point inputs, the value is truncated to int, so `intExp2` is not
        /// strictly monotonic (e.g. `intExp2(1.5)` == `intExp2(1.9)`).
        bool is_strict = type.isValueRepresentedByInteger();
        return { .is_monotonic = true, .is_strict = is_strict };
    }
};

REGISTER_FUNCTION(IntExp2)
{
    FunctionDocumentation::Description description = R"(
Like [exp2](#exp2) but returns a `UInt64` number.
)";
    FunctionDocumentation::Syntax syntax = "intExp2(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The exponent.", {"Int*", "UInt*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns 2^x.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT intExp2(3);", "8"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIntExp2>(documentation);
}

}
