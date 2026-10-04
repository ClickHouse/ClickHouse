#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <DataTypes/NumberTraits.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

template <typename A>
struct SignImpl
{
    using ResultType = Int8;
    static constexpr bool allow_string_or_fixed_string = false;

    ALWAYS_INLINE static NO_SANITIZE_UNDEFINED ResultType apply(A a)
    {
        if constexpr (is_decimal<A> || is_floating_point<A>)
            return a < A(0) ? -1 : a == A(0) ? 0 : 1;
        else if constexpr (is_signed_v<A>)
        {
            /// Written as two `Int8` pieces rather than `(a >> shift) | !!a` cast to `Int8`: clang 23 folds the latter
            /// into a three-way compare with an `Int8` result (llvm/llvm-project#196828, llvm/llvm-project#196847),
            /// which the x86 backend scalarizes for 32- and 64-bit operands, one `cmp`/`sets`/`setg` per row instead
            /// of a vector compare. The two pieces vectorize on every target level.
            constexpr int shift = sizeof(A) * 8 - 1;
            ResultType result = static_cast<ResultType>(a >> shift);
            result |= static_cast<ResultType>(a > A(0));
            return result;
        }
        else if constexpr (is_unsigned_v<A>)
            return a == 0 ? 0 : 1;
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * arg, bool sign)
    {
        auto * result_type = b.getInt8Ty();
        auto * res_zero = llvm::ConstantInt::getSigned(result_type, 0);
        auto * res_one = llvm::ConstantInt::getSigned(result_type, 1);
        auto * res_minus_one = llvm::ConstantInt::getSigned(result_type, -1);

        const auto & type = arg->getType();
        if (type->isIntegerTy())
        {
            auto * zero = llvm::ConstantInt::get(type, 0, sign);
            auto * is_zero = b.CreateICmpEQ(arg, zero);

            if (sign)
            {
                auto * is_negative = b.CreateICmpSLT(arg, zero);
                auto * select_zero = b.CreateSelect(is_zero, res_zero, res_one);
                return b.CreateSelect(is_negative, res_minus_one, select_zero);
            }
            else
                return b.CreateSelect(is_zero, res_zero, res_one);
        }
        else if (type->isDoubleTy() || type->isFloatTy())
        {
            auto * zero = llvm::ConstantFP::get(type, 0.0);
            auto * is_zero = b.CreateFCmpOEQ(arg, zero);
            auto * is_negative = b.CreateFCmpOLT(arg, zero);

            auto * select_zero = b.CreateSelect(is_zero, res_zero, res_one);
            return b.CreateSelect(is_negative, res_minus_one, select_zero);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "SignImpl compilation expected native integer or floating point type");
    }


#endif
};

struct NameSign
{
    static constexpr auto name = "sign";
};
using FunctionSign = FunctionUnaryArithmetic<SignImpl, NameSign, false>;

template <>
struct FunctionUnaryArithmeticMonotonicity<NameSign>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const IDataType &, const Field &, const Field &)
    {
        return { .is_monotonic = true };
    }
};

REGISTER_FUNCTION(Sign)
{
    FunctionDocumentation::Description description = R"(
Returns the sign of a real number.
)";
    FunctionDocumentation::Syntax syntax = "sign(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Values from -∞ to +∞.", {"(U)Int*", "Decimal*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `-1` for `x < 0`, `0` for `x = 0`, `1` for `x > 0`.", {"Int8"}};
    FunctionDocumentation::Examples examples = {
        {"Sign for zero", "SELECT sign(0)", "0"},
        {"Sign for positive", "SELECT sign(1)", "1"},
        {"Sign for negative", "SELECT sign(-1)", "-1"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 2};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionSign>(documentation, FunctionFactory::Case::Insensitive);
}

}
