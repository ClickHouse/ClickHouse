#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <DataTypes/NumberTraits.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

template <typename A>
struct BitNotImpl
{
    using ResultType = typename NumberTraits::ResultOfBitNot<A>::Type;
    static constexpr bool allow_string_or_fixed_string = true;

    static ResultType NO_SANITIZE_UNDEFINED apply(A a)
    {
        return ~static_cast<ResultType>(a);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * arg, bool)
    {
        if (!arg->getType()->isIntegerTy())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "BitNotImpl expected an integral type");
        return b.CreateNot(arg);
    }
#endif
};

struct NameBitNot { static constexpr auto name = "bitNot"; };
using FunctionBitNot = FunctionUnaryArithmetic<BitNotImpl, NameBitNot, true>;

}

template <> struct FunctionUnaryArithmeticMonotonicity<NameBitNot>
{
    static bool has() { return false; }
    static IFunction::Monotonicity get(const IDataType &, const Field &, const Field &)
    {
        return {};
    }
};

REGISTER_FUNCTION(BitNot)
{
    FunctionDocumentation::Description description = "Performs the bitwise NOT operation.";
    FunctionDocumentation::Syntax syntax = "bitNot(a)";
    FunctionDocumentation::Arguments arguments = {
        {"a", "Value for which to apply bitwise NOT operation.", {"(U)Int*", "Float*", "String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the result of `~a` i.e `a` with bits flipped."};
    FunctionDocumentation::Examples examples = {{"Usage example",
        R"(
SELECT
    CAST('5', 'UInt8') AS original,
    bin(original) AS original_binary,
    bitNot(original) AS result,
    bin(bitNot(original)) AS result_binary;
        )",
        R"(
┌─original─┬─original_binary─┬─result─┬─result_binary─┐
│        5 │ 00000101        │    250 │ 11111010      │
└──────────┴─────────────────┴────────┴───────────────┘
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Bit;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBitNot>(documentation);
}

}
