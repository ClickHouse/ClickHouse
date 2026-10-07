#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/FunctionExactDivide.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool exact_division_for_integers;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename A, typename B>
struct DivideFloatingImpl
{
    using ResultType = typename NumberTraits::ResultOfFloatingPointDivision<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static NO_SANITIZE_UNDEFINED Result apply(A a [[maybe_unused]], B b [[maybe_unused]])
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

template <typename A, typename B>
struct DivideFloatingOrNullImpl : DivideFloatingImpl<A, B>
{
#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false;
#endif
};

struct NameDivide { static constexpr auto name = "divide"; };
using FunctionDivideLegacy = BinaryArithmeticOverloadResolver<DivideFloatingImpl, NameDivide>;

class FunctionDivide final : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = NameDivide::name;

    explicit FunctionDivide(ContextPtr context)
        : legacy(FunctionDivideLegacy::create(context))
        , exact(std::make_unique<FunctionToOverloadResolverAdaptor>(createFunctionExactDivide()))
    {
    }

    static FunctionOverloadResolverPtr create(ContextPtr context)
    {
        if (!context || !context->getSettingsRef()[Setting::exact_division_for_integers])
            return FunctionDivideLegacy::create(context);
        return std::make_unique<FunctionDivide>(std::move(context));
    }

    String getName() const override
    {
        return name;
    }
    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return delegate(arguments)->getReturnType(arguments);
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override
    {
        return delegate(arguments)->build(arguments);
    }

private:
    const FunctionOverloadResolverPtr & delegate(const ColumnsWithTypeAndName & arguments) const
    {
        if (isInteger(removeNullable(recursiveRemoveLowCardinality(arguments[0].type)))
            && isInteger(removeNullable(recursiveRemoveLowCardinality(arguments[1].type))))
            return exact;
        return legacy;
    }

    FunctionOverloadResolverPtr legacy;
    FunctionOverloadResolverPtr exact;
};

REGISTER_FUNCTION(Divide)
{
    FunctionDocumentation::Description description = R"(
Calculates the quotient of two values `a` and `b`. The `/` operator is equivalent to this function.
For integer and floating-point arguments, the result type is [Float64](/reference/data-types/float) by default.
With `exact_division_for_integers = 1`, two integer arguments instead produce an integer quotient truncated toward zero.
The result has the width of the first argument and is signed if either argument is signed.
Division by zero or a quotient outside the result type's range throws an exception in this mode.

When a `Decimal` argument is combined with another `Decimal` or an integer, the result is `Decimal`.
For two `Decimal` arguments, the result uses the wider storage type and the first argument's scale.
For a `Decimal` and an integer, it uses the `Decimal` argument's type and scale.
The setting does not change these rules or the behavior of `intDiv`, `divideDecimal`, and `divideOrNull`.

<Note>
Floating-point division by `0` returns `inf`, `-inf`, or `nan`. `Decimal` division by zero throws an exception.
</Note>
    )";
    FunctionDocumentation::Syntax syntax = "divide(x, y)";
    FunctionDocumentation::Argument argument1 = {"x", "Dividend"};
    FunctionDocumentation::Argument argument2 = {"y", "Divisor"};
    FunctionDocumentation::Arguments arguments = {argument1, argument2};
    FunctionDocumentation::ReturnedValue returned_value = {"The quotient of x and y"};
    FunctionDocumentation::Example example1 = {"Dividing two numbers", "SELECT divide(25,5) AS quotient, toTypeName(quotient)", "5\tFloat64"};
    FunctionDocumentation::Example example2 = {"Dividing by zero", "SELECT divide(25,0)", "inf"};
    FunctionDocumentation::Examples examples = {example1, example2};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionDivide>(documentation);
}

struct NameDivideOrNull { static constexpr auto name = "divideOrNull"; };
using FunctionDivideOrNull = BinaryArithmeticOverloadResolver<DivideFloatingOrNullImpl, NameDivideOrNull>;

REGISTER_FUNCTION(DivideOrNull)
{
    FunctionDocumentation::Description description = R"(
Same as `divide` with `exact_division_for_integers = 0`, but returns `NULL` when dividing by zero.
The `exact_division_for_integers` setting does not affect this function.
    )";
    FunctionDocumentation::Syntax syntax = "divideOrNull(x, y)";
    FunctionDocumentation::Argument argument1 = {"x", "Dividend"};
    FunctionDocumentation::Argument argument2 = {"y", "Divisor"};
    FunctionDocumentation::Arguments arguments = {argument1, argument2};
    FunctionDocumentation::ReturnedValue returned_value = {"The quotient of x and y, or NULL."};
    FunctionDocumentation::Example example1 = {"Dividing by zero", "SELECT divideOrNull(25, 0)", "\\N"};
    FunctionDocumentation::Examples examples = {example1};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionDivideOrNull>(documentation);
}

}
