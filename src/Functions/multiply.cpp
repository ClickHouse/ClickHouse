#include <type_traits>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/FunctionFactory.h>
#include <base/arithmeticOverflow.h>

namespace DB
{

template <typename A, typename B>
struct MultiplyImpl
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static NO_SANITIZE_UNDEFINED Result apply(A a, B b)
    {
        if constexpr (is_big_int_v<A> || is_big_int_v<B>)
        {
            using CastA = std::conditional_t<is_floating_point<B>, B, A>;
            using CastB = std::conditional_t<is_floating_point<A>, A, B>;

            return static_cast<Result>(static_cast<CastA>(a)) * static_cast<Result>(static_cast<CastB>(b));
        }
        else
            return static_cast<Result>(a) * static_cast<Result>(b);
    }

    /// Apply operation and check overflow. It's used for Decimal operations. @returns true if overflowed, false otherwise.
    template <typename Result = ResultType>
    static bool apply(A a, B b, Result & c)
    {
        if constexpr (std::is_same_v<Result, float> || std::is_same_v<Result, double>)
        {
            c = static_cast<Result>(a) * b;
            return false;
        }
        else
            return common::mulOverflow(static_cast<Result>(a), b, c);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        return left->getType()->isIntegerTy() ? b.CreateMul(left, right) : b.CreateFMul(left, right);
    }
#endif
};

struct NameMultiply
{
    static constexpr auto name = "multiply";
};
using FunctionMultiply = BinaryArithmeticOverloadResolver<MultiplyImpl, NameMultiply>;

class FunctionSqr final : public IFunction
{
public:
    static constexpr auto name = "sqr";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionSqr>(context); }

    explicit FunctionSqr(ContextPtr context)
        : multiply(FunctionFactory::instance().get("multiply", context))
    {
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool isNameInsensitive() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        ColumnWithTypeAndName argument;
        argument.type = arguments.front();
        return buildMultiply(argument)->getResultType();
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return buildMultiply(arguments.front())->getResultType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto & argument = arguments.front();
        return buildMultiply(argument)->execute({argument, argument}, result_type, input_rows_count, /* dry_run = */ false);
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes & arguments, const DataTypePtr & /*result_type*/) const override
    {
        ColumnWithTypeAndName argument;
        argument.type = arguments.front();
        return buildMultiply(argument)->isCompilable();
    }

    llvm::Value *
    compileImpl(llvm::IRBuilderBase & builder, const ValuesWithType & arguments, const DataTypePtr & /*result_type*/) const override
    {
        ColumnWithTypeAndName argument;
        argument.type = arguments.front().type;
        auto multiply_function = buildMultiply(argument);

        ValuesWithType multiply_arguments;
        multiply_arguments.push_back(arguments.front());
        multiply_arguments.push_back(arguments.front());
        return multiply_function->compile(builder, multiply_arguments);
    }
#endif

private:
    FunctionBasePtr buildMultiply(const ColumnWithTypeAndName & argument) const { return multiply->build({argument, argument}); }

    FunctionOverloadResolverPtr multiply;
};

REGISTER_FUNCTION(Multiply)
{
    FunctionDocumentation::Description description = "Calculates the product of two values `x` and `y`.";
    FunctionDocumentation::Syntax syntax = "multiply(x, y)";
    FunctionDocumentation::Arguments arguments
        = {{"x", "factor.", {"(U)Int*", "Float*", "Decimal"}}, {"y", "factor.", {"(U)Int*", "Float*", "Decimal"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the product of x and y"};
    FunctionDocumentation::Examples examples = {{"Multiplying two numbers", "SELECT multiply(5,5)", "25"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiply>(documentation);
}

REGISTER_FUNCTION(Sqr)
{
    FunctionDocumentation::Description description = "Calculates the square of a value `x`.";
    FunctionDocumentation::Syntax syntax = "sqr(x)";
    FunctionDocumentation::Arguments arguments = {{"x", "Value to square.", {"(U)Int*", "Float*", "Decimal"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the product of `x` multiplied by itself."};
    FunctionDocumentation::Examples examples = {{"Squaring a number", "SELECT sqr(5)", "25"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionSqr>(documentation, FunctionFactory::Case::Insensitive);
}
}
