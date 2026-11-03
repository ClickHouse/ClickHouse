#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/FunctionExactDivide.h>

namespace DB
{
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

    /// Documentation-only — see `PlusImpl::signature_documentation`.
    static constexpr auto signature_documentation = "(Any, Any) -> Any";

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

/// Keep the serialized AST and the resolved expression under the same name. In particular, key
/// expressions are built by the legacy analyzer while predicates use the query-tree analyzer.
class FunctionDivideExactBase final : public IFunctionBase
{
public:
    explicit FunctionDivideExactBase(FunctionBasePtr implementation_) : implementation(std::move(implementation_))
    {
    }

    String getName() const override
    {
        return "divideExact";
    }
    const DataTypes & getArgumentTypes() const override
    {
        return implementation->getArgumentTypes();
    }
    const DataTypePtr & getResultType() const override
    {
        return implementation->getResultType();
    }
    void updateHash(SipHash & hash) const override
    {
        implementation->updateHash(hash);
    }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName & arguments) const override
    {
        return implementation->prepare(arguments);
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override
    {
        return implementation->isSuitableForShortCircuitArgumentsExecution(arguments);
    }

    bool isSuitableForConstantFolding() const override
    {
        return implementation->isSuitableForConstantFolding();
    }
    ColumnPtr getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override
    {
        return implementation->getConstantResultForNonConstArguments(arguments, result_type);
    }
    bool isInjective(const ColumnsWithTypeAndName & arguments) const override
    {
        return implementation->isInjective(arguments);
    }
    bool isNameInsensitive() const override
    {
        return implementation->isNameInsensitive();
    }
    bool hasInformationAboutMonotonicity() const override
    {
        return implementation->hasInformationAboutMonotonicity();
    }
    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        return implementation->getMonotonicityForRange(type, left, right);
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilable() const override
    {
        return implementation->isCompilable();
    }
    ColumnNumbers getArgumentsThatDontParticipateInCompilation(const DataTypes & types) const override
    {
        return implementation->getArgumentsThatDontParticipateInCompilation(types);
    }
    llvm::Value * compile(llvm::IRBuilderBase & builder, const ValuesWithType & arguments) const override
    {
        return implementation->compile(builder, arguments);
    }
#endif

private:
    FunctionBasePtr implementation;
};

class FunctionDivideExact final : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "divideExact";

    explicit FunctionDivideExact(ContextPtr context)
        : legacy(FunctionDivideLegacy::create(context))
        , exact(std::make_unique<FunctionToOverloadResolverAdaptor>(createFunctionExactDivide()))
        , integral(FunctionFactory::instance().get("intDiv", context))
    {
    }

    static FunctionOverloadResolverPtr create(ContextPtr context)
    {
        return std::make_unique<FunctionDivideExact>(std::move(context));
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
        return std::make_shared<FunctionDivideExactBase>(delegate(arguments)->build(arguments));
    }

private:
    const FunctionOverloadResolverPtr & delegate(const ColumnsWithTypeAndName & arguments) const
    {
        const auto left = recursiveRemoveLowCardinality(arguments[0].type);
        const auto right = recursiveRemoveLowCardinality(arguments[1].type);
        const bool constant_unsigned_divisor_fits = isUInt(right) && arguments[1].column
            && isColumnConst(*arguments[1].column)
            && accurateLess((*arguments[1].column)[0], Field(UInt256(1) << (8 * right->getSizeOfValueInMemory() - 1)));
        /// Reuse `intDiv` where its operand casts preserve every input value, including a signed
        /// dividend with a narrower unsigned divisor or a constant that fits its signed cast.
        /// This retains its constant-divisor kernels and monotonicity metadata, under the stable
        /// `divideExact` name, distinct from floating-point `divide`.
        /// Other mixed-sign pairs need checked magnitudes; nullable inputs need to skip masked
        /// overflow and zero-divisor rows.
        if ((isInt(left) && isInt(right)) || (isUInt(left) && isUInt(right))
            || (isInt(left) && isUInt(right)
                && (left->getSizeOfValueInMemory() > right->getSizeOfValueInMemory() || constant_unsigned_divisor_fits)))
        {
            /// `intDiv` disables lazy execution for constant divisors. Keep potentially throwing
            /// constants on the checked path so an enclosing `if` can mask zero and overflow.
            if (arguments[1].column && isColumnConst(*arguments[1].column))
            {
                const auto divisor = (*arguments[1].column)[0];
                if (accurateEquals(divisor, Field(0)) || accurateEquals(divisor, Field(-1)))
                    return exact;
            }
            return integral;
        }
        if (isInteger(removeNullable(left)) && isInteger(removeNullable(right)))
            return exact;
        return legacy;
    }

    FunctionOverloadResolverPtr legacy;
    FunctionOverloadResolverPtr exact;
    FunctionOverloadResolverPtr integral;
};

REGISTER_FUNCTION(Divide)
{
    FunctionDocumentation::Description description = R"(
Calculates the quotient of two values `a` and `b`. The `/` operator is equivalent to this function.
For integer and floating-point arguments, the result type is [Float64](/reference/data-types/float) by default.
With `sql_compatible_division_for_integers = 1`, two integer arguments instead produce an integer quotient truncated toward zero.
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

    factory.registerFunction<FunctionDivideLegacy>(documentation);

    FunctionDocumentation::Description exact_description = R"(
Calculates the quotient of `x` and `y`. When both arguments are integers, the quotient is computed without conversion to floating point and truncated toward zero.
The result has the width of `x` and is signed if either argument is signed. Integer types through 256 bits are supported, including mixed signed and unsigned arguments.
Division by zero or a quotient outside the result type's range throws `ILLEGAL_DIVISION`. Nullable arguments propagate `NULL`.

For floating-point or `Decimal` arguments, the function follows the same rules as `divide` with `sql_compatible_division_for_integers = 0`.

The function's behavior does not depend on `sql_compatible_division_for_integers`.
When that setting is enabled, newly submitted `/` and `divide` expressions are rewritten to `divideExact`.
This explicit name is preserved in stored expressions, such as table metadata and views, so changing the setting later does not change their division semantics.
Existing stored `/` and `divide` expressions retain their historical behavior.
    )";
    FunctionDocumentation::Syntax exact_syntax = "divideExact(x, y)";
    FunctionDocumentation::ReturnedValue exact_returned_value = {"The quotient of `x` and `y`, truncated toward zero when both arguments are integers."};
    FunctionDocumentation::Examples exact_examples =
    {
        {"Integer division", "SELECT divideExact(toUInt64(5), toUInt8(2)) AS quotient, toTypeName(quotient)", "2\tUInt64"},
        {"Mixed signed and unsigned arguments", "SELECT divideExact(toInt32(-5), toUInt64(2)) AS quotient, toTypeName(quotient)", "-2\tInt32"},
        {"Floating-point division", "SELECT divideExact(5.0, 2) AS quotient, toTypeName(quotient)", "2.5\tFloat64"}
    };
    FunctionDocumentation::IntroducedIn exact_introduced_in = {26, 10};
    FunctionDocumentation exact_documentation =
    {
        exact_description, exact_syntax, arguments, {}, exact_returned_value, exact_examples, exact_introduced_in, category};
    factory.registerFunction<FunctionDivideExact>(exact_documentation);
}

struct NameDivideOrNull { static constexpr auto name = "divideOrNull"; };
using FunctionDivideOrNull = BinaryArithmeticOverloadResolver<DivideFloatingOrNullImpl, NameDivideOrNull>;

REGISTER_FUNCTION(DivideOrNull)
{
    FunctionDocumentation::Description description = R"(
Same as `divide` with `sql_compatible_division_for_integers = 0`, but returns `NULL` when dividing by zero.
The `sql_compatible_division_for_integers` setting does not affect this function.
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
