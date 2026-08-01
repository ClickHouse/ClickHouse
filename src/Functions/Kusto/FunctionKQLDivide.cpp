#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/** `kqlDivide(x, y)` - the `/` operator of the Kusto dialect.
  *
  * KQL divides two integers as integers: `7 / 2` is 3, not 3.5. Which rule applies depends on
  * the operand *types*, which a parser translating text to text cannot see - the previous KQL
  * implementation emitted a plain `divide` and silently answered 3.5.
  *
  * This is an overload *resolver* rather than a function: the choice depends only on the
  * argument types, so it is made once while the query is analysed, and the chosen function
  * then runs with no further dispatch.
  */
class FunctionKQLDivideOverloadResolver final : public IFunctionOverloadResolver, WithContext
{
public:
    static constexpr auto name = "kqlDivide";

    explicit FunctionKQLDivideOverloadResolver(ContextPtr context_) : WithContext(context_) { }

    static FunctionOverloadResolverPtr create(ContextPtr context_)
    {
        return std::make_unique<FunctionKQLDivideOverloadResolver>(std::move(context_));
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForNulls() const override { return false; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override
    {
        return delegate(arguments);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return delegate(arguments)->getResultType();
    }

private:
    /// Integer over integer keeps the KQL rule; anything else divides as usual.
    FunctionBasePtr delegate(const ColumnsWithTypeAndName & arguments) const
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly 2 arguments", getName());

        const bool both_integral
            = isInteger(removeNullable(arguments[0].type)) && isInteger(removeNullable(arguments[1].type));
        return FunctionFactory::instance().get(both_integral ? "intDiv" : "divide", getContext())->build(arguments);
    }
};

}

REGISTER_FUNCTION(KQLDivide)
{
    FunctionDocumentation documentation{
        .description = R"(
Division as the Kusto Query Language defines it: two integer operands divide to an integer,
so `7 / 2` is `3`. Any other combination of operand types divides as [`divide`](#divide) does.

This function backs the `/` operator when `dialect = 'kusto'`. It is not meant to be called
directly from SQL.
)",
        .syntax = "kqlDivide(x, y)",
        .arguments = {{"x", "The dividend."}, {"y", "The divisor."}},
        .returned_value = {"`intDiv(x, y)` when both arguments are integers, `divide(x, y)` otherwise."},
        .examples = {{"integers", "SELECT kqlDivide(7, 2)", "3"}, {"reals", "SELECT kqlDivide(7.0, 2)", "3.5"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Arithmetic,
    };

    factory.registerFunction(
        FunctionKQLDivideOverloadResolver::name,
        [](ContextPtr context) { return FunctionKQLDivideOverloadResolver::create(std::move(context)); },
        documentation);
}

}
