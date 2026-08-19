#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/array/has.h>

namespace DB
{

namespace
{

/// notHas(haystack, needle) - the negation of `has`, computed as not(has(haystack, needle)).
///
/// When the haystack is a constant array, `RewriteHasToInPass` (`optimize_rewrite_has_to_in`)
/// rewrites `notHas(constant_array, x)` to `notIn(x, constant_array)`, which executes through the
/// set machinery and can prune by the primary key the same way `NOT IN` does.
class FunctionNotHas : public IFunction
{
public:
    static constexpr auto name = "notHas";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionNotHas>(FunctionFactory::instance().get("not", context));
    }

    explicit FunctionNotHas(FunctionOverloadResolverPtr not_function_resolver_)
        : not_function_resolver(std::move(not_function_resolver_))
    {
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto has_function = createInternalFunctionHasOverloadResolver()->build(arguments);
        ColumnsWithTypeAndName not_arguments{{nullptr, has_function->getResultType(), ""}};
        return not_function_resolver->build(not_arguments)->getResultType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto has_function = createInternalFunctionHasOverloadResolver()->build(arguments);
        auto has_result = has_function->execute(arguments, has_function->getResultType(), input_rows_count, /*dry_run*/ false);

        ColumnsWithTypeAndName not_arguments{{has_result, has_function->getResultType(), ""}};
        auto not_function = not_function_resolver->build(not_arguments);
        return not_function->execute(not_arguments, not_function->getResultType(), input_rows_count, /*dry_run*/ false);
    }

private:
    FunctionOverloadResolverPtr not_function_resolver;
};

}

REGISTER_FUNCTION(NotHas)
{
    FunctionDocumentation::Description description
        = "Returns whether the array does not contain the specified element, the map does not contain the specified key, "
          "or the JSON object does not contain the specified path. The negation of `has`.\n\n";

    FunctionDocumentation::Syntax syntax = "notHas(haystack, needle)";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "The source array, map, or JSON.", {"Array", "Map", "JSON"}},
        {"needle", "The value to search for (element in array, key in map, or path string in JSON)."}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if the haystack does not contain the specified needle, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
        {"Array basic usage", "SELECT notHas([1, 2, 3], 2)", "0"},
        {"Array not found", "SELECT notHas([1, 2, 3], 4)", "1"},
        {"Map basic usage", "SELECT notHas(map('a', 1, 'b', 2), 'c')", "1"},
        {"JSON basic usage", R"(SELECT notHas('{"a" : 1, "b" : {"c" : 2}}'::JSON, 'b.c'))", "0"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionNotHas>(documentation);
}

}
