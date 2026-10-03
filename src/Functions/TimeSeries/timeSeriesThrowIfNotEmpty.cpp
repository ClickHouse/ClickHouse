#include <Functions/FunctionFactory.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// Function timeSeriesThrowIfNotEmpty(x, array, message) returns `x` as is if `array` is empty in every row, and throws an
/// exception with `message` otherwise. It does what `if(empty(array), x, throwIf(notEmpty(array), message))` does, but once
/// per block: the arrays of a block are all empty if and only if their nested column is empty. The evaluation of prometheus
/// queries uses it to reject the kind of samples it doesn't support yet, e.g. `timeSeriesThrowIfNotEmpty(value, histogram, ...)`.
class FunctionTimeSeriesThrowIfNotEmpty final : public IFunction
{
public:
    static constexpr auto name = "timeSeriesThrowIfNotEmpty";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesThrowIfNotEmpty>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }

    /// A constant `array` is checked without materializing it.
    bool useDefaultImplementationForConstants() const override { return false; }

    /// `x` is returned as is, so the result type must be exactly its type (see `FunctionIdentityBase`).
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool useDefaultImplementationForVariant() const override { return false; }

    /// Inside `if` it must check only the rows of its branch, like `throwIf`.
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} must be called with three arguments: {}(x, array, message)", name, name);

        if (!isArray(arguments[1].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument of function {} must be an array (passed: {})", name, arguments[1].type->getName());

        if (!isString(arguments[2].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Third argument of function {} must be a constant string (passed: {})", name, arguments[2].type->getName());

        return arguments[0].type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t /* input_rows_count */) const override
    {
        const IColumn * array_column = arguments[1].column.get();
        if (const auto * array_const = checkAndGetColumn<ColumnConst>(array_column))
            array_column = &array_const->getDataColumn();

        const auto * array = checkAndGetColumn<ColumnArray>(array_column);
        if (!array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Second argument of function {} must be an array column (passed: {})", name, array_column->getName());

        if (!array->getData().empty())
        {
            const auto * message_column = checkAndGetColumnConstStringOrFixedString(arguments[2].column.get());
            if (!message_column)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "Third argument of function {} must be a constant string (passed: {})", name, arguments[2].column->getName());
            throw Exception::createRuntime(ErrorCodes::NOT_IMPLEMENTED, String{message_column->getDataAt(0)});
        }

        return arguments[0].column;
    }
};

}

REGISTER_FUNCTION(TimeSeriesThrowIfNotEmpty)
{
    FunctionDocumentation::Description description = R"(
Returns `x` as is if `array` is empty in every row, and throws an exception with `message` otherwise.
The result is the same as of `if(empty(array), x, throwIf(notEmpty(array), message))`,
but the arrays are checked once per block of rows instead of row by row.
The evaluation of prometheus queries uses it to reject the kind of samples returned by
[timeSeriesSelector](/reference/functions/table-functions/timeSeriesSelector) which it doesn't support yet.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesThrowIfNotEmpty(x, array, message)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Value to return.", {"Any"}},
        {"array", "Array which must be empty in every row.", {"Array(T)"}},
        {"message", "Message of the exception thrown if any `array` is not empty.", {"const String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `x`.", {"Any"}};
    FunctionDocumentation::Examples examples = {
    {
        "Empty arrays",
        "SELECT timeSeriesThrowIfNotEmpty(number * 0.5, [], 'histogram samples are not supported') AS value FROM numbers(3)",
        R"(
┌─value─┐
│     0 │
│   0.5 │
│     1 │
└───────┘
        )",
    },
    {
        "A non-empty array",
        "SELECT timeSeriesThrowIfNotEmpty(0, [(1, 2)], 'histogram samples are not supported')",
        R"(
Received exception:
Code: 48. DB::Exception: histogram samples are not supported. (NOT_IMPLEMENTED)
        )",
    }};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesThrowIfNotEmpty>(documentation);
}

}
