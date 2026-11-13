#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnLowCardinality.h>
#include <Common/typeid_cast.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

class FunctionToLowCardinality final : public IFunction
{
public:
    static constexpr auto name = "toLowCardinality";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionToLowCardinality>(); }

    [[nodiscard]] String getName() const override { return name; }

    [[nodiscard]] size_t getNumberOfArguments() const override { return 1; }

    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return false; }
    [[nodiscard]] bool isNullPropagating(const DataTypePtr & result_type) const override { return isNullableOrLowCardinalityNullable(result_type); }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    [[nodiscard]] bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    [[nodiscard]] String getSignatureString() const override
    {
        return "(T) -> LowCardinality(T)";
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & res_type, size_t /*input_rows_count*/) const override
    {
        const auto & arg = arguments[0];

        if (arg.type->lowCardinality())
            return arg.column;

        if (!res_type->lowCardinality())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected LowCardinality type as result type for toLowCardinality function, got: {}", res_type->getName());

        auto column = res_type->createColumn();
        typeid_cast<ColumnLowCardinality &>(*column).insertRangeFromFullColumn(*arg.column, 0, arg.column->size());
        return column;
    }

    [[nodiscard]] bool hasInformationAboutMonotonicity() const override { return true; }

    [[nodiscard]] Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
    {
        return { .is_monotonic = true, .is_positive = true, .is_always_monotonic = true, .is_strict = true };
    }
};

}

REGISTER_FUNCTION(ToLowCardinality)
{
    /// toLowCardinality documentation
    FunctionDocumentation::Description description = R"(
Converts the input argument to the [LowCardinality](/reference/data-types/lowcardinality) version of same data type.

<Tip>
To convert from the `LowCardinality` data type to a regular data type, use the [CAST](#CAST) function.
For example: `CAST(x AS String)`.
</Tip>
    )";
    FunctionDocumentation::Syntax syntax = "toLowCardinality(expr)";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Expression resulting in one of the supported data types.", {"String", "FixedString", "Date", "DateTime", "(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the input value converted to the `LowCardinality` data type.", {"LowCardinality"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT toLowCardinality('1')
        )",
        R"(
┌─toLowCardinality('1')─┐
│ 1                     │
└───────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {18, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToLowCardinality>(documentation);
}

}
