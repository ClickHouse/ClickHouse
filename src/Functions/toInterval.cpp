#include <Functions/IFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeInterval.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionToInterval : public IFunction
{
public:
    static constexpr auto name = "toInterval";

    explicit FunctionToInterval(ContextPtr context_) : context(context_) {}

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionToInterval>(context); }

    String getName() const override { return name; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool hasInformationAboutMonotonicity() const override { return true; }

    Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
    {
        return { .is_monotonic = true, .is_always_monotonic = true };
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be 2 arguments", getName());

        /// The second argument is a constant string with the name of interval kind.
        String interval_kind;
        const ColumnConst * kind_column = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());
        if (!kind_column)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument for function {} must be constant string: "
                "unit of interval", getName());

        interval_kind = Poco::toLower(kind_column->getValue<String>());
        if (interval_kind.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second argument (unit) for function {} cannot be empty", getName());

        if (!IntervalKind::tryParseString(interval_kind, kind.kind))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} doesn't look like an interval unit in {}", interval_kind, getName());

        return std::make_shared<DataTypeInterval>(kind);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        ColumnsWithTypeAndName temp_columns(1);
        temp_columns[0] = arguments[0];

        const char * to_interval_function_name = kind.toNameOfFunctionToIntervalDataType();
        auto to_interval_function = FunctionFactory::instance().get(to_interval_function_name, context);

        return to_interval_function->build(temp_columns)->execute(temp_columns, result_type, input_rows_count, /* dry run = */ false);
    }

private:
    ContextPtr context;
    mutable IntervalKind kind = IntervalKind::Kind::Second;
};

REGISTER_FUNCTION(ToInterval)
{
    factory.registerFunction<FunctionToInterval>(
        FunctionDocumentation{.description = R"(Creates an interval from a value and a unit.)"});
}

}
