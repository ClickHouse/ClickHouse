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

class FunctionMakeInterval : public IFunction
{
public:
    static constexpr auto name = "makeInterval";

    explicit FunctionMakeInterval(ContextPtr context_) : context(context_) {}

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionMakeInterval>(context); }

    String getName() const override { return name; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    bool hasInformationAboutMonotonicity() const override { return true; }

    Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
    {
        return { .is_monotonic = true, .is_always_monotonic = true };
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be 2 arguments", getName());

        /// The first argument is a constant string with the name of interval kind.
        String interval_kind;
        const ColumnConst * datepart_column = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
        if (!datepart_column)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be constant string: "
                "name of datepart", getName());

        interval_kind = Poco::toLower(datepart_column->getValue<String>());
        if (interval_kind.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "First argument (name of datepart) for function {} cannot be empty", getName());
        
        if (!IntervalKind::tryParseString(interval_kind, kind.kind))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} doesn't look like datepart name in {}", interval_kind, getName());

        /// The second argument is an integer representing the length of the interval
        if (!isInteger(arguments[1].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of 2nd argument of function {}. "
                "Should be an integer", arguments[1].type->getName(), getName());

        return std::make_shared<DataTypeInterval>(kind);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        ColumnsWithTypeAndName temp_columns(1);
        temp_columns[0] = arguments[1];

        const char * to_interval_function_name = kind.toNameOfFunctionToIntervalDataType();
        auto to_interval_function = FunctionFactory::instance().get(to_interval_function_name, context);

        return to_interval_function->build(temp_columns)->execute(temp_columns, result_type, input_rows_count, /* dry run = */ false);
    }

private:
    ContextPtr context;
    mutable IntervalKind kind = IntervalKind::Kind::Second;
};

REGISTER_FUNCTION(MakeInterval)
{
    factory.registerFunction<FunctionMakeInterval>(
        FunctionDocumentation{.description = R"(Creates an interval value from an interval type and length.)"});
}

}
