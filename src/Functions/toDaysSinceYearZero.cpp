#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** Returns number of days passed since 0000-01-01 */
class FunctionToDaysSinceYearZero : public IFunction
{
    using ResultType = DataTypeUInt32;
public:
    static constexpr auto name = "toDaysSinceYearZero";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionToDaysSinceYearZero>(context); }

    explicit FunctionToDaysSinceYearZero(ContextPtr /*context*/) {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"date", &isDateOrDate32<IDataType>, nullptr, "Date or Date32"}
        };

        validateFunctionArgumentTypes(*this, arguments, mandatory_args);

        return std::make_shared<DataTypeUInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);

        if (which.isDate())
            return DateTimeTransformImpl<DataTypeDate, ResultType, ToDaysSinceYearZeroImpl>::execute(arguments, result_type, input_rows_count);
        else if (which.isDate32())
            return DateTimeTransformImpl<DataTypeDate32, ResultType, ToDaysSinceYearZeroImpl>::execute(arguments, result_type, input_rows_count);

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument of function {}",
            arguments[0].type->getName(), this->getName());
    }
};

}

REGISTER_FUNCTION(ToDaysSinceYearZero)
{
    factory.registerFunction<FunctionToDaysSinceYearZero>(
    FunctionDocumentation{
    .description=R"(
Returns for a given date, the number of days passed since 1 January 0000 in the proleptic Gregorian calendar defined by ISO 8601.
The calculation is the same as in MySQL's TO_DAYS() function.
)",
    .examples{
        {"typical", "SELECT toDaysSinceYearZero(toDate('2023-09-08'))", "713569"}},
    .categories{"Dates and Times"}
    });

    /// MySQL compatibility alias.
    factory.registerAlias("TO_DAYS", FunctionToDaysSinceYearZero::name, FunctionFactory::CaseInsensitive);
}

}
