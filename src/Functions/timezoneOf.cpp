#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Common/DateLUTImpl.h>
#include <Core/Field.h>

namespace DB
{

namespace
{


/** timezoneOf(x) - get the name of the timezone of DateTime data type.
  * Example: Pacific/Pitcairn.
  */
class FunctionTimezoneOf : public IFunction
{
public:
    static constexpr auto name = "timezoneOf";
    String getName() const override { return name; }
    static FunctionPtr create(ContextPtr) { return std::make_unique<FunctionTimezoneOf>(); }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & types) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"datetime", &isDateTimeOrDateTime64, nullptr, "DateTime or DateTime64"},
        };

        validateFunctionArguments(*this, types, mandatory_args);

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        DataTypePtr type_no_nullable = removeNullable(arguments[0].type);

        return DataTypeString().createColumnConst(input_rows_count,
            dynamic_cast<const TimezoneMixin &>(*type_no_nullable).getTimeZone().getTimeZone());
    }

    ColumnPtr getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override
    {
        DataTypePtr type_no_nullable = removeNullable(arguments[0].type);

        return DataTypeString().createColumnConst(1,
            dynamic_cast<const TimezoneMixin &>(*type_no_nullable).getTimeZone().getTimeZone());
    }

private:
    static bool isDateTimeOrDateTime64(const IDataType & type)
    {
        DataTypePtr type_no_nullable = removeNullable(type.getPtr());

        return WhichDataType(*type_no_nullable).isDateTimeOrDateTime64();
    }
};

}

REGISTER_FUNCTION(TimezoneOf)
{
    factory.registerFunction<FunctionTimezoneOf>();
    factory.registerAlias("timeZoneOf", "timezoneOf");
}

}
