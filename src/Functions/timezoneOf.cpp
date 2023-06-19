#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Common/DateLUTImpl.h>
#include <Core/Field.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


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

    DataTypePtr getReturnTypeImpl(const DataTypes & types) const override
    {
        DataTypePtr type_no_nullable = removeNullable(types[0]);

        if (isDateTime(type_no_nullable) || isDateTime64(type_no_nullable))
            return std::make_shared<DataTypeString>();
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad argument for function {}, should be DateTime or DateTime64", name);
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
};

}

REGISTER_FUNCTION(TimezoneOf)
{
    factory.registerFunction<FunctionTimezoneOf>();
    factory.registerAlias("timeZoneOf", "timezoneOf");
}

}
