#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>

#include <IO/WriteHelpers.h>
#include <Common/assert_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// Just changes time zone information for data type. The calculation is free.
class FunctionToTimezone : public IFunction
{
public:
    static constexpr auto name = "toTimezone";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionToTimezone>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto which_type = WhichDataType(arguments[0].type);
        if (!which_type.isDateTime() && !which_type.isDateTime64())
            throw Exception{"Illegal type " + arguments[0].type->getName() + " of argument of function " + getName() +
                ". Should be DateTime or DateTime64", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        String time_zone_name = extractTimeZoneNameFromFunctionArguments(arguments, 1, 0);
        if (which_type.isDateTime())
            return std::make_shared<DataTypeDateTime>(time_zone_name);

        const auto * date_time64 = assert_cast<const DataTypeDateTime64 *>(arguments[0].type.get());
        return std::make_shared<DataTypeDateTime64>(date_time64->getScale(), time_zone_name);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        return arguments[0].column;
    }
};

}

void registerFunctionToTimeZone(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToTimezone>();
    factory.registerAlias("toTimeZone", "toTimezone");
}

}
