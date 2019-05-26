#include <DataTypes/DataTypeDateTime.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>

#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// Just changes time zone information for data type. The calculation is free.
class FunctionToTimeZone : public IFunction
{
public:
    static constexpr auto name = "toTimeZone";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionToTimeZone>(); }

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

        if (!WhichDataType(arguments[0].type).isDateTime())
            throw Exception{"Illegal type " + arguments[0].type->getName() + " of argument of function " + getName() +
                ". Should be DateTime", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        String time_zone_name = extractTimeZoneNameFromFunctionArguments(arguments, 1, 0);
        return std::make_shared<DataTypeDateTime>(time_zone_name);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        block.getByPosition(result).column = block.getByPosition(arguments[0]).column;
    }
};

void registerFunctionToTimeZone(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToTimeZone>();
}

}
