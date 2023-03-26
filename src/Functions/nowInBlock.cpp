#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** Returns current time at calculation of every block.
  * In contrast to 'now' function, it's not a constant expression and is not a subject of constant folding.
  */
class FunctionNowInBlock : public IFunction
{
public:
    static constexpr auto name = "nowInBlock";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionNowInBlock>();
    }

    String getName() const override
    {
        return name;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return false;
    }

    /// Optional timezone argument.
    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isDeterministic() const override
    {
        return false;
    }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() > 1)
        {
            throw Exception("Arguments size of function " + getName() + " should be 0 or 1", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
        if (arguments.size() == 1 && !isStringOrFixedString(arguments[0].type))
        {
            throw Exception(
                "Arguments of function " + getName() + " should be String or FixedString", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        if (arguments.size() == 1)
        {
            return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 0, 0));
        }
        return std::make_shared<DataTypeDateTime>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return ColumnUInt32::create(input_rows_count, time(nullptr));
    }
};

}

REGISTER_FUNCTION(NowInBlock)
{
    factory.registerFunction<FunctionNowInBlock>();
}

}
