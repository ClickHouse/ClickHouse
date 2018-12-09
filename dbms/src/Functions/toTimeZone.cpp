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

    String getName() const override { return name; }
    String getSignature() const override { return "f(DateTime, const timezone String) -> DateTime(timezone)"; }

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
