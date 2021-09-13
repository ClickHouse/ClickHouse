#include <Functions/FunctionServerConstantBase.h>
#include <DataTypes/DataTypeString.h>
#include <common/DateLUT.h>


namespace DB
{
namespace
{
    constexpr char name[] = "timezone";

    /// Returns the server time zone.
    class FunctionTimezone : public FunctionServerConstantBase<String, DataTypeString, name>
    {
    public:
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTimezone>(context); }

        explicit FunctionTimezone(ContextPtr context) : FunctionServerConstantBase(context, String{DateLUT::instance().getTimeZone()}) {}
    };
}

void registerFunctionTimezone(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTimezone>();
    factory.registerAlias("timeZone", "timezone");
}

}
