#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <common/DateLUT.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeString.h>


namespace DB
{
namespace
{

/** Returns the server time zone.
  */
class FunctionTimezone : public IFunction
{
public:
    static constexpr auto name = "timezone";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionTimezone>();
    }

    String getName() const override
    {
        return name;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool isDeterministic() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeString().createColumnConst(input_rows_count, DateLUT::instance().getTimeZone());
    }
};

}

void registerFunctionTimezone(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTimezone>();
    factory.registerAlias("timeZone", "timezone");
}

}
