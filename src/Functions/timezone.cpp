#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <common/DateLUT.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>


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
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionTimezone>(context);
    }

    explicit FunctionTimezone(ContextPtr context_) : context(context_)
    {
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
    bool isDeterministicInScopeOfQuery() const override { return true; }
    bool isSuitableForConstantFolding() const override { return !context->isDistributed(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeString().createColumnConst(input_rows_count, DateLUT::instance().getTimeZone());
    }
private:
    ContextPtr context;
};

}

void registerFunctionTimezone(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTimezone>();
    factory.registerAlias("timeZone", "timezone");
}

}
