#include <Functions/IFunctionImpl.h>
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
class FunctionTimeZone : public IFunction
{
public:
    static constexpr auto name = "timezone";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionTimeZone>();
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

void registerFunctionTimeZone(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTimeZone>();
}

}
