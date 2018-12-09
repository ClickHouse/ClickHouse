#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <common/DateLUT.h>
#include <DataTypes/DataTypeString.h>


namespace DB
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

    String getSignature() const override { return "f() -> String"; }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeString().createColumnConst(input_rows_count, DateLUT::instance().getTimeZone());
    }
};


void registerFunctionTimeZone(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTimeZone>();
}

}
