#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Common/config_version.h>


namespace DB
{

/** version() - returns the current version as a string.
  */
class FunctionVersion : public IFunction
{
public:
    static constexpr auto name = "version";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionVersion>();
    }

    String getName() const override
    {
        return name;
    }

    String getSignature() const override { return "f() -> String"; }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeString().createColumnConst(input_rows_count, VERSION_STRING);
    }
};


void registerFunctionVersion(FunctionFactory & factory)
{
    factory.registerFunction<FunctionVersion>();
}

}
