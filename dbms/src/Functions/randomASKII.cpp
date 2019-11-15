#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Field.h>


namespace DB
{

class FunctionRandomASKII : public IFunction
{

public:
    static constexpr auto name = "randomASKII";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionRandomASKII>();
    }

    explicit FunctionRandomASKII()
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

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeString().createColumnConst(input_rows_count, "randomASKII");
    }
};


void registerFunctionRandomASKII(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRandomASKII>();
}

}
