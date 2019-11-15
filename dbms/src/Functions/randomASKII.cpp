#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Field.h>


namespace DB
{

class FunctionRandomASKII : public IFunction
{
    const String db_name;

public:
    static constexpr auto name = "randomASKII";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionRandomASKII>(context.getRandomASKII());
    }

    explicit FunctionRandomASKII(const String & db_name_) : db_name{db_name_}
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
        block.getByPosition(result).column = DataTypeString().createColumnConst(input_rows_count, db_name);
    }
};


void registerFunctionRandomASKII(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRandomASKII>();
}

}
