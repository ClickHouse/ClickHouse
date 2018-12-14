#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>


namespace DB
{

class FunctionCurrentDatabase : public IFunction
{
    const String db_name;

public:
    static constexpr auto name = "currentDatabase";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionCurrentDatabase>(context.getCurrentDatabase());
    }

    explicit FunctionCurrentDatabase(const String & db_name) : db_name{db_name}
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


void registerFunctionCurrentDatabase(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCurrentDatabase>();
}

}
