#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>


namespace DB
{

/** Returns current database name.
  */
class FunctionDatabase : public IFunction
{
public:
    static constexpr auto name = "database";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionDatabase>(context.getCurrentDatabase());
    }

    explicit FunctionDatabase(std::string database_) : database{database_}
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

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeString().createColumnConst(input_rows_count, database);
    }

private:
    std::string database;
};


void registerFunctionDatabase(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDatabase>();
}

}
