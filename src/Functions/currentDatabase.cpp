#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Field.h>


namespace DB
{
namespace
{

class FunctionCurrentDatabase : public IFunction
{
    const String db_name;

public:
    static constexpr auto name = "currentDatabase";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionCurrentDatabase>(context->getCurrentDatabase());
    }

    explicit FunctionCurrentDatabase(const String & db_name_) : db_name{db_name_}
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

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeString().createColumnConst(input_rows_count, db_name);
    }
};

}

void registerFunctionCurrentDatabase(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCurrentDatabase>();
    factory.registerFunction<FunctionCurrentDatabase>("DATABASE", FunctionFactory::CaseInsensitive);
}

}
