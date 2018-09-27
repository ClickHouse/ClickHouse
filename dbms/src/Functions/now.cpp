#include <DataTypes/DataTypeDateTime.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

/// Get the current time. (It is a constant, it is evaluated once for the entire query.)
class FunctionNow : public IFunction
{
public:
    static constexpr auto name = "now";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionNow>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeDateTime>();
    }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeDateTime().createColumnConst(
            input_rows_count,
            static_cast<UInt64>(time(nullptr)));
    }
};

void registerFunctionNow(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNow>(FunctionFactory::CaseInsensitive);
}

}
