#include <common/DateLUT.h>

#include <DataTypes/DataTypeDate.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

class FunctionYesterday : public IFunction
{
public:
    static constexpr auto name = "yesterday";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionYesterday>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeDate>();
    }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeDate().createColumnConst(
            input_rows_count,
            UInt64(DateLUT::instance().toDayNum(time(nullptr)) - 1));
    }
};

void registerFunctionYesterday(FunctionFactory & factory)
{
    factory.registerFunction<FunctionYesterday>();
}

}
