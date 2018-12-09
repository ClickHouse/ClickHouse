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

    String getSignature() const override { return "f() -> Date"; }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeDate().createColumnConst(
            input_rows_count,
            DateLUT::instance().toDayNum(time(nullptr)) - 1);
    }
};

void registerFunctionYesterday(FunctionFactory & factory)
{
    factory.registerFunction<FunctionYesterday>();
}

}
