#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

/** ignore(...) is a function that takes any arguments, and always returns 0.
  */
class FunctionIgnore : public IFunction
{
public:
    static constexpr auto name = "ignore";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionIgnore>();
    }

    bool isVariadic() const override
    {
        return true;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    String getName() const override
    {
        return name;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeUInt8().createColumnConst(input_rows_count, 0u);
    }

    ColumnPtr getResultIfAlwaysReturnsConstantAndHasArguments(const Block &, const ColumnNumbers &) const override
    {
        return DataTypeUInt8().createColumnConst(1, 0u);
    }
};


void registerFunctionIgnore(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIgnore>();
}

}
