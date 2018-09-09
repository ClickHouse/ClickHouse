#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>


namespace DB
{

/** toTypeName(x) - get the type name
  * Returns name of IDataType instance (name of data type).
  */
class FunctionToTypeName : public IFunction
{
public:
    static constexpr auto name = "toTypeName";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionToTypeName>();
    }

    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForColumnsWithDictionary() const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    /// Execute the function on the block.
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column
            = DataTypeString().createColumnConst(input_rows_count, block.getByPosition(arguments[0]).type->getName());
    }
};


void registerFunctionToTypeName(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToTypeName>();
}

}
