#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

/** materialize(x) - materialize the constant
  */
class FunctionMaterialize : public IFunction
{
public:
    static constexpr auto name = "materialize";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionMaterialize>();
    }

    bool useDefaultImplementationForNulls() const override
    {
        return false;
    }

    /// Get the function name.
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return arguments[0];
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
    {
        block.getByPosition(result).column = block.getByPosition(arguments[0]).column->convertToFullColumnIfConst();
    }
};


void registerFunctionMaterialize(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMaterialize>();
}

}
