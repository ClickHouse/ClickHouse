#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

/// Returns 1 if and only if the argument is constant expression.
/// This function exists for development, debugging and demonstration purposes.
class FunctionIsConstant : public IFunction
{
public:
    static constexpr auto name = "isConstant";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionIsConstant>();
    }

    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        const auto & elem = block.getByPosition(arguments[0]);
        block.getByPosition(result).column = ColumnUInt8::create(input_rows_count, isColumnConst(*elem.column));
    }
};


void registerFunctionIsConstant(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIsConstant>();
}

}

