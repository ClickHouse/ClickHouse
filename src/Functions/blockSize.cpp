#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

/** blockSize() - get the block size in number of rows.
  */
class FunctionBlockSize : public IFunction
{
public:
    static constexpr auto name = "blockSize";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionBlockSize>();
    }

    /// Get the function name.
    String getName() const override
    {
        return name;
    }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) const override
    {
        block.getByPosition(result).column = ColumnUInt64::create(input_rows_count, input_rows_count);
    }
};


void registerFunctionBlockSize(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBlockSize>();
}

}
