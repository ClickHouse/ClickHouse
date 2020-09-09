#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <atomic>


namespace DB
{

/** Incremental block number among calls of this function. */
class FunctionBlockNumber : public IFunction
{
private:
    mutable std::atomic<size_t> block_number{0};

public:
    static constexpr auto name = "blockNumber";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionBlockNumber>();
    }

    /// Get the function name.
    String getName() const override
    {
        return name;
    }

    bool isStateful() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) const override
    {
        size_t current_block_number = block_number++;
        block.getByPosition(result).column = ColumnUInt64::create(input_rows_count, current_block_number);
    }
};


void registerFunctionBlockNumber(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBlockNumber>();
}

}
