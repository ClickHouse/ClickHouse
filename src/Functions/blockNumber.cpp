#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <atomic>


namespace DB
{
namespace
{

/** Incremental columns number among calls of this function. */
class FunctionBlockNumber : public IFunction
{
private:
    mutable std::atomic<size_t> columns_number{0};

public:
    static constexpr auto name = "blockNumber";
    static FunctionPtr create(ContextPtr)
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

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return false;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isDeterministic() const override
    {
        return false;
    }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        size_t current_columns_number = columns_number++;
        return ColumnUInt64::create(input_rows_count, current_columns_number);
    }
};

}

void registerFunctionBlockNumber(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBlockNumber>();
}

}
