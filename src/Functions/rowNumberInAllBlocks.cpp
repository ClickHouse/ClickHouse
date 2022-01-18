#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <atomic>


namespace DB
{
namespace
{

/** Incremental number of row within all columnss passed to this function. */
class FunctionRowNumberInAllBlocks : public IFunction
{
private:
    mutable std::atomic<size_t> rows{0};

public:
    static constexpr auto name = "rowNumberInAllBlocks";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionRowNumberInAllBlocks>();
    }

    /// Get the name of the function.
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

    ColumnPtr executeImplDryRun(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return ColumnUInt64::create(input_rows_count);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        size_t current_row_number = rows.fetch_add(input_rows_count);

        auto column = ColumnUInt64::create();
        auto & data = column->getData();
        data.resize(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i)
            data[i] = current_row_number + i;

        return column;
    }
};

}

void registerFunctionRowNumberInAllBlocks(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRowNumberInAllBlocks>();
}

}
