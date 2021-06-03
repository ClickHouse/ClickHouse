#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{
namespace
{

class FunctionRowNumberInBlock : public IFunction
{
public:
    static constexpr auto name = "rowNumberInBlock";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionRowNumberInBlock>();
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

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto column = ColumnUInt64::create();
        auto & data = column->getData();
        data.resize(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i)
            data[i] = i;

        return column;
    }
};

}

void registerFunctionRowNumberInBlock(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRowNumberInBlock>();
}

}
