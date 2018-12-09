#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnLowCardinality.h>
#include <Common/typeid_cast.h>


namespace DB
{

class FunctionToLowCardinality: public IFunction
{
public:
    static constexpr auto name = "toLowCardinality";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionToLowCardinality>(); }

    String getName() const override { return name; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    String getSignature() const override { return "f(T) -> LowCardinality(T)"; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        auto arg_num = arguments[0];
        const auto & arg = block.getByPosition(arg_num);
        auto & res = block.getByPosition(result);

        if (arg.type->lowCardinality())
            res.column = arg.column;
        else
        {
            auto column = res.type->createColumn();
            typeid_cast<ColumnLowCardinality &>(*column).insertRangeFromFullColumn(*arg.column, 0, arg.column->size());
            res.column = std::move(column);
        }
    }
};


void registerFunctionToLowCardinality(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToLowCardinality>();
}

}
