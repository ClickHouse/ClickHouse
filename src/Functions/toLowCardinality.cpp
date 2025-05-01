#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnLowCardinality.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace
{

class FunctionToLowCardinality: public IFunction
{
public:
    static constexpr auto name = "toLowCardinality";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionToLowCardinality>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->lowCardinality())
            return arguments[0];

        return std::make_shared<DataTypeLowCardinality>(arguments[0]);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & res_type, size_t /*input_rows_count*/) const override
    {
        const auto & arg = arguments[0];

        if (arg.type->lowCardinality())
            return arg.column;

        auto column = res_type->createColumn();
        typeid_cast<ColumnLowCardinality &>(*column).insertRangeFromFullColumn(*arg.column, 0, arg.column->size());
        return column;
    }
};

}

REGISTER_FUNCTION(ToLowCardinality)
{
    factory.registerFunction<FunctionToLowCardinality>();
}

}
