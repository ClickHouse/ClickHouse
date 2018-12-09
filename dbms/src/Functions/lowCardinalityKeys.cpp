#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnLowCardinality.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


class FunctionLowCardinalityKeys: public IFunction
{
public:
    static constexpr auto name = "lowCardinalityKeys";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionLowCardinalityKeys>(); }

    String getName() const override { return name; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    String getSignature() const override { return "f(LowCardinality(T)) -> T"; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        auto arg_num = arguments[0];
        const auto & arg = block.getByPosition(arg_num);
        auto & res = block.getByPosition(result);
        const auto * low_cardinality_column = typeid_cast<const ColumnLowCardinality *>(arg.column.get());
        res.column = low_cardinality_column->getDictionary().getNestedColumn()->cloneResized(arg.column->size());
    }
};


void registerFunctionLowCardinalityKeys(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLowCardinalityKeys>();
}

}
