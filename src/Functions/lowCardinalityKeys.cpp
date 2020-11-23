#include <Functions/IFunctionImpl.h>
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

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * type = typeid_cast<const DataTypeLowCardinality *>(arguments[0].get());
        if (!type)
            throw Exception("First first argument of function lowCardinalityKeys must be ColumnLowCardinality, but got"
                            + arguments[0]->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return type->getDictionaryType();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
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
