#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** finalizeAggregation(agg_state) - get the result from the aggregation state.
  * Takes state of aggregate function. Returns result of aggregation (finalized state).
  */
class FunctionFinalizeAggregation : public IFunction
{
public:
    static constexpr auto name = "finalizeAggregation";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionFinalizeAggregation>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeAggregateFunction * type = checkAndGetDataType<DataTypeAggregateFunction>(arguments[0].get());
        if (!type)
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument for function '{}' must have type AggregateFunction - state of aggregate function."
                " Got '{}' instead", getName(), arguments[0]->getName());
        }

        return type->getReturnType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        auto column = arguments.at(0).column;
        if (!typeid_cast<const ColumnAggregateFunction *>(column.get()))
            throw Exception("Illegal column " + arguments.at(0).column->getName()
                    + " of first argument of function "
                    + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        /// Column is copied here, because there is no guarantee that we own it.
        auto mut_column = IColumn::mutate(std::move(column));
        return ColumnAggregateFunction::convertToValues(std::move(mut_column));
    }
};

}

void registerFunctionFinalizeAggregation(FunctionFactory & factory)
{
    factory.registerFunction<FunctionFinalizeAggregation>();
}

}
