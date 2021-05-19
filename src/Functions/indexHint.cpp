#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{


/** The `indexHint` function takes any number of any arguments and always returns one.
  *
  * This function has a special meaning (see ExpressionAnalyzer, KeyCondition)
  * - the expressions inside it are not evaluated;
  * - but when analyzing the index (selecting ranges for reading), this function is treated the same way,
  *   as if instead of using it the expression itself would be.
  *
  * Example: WHERE something AND indexHint(CounterID = 34)
  * - do not read or calculate CounterID = 34, but select ranges in which the CounterID = 34 expression can be true.
  *
  * The function can be used for debugging purposes, as well as for (hidden from the user) query conversions.
  */
class FunctionIndexHint : public IFunction
{
public:
    static constexpr auto name = "indexHint";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionIndexHint>();
    }

    bool isVariadic() const override
    {
        return true;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool isSuitableForConstantFolding() const override { return false; }

    String getName() const override
    {
        return name;
    }
    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeUInt8().createColumnConst(input_rows_count, 1u);
    }

    ColumnPtr getResultIfAlwaysReturnsConstantAndHasArguments(const ColumnsWithTypeAndName &) const override
    {
        return DataTypeUInt8().createColumnConst(1, 1u);
    }
};


void registerFunctionIndexHint(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIndexHint>();
}

}
