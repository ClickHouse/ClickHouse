#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

namespace
{

template <typename DataType>
class FunctionEmptyArray : public IFunction
{
public:
    static String getNameImpl() { return "emptyArray" + DataType().getName(); }
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionEmptyArray>(); }

private:
    String getName() const override
    {
        return getNameImpl();
    }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataType>());
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return ColumnArray::create(
            DataType().createColumn(),
            ColumnArray::ColumnOffsets::create(input_rows_count, 0));
    }
};

template <typename F>
void registerFunction(FunctionFactory & factory)
{
    factory.registerFunction<F>(F::getNameImpl());
}

}

void registerFunctionsEmptyArray(FunctionFactory & factory)
{
    registerFunction<FunctionEmptyArray<DataTypeUInt8>>(factory);
    registerFunction<FunctionEmptyArray<DataTypeUInt16>>(factory);
    registerFunction<FunctionEmptyArray<DataTypeUInt32>>(factory);
    registerFunction<FunctionEmptyArray<DataTypeUInt64>>(factory);
    registerFunction<FunctionEmptyArray<DataTypeInt8>>(factory);
    registerFunction<FunctionEmptyArray<DataTypeInt16>>(factory);
    registerFunction<FunctionEmptyArray<DataTypeInt32>>(factory);
    registerFunction<FunctionEmptyArray<DataTypeInt64>>(factory);
    registerFunction<FunctionEmptyArray<DataTypeFloat32>>(factory);
    registerFunction<FunctionEmptyArray<DataTypeFloat64>>(factory);
    registerFunction<FunctionEmptyArray<DataTypeDate>>(factory);
    registerFunction<FunctionEmptyArray<DataTypeDateTime>>(factory);
    registerFunction<FunctionEmptyArray<DataTypeString>>(factory);
}

}
