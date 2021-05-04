#include <Functions/IFunctionImpl.h>
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

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return ColumnArray::create(
            DataType().createColumn(),
            ColumnArray::ColumnOffsets::create(input_rows_count, 0));
    }
};


using FunctionEmptyArrayUInt8 = FunctionEmptyArray<DataTypeUInt8>;
using FunctionEmptyArrayUInt16 = FunctionEmptyArray<DataTypeUInt16>;
using FunctionEmptyArrayUInt32 = FunctionEmptyArray<DataTypeUInt32>;
using FunctionEmptyArrayUInt64 = FunctionEmptyArray<DataTypeUInt64>;
using FunctionEmptyArrayInt8 = FunctionEmptyArray<DataTypeInt8>;
using FunctionEmptyArrayInt16 = FunctionEmptyArray<DataTypeInt16>;
using FunctionEmptyArrayInt32 = FunctionEmptyArray<DataTypeInt32>;
using FunctionEmptyArrayInt64 = FunctionEmptyArray<DataTypeInt64>;
using FunctionEmptyArrayFloat32 = FunctionEmptyArray<DataTypeFloat32>;
using FunctionEmptyArrayFloat64 = FunctionEmptyArray<DataTypeFloat64>;
using FunctionEmptyArrayDate = FunctionEmptyArray<DataTypeDate>;
using FunctionEmptyArrayDateTime = FunctionEmptyArray<DataTypeDateTime>;
using FunctionEmptyArrayString = FunctionEmptyArray<DataTypeString>;

template <typename F>
void registerFunction(FunctionFactory & factory)
{
    factory.registerFunction<F>(F::getNameImpl());
}

}

void registerFunctionsEmptyArray(FunctionFactory & factory)
{
    registerFunction<FunctionEmptyArrayUInt8>(factory);
    registerFunction<FunctionEmptyArrayUInt16>(factory);
    registerFunction<FunctionEmptyArrayUInt32>(factory);
    registerFunction<FunctionEmptyArrayUInt64>(factory);
    registerFunction<FunctionEmptyArrayInt8>(factory);
    registerFunction<FunctionEmptyArrayInt16>(factory);
    registerFunction<FunctionEmptyArrayInt32>(factory);
    registerFunction<FunctionEmptyArrayInt64>(factory);
    registerFunction<FunctionEmptyArrayFloat32>(factory);
    registerFunction<FunctionEmptyArrayFloat64>(factory);
    registerFunction<FunctionEmptyArrayDate>(factory);
    registerFunction<FunctionEmptyArrayDateTime>(factory);
    registerFunction<FunctionEmptyArrayString>(factory);
}

}
