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

template <typename DataType, const char * FunctionName>
class FunctionEmptyArray : public IFunction
{
public:
    /// NOTE: Not using DataType().getName() because DataType ctor might be very heavy (e.g. for DataTypeDateTime as it initializes DateLUT singleton).
    static String getNameImpl() { return FunctionName; }
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

REGISTER_FUNCTION(EmptyArray)
{

#define REGISTER_EMPTY_ARRAY_FUNCTION(TYPE) \
    do { \
        static const char name[] = "emptyArray" #TYPE; \
       registerFunction<FunctionEmptyArray<DataType##TYPE, name>>(factory); \
    } while(0)

    REGISTER_EMPTY_ARRAY_FUNCTION(UInt8);
    REGISTER_EMPTY_ARRAY_FUNCTION(UInt16);
    REGISTER_EMPTY_ARRAY_FUNCTION(UInt32);
    REGISTER_EMPTY_ARRAY_FUNCTION(UInt64);
    REGISTER_EMPTY_ARRAY_FUNCTION(Int8);
    REGISTER_EMPTY_ARRAY_FUNCTION(Int16);
    REGISTER_EMPTY_ARRAY_FUNCTION(Int32);
    REGISTER_EMPTY_ARRAY_FUNCTION(Int64);
    REGISTER_EMPTY_ARRAY_FUNCTION(Float32);
    REGISTER_EMPTY_ARRAY_FUNCTION(Float64);
    REGISTER_EMPTY_ARRAY_FUNCTION(Date);
    REGISTER_EMPTY_ARRAY_FUNCTION(DateTime);
    REGISTER_EMPTY_ARRAY_FUNCTION(String);
}

}
