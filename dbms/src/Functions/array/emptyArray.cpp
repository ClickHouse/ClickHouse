#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

/// TODO Make it simple.

template <typename Type> struct TypeToColumnType { using ColumnType = ColumnVector<Type>; };
template <> struct TypeToColumnType<String> { using ColumnType = ColumnString; };

template <typename DataType> struct DataTypeToName : TypeName<typename DataType::FieldType> { };
template <> struct DataTypeToName<DataTypeDate> { static std::string get() { return "Date"; } };
template <> struct DataTypeToName<DataTypeDateTime> { static std::string get() { return "DateTime"; } };


template <typename DataType>
struct FunctionEmptyArray : public IFunction
{
    static constexpr auto base_name = "emptyArray";
    static const String name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionEmptyArray>(); }

private:
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataType>());
    }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        using UnderlyingColumnType = typename TypeToColumnType<typename DataType::FieldType>::ColumnType;

        block.getByPosition(result).column = ColumnArray::create(
            UnderlyingColumnType::create(),
            ColumnArray::ColumnOffsets::create(input_rows_count, 0));
    }
};


template <typename DataType>
const String FunctionEmptyArray<DataType>::name = FunctionEmptyArray::base_name + String(DataTypeToName<DataType>::get());

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


void registerFunctionsEmptyArray(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayUInt8>();
    factory.registerFunction<FunctionEmptyArrayUInt16>();
    factory.registerFunction<FunctionEmptyArrayUInt32>();
    factory.registerFunction<FunctionEmptyArrayUInt64>();
    factory.registerFunction<FunctionEmptyArrayInt8>();
    factory.registerFunction<FunctionEmptyArrayInt16>();
    factory.registerFunction<FunctionEmptyArrayInt32>();
    factory.registerFunction<FunctionEmptyArrayInt64>();
    factory.registerFunction<FunctionEmptyArrayFloat32>();
    factory.registerFunction<FunctionEmptyArrayFloat64>();
    factory.registerFunction<FunctionEmptyArrayDate>();
    factory.registerFunction<FunctionEmptyArrayDateTime>();
    factory.registerFunction<FunctionEmptyArrayString>();
}

}
