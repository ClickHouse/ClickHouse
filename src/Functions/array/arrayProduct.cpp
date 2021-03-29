#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
}

/// arrayProduct([1,2,3])=6
class FunctionArrayProduct : public IFunction
{
public:
    static constexpr auto name = "arrayProduct";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionArrayProduct>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override;

private: 
    template <typename T>
    static ColumnPtr executeProductDecimal(const IColumn & col_from, const IColumn::Offsets & offsets);

    template <typename T>
    static ColumnPtr executeProduct(const IColumn & src_data);
};

DataTypePtr FunctionArrayProduct::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    if (arguments.size() != 1)
        throw Exception(
            "Function " + getName() + " needs one argument; passed " + toString(arguments.size()) + ".",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].type.get());
    if (!array_type)
        throw Exception(
            "Argument 0 of function " + getName() + " must be array. Found " + arguments[0].type->getName() + " instead.",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    const DataTypePtr & nest_type = array_type->getNestedType();
    const WhichDataType type(nest_type);
    if (!(type.isInt() || type.isUInt() || type.isFloat() || type.isDecimal()))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument for function '{}' must be an array of integer/float/decimal type, got '{}' instead",
            getName(),
            nest_type->getName());
    if (type.isDecimal())
    {
        UInt32 scale = getDecimalScale(*nest_type);
        return std::make_shared<DataTypeDecimal<Decimal128>>(DecimalUtils::max_precision<Decimal128>, scale);
    }

    return nest_type;
}

template <typename T>
ColumnPtr FunctionArrayProduct::executeProduct(const IColumn & col_from)
{
    const ColumnVector<T> * col_from_data = checkAndGetColumn<ColumnVector<T>>(&col_from);
    if (!col_from_data)
        return nullptr;

    size_t size = col_from_data->size();
    const PaddedPODArray<T> & values = col_from_data->getData();

    auto col_res = ColumnVector<T>::create();
    typename ColumnVector<T>::Container & res = col_res->getData();
    if (size > 0)
    {
        res.resize(1);
        res[0] = values[0];
        for (size_t i = 1; i < size; i++)
            res[0] *= values[i];
    }
    return col_res;
}

template <typename T>
ColumnPtr FunctionArrayProduct::executeProductDecimal(const IColumn & col_from, const IColumn::Offsets & offsets)
{
    const ColumnDecimal<T> * col_from_data = checkAndGetColumn<ColumnDecimal<T>>(&col_from);
    if (!col_from_data)
        return nullptr;

    size_t size = col_from_data->size();
    auto & values = col_from_data->getData();

    UInt32 scale = (offsets.size() == 0) ? col_from_data->getScale() : (col_from_data->getScale() * size);
    auto col_res = ColumnDecimal<Decimal128>::create(offsets.size(), scale);
    typename ColumnDecimal<Decimal128>::Container & res = col_res->getData();

    if (size > 0)
    {
        res.resize(1);
        res[0] = values[0];
        for (size_t i = 1; i < size; i++)
            res[0] *= values[i];
    }
    
    return col_res;
}


ColumnPtr FunctionArrayProduct::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const
{
    const ColumnArray * column_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());

    if (!column_array)
        throw Exception("Argument 0 of function " + getName() + " must be array", ErrorCodes::ILLEGAL_COLUMN);

    const IColumn & src_data = column_array->getData();
    const IColumn::Offsets & offsets = column_array->getOffsets();

    ColumnPtr res;
    if (!((res = executeProduct<UInt8>(src_data))
          || (res = executeProduct<UInt16>(src_data))
          || (res = executeProduct<UInt32>(src_data))
          || (res = executeProduct<UInt64>(src_data))
          || (res = executeProduct<Int8>(src_data))
          || (res = executeProduct<Int16>(src_data))
          || (res = executeProduct<Int32>(src_data))
          || (res = executeProduct<Int64>(src_data))
          || (res = executeProduct<Float32>(src_data))
          || (res = executeProduct<Float64>(src_data))
          || (res = executeProductDecimal<Decimal32>(src_data, offsets))
          || (res = executeProductDecimal<Decimal64>(src_data, offsets))
          || (res = executeProductDecimal<Decimal128>(src_data, offsets))))
        throw Exception(
            "Illegal column " + arguments[0].column->getName() + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
    return res;
}

void registerFunctionArrayProduct(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayProduct>();
}

}

