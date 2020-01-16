#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Common/FieldVisitors.h>
#include <Common/memcmpSmall.h>
#include <Common/assert_cast.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename Method, typename Name>
class FunctionArrayScalarProduct : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionArrayScalarProduct>(context); }
    FunctionArrayScalarProduct(const Context & context_): context(context_) {}

private:
    using ResultColumnType = ColumnVector<typename Method::ResultType>;

    template <typename T>
    bool executeNumber(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        return executeNumberNumber<T, UInt8>(block, arguments, result)
            || executeNumberNumber<T, UInt16>(block, arguments, result)
            || executeNumberNumber<T, UInt32>(block, arguments, result)
            || executeNumberNumber<T, UInt64>(block, arguments, result)
            || executeNumberNumber<T, Int8>(block, arguments, result)
            || executeNumberNumber<T, Int16>(block, arguments, result)
            || executeNumberNumber<T, Int32>(block, arguments, result)
            || executeNumberNumber<T, Int64>(block, arguments, result)
            || executeNumberNumber<T, Float32>(block, arguments, result)
            || executeNumberNumber<T, Float64>(block, arguments, result);
    }


    template <typename T, typename U>
    bool executeNumberNumber(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        ColumnPtr col1 = block.getByPosition(arguments[0]).column->convertToFullColumnIfConst();
        ColumnPtr col2 = block.getByPosition(arguments[1]).column->convertToFullColumnIfConst();
        if (! col1 || ! col2) 
            return false;

        const ColumnArray* col_array1 = checkAndGetColumn<ColumnArray>(col1.get());
        const ColumnArray* col_array2 = checkAndGetColumn<ColumnArray>(col2.get());
        if (! col_array1 || ! col_array2) 
            return false;

        const ColumnVector<T> * col_nested1 = checkAndGetColumn<ColumnVector<T>>(col_array1->getData());
        const ColumnVector<U> * col_nested2 = checkAndGetColumn<ColumnVector<U>>(col_array2->getData());
        if (! col_nested1 || ! col_nested2)
            return false;

        auto col_res = ResultColumnType::create();
        vector(col_nested1->getData(), col_array1->getOffsets(),
            col_nested2->getData(), col_array2->getOffsets(), col_res->getData());
        block.getByPosition(result).column = std::move(col_res);
        return true;
    }

    template <typename T, typename U>
    static void vector(const PaddedPODArray<T> & data1, const ColumnArray::Offsets & offsets1,
        const PaddedPODArray<U> & data2, const ColumnArray::Offsets & offsets2, 
        PaddedPODArray<typename Method::ResultType> & result)
    {
        size_t size = offsets1.size();
        result.resize(size);

        ColumnArray::Offset current_offset1 = 0;
        ColumnArray::Offset current_offset2 = 0;
        for (size_t i = 0; i < size; ++i) {
            size_t array1_size = offsets1[i] - current_offset1;
            size_t array2_size = offsets2[i] - current_offset2;
            result[i] = Method::apply(data1, current_offset1, array1_size, data2, current_offset2, array2_size);

            current_offset1 = offsets1[i];
            current_offset2 = offsets2[i];
        }
    }

public:
    /// Get function name.
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        // Basic type check
        std::vector<DataTypePtr> nested_types(2, nullptr);
        for (size_t i = 0; i < getNumberOfArguments(); ++i) 
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].get());
            if (!array_type)
                throw Exception("All argument for function " + getName() + " must be an array.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            
            auto & nested_type = array_type->getNestedType();
            WhichDataType which(nested_type);
            bool is_number = which.isNativeInt() || which.isNativeUInt() || which.isFloat();
            if (! is_number) 
            {
                throw Exception(getName() + " cannot process values of type " + nested_type->getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
            nested_types[i] = nested_type;
        }

        // Detail type check in Method, then return ReturnType
        return Method::getReturnType(nested_types[0], nested_types[1]);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /* input_rows_count */) override
    {
        if (!(executeNumber<UInt8>(block, arguments, result)
            || executeNumber<UInt16>(block, arguments, result)
            || executeNumber<UInt32>(block, arguments, result)
            || executeNumber<UInt64>(block, arguments, result)
            || executeNumber<Int8>(block, arguments, result)
            || executeNumber<Int16>(block, arguments, result)
            || executeNumber<Int32>(block, arguments, result)
            || executeNumber<Int64>(block, arguments, result)
            || executeNumber<Float32>(block, arguments, result)
            || executeNumber<Float64>(block, arguments, result)))
            throw Exception{"Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};

    }

private:
    const Context & context;
};

}