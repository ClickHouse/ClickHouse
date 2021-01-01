#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Common/UInt128.h>
#include <Core/Types.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes//DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>

#include <ext/range.h>

namespace DB
{

namespace
{

struct ResultHolder
{
    ColumnUInt64::Container & vec_res;
    bool is_first_argument;

    ResultHolder(ColumnUInt64::Container & vec_res_, bool is_first_argument_)
        : vec_res(vec_res_), is_first_argument(is_first_argument_) {}

    void set(UInt64 byte_size)
    {
        size_t vec_size = vec_res.size();
        if (is_first_argument)
        {
            for (size_t i = 0; i < vec_size; ++i)
                vec_res[i] = byte_size;
        }
        else
        {
            for (size_t i = 0; i < vec_size; ++i)
                vec_res[i] += byte_size;
        }
    }

    void set(size_t idx, UInt64 byte_size)
    {
        if (is_first_argument)
            vec_res[idx] = byte_size;
        else
            vec_res[idx] += byte_size;
    }

    size_t size() const
    {
        return vec_res.size();
    }
};

/** byteSize() - get the columns size in number of bytes.
  */
class FunctionByteSize : public IFunction
{
public:
    static constexpr auto name = "byteSize";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionByteSize>();
    }

    String getName() const override { return name; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto result_col = ColumnUInt64::create(input_rows_count);
        auto & vec_res = result_col->getData();
        size_t size = arguments.size();
        for (size_t i = 0; i < size; ++i)
        {
            const IColumn * column = arguments[i].column.get();
            const IDataType * data_type = arguments[i].type.get();
            ResultHolder res(vec_res, i == 0);
            byteSizeOne(data_type, column, res);
        }
        return result_col;
    }

private:
    static void byteSizeOne(const IDataType * data_type, const IColumn * column, ResultHolder & res)
    {
        UInt64 byte_size = 0;
        if (byteSizeByDataType(data_type, byte_size))
        {
            res.set(byte_size);
        }
        else if (byteSizeByColumn(data_type, column, res))
            ;
        else
            throw Exception("byteSize for \"" + data_type->getName() + "\" is not supported.",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    static bool byteSizeByDataType(const IDataType * data_type, UInt64 & byte_size)
    {
        if (data_type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
        {
            byte_size = data_type->getSizeOfValueInMemory();
            return true;
        }
        return false;
    }

    static bool byteSizeByColumn(const IDataType * data_type, const IColumn * column, ResultHolder & res)
    {
        WhichDataType which(data_type);
        size_t res_size = res.size();

        UInt64 byte_size = 0;
        if (byteSizeForConstColumn(data_type, column, byte_size))
        {
            res.set(byte_size);
            return true;
        }
        else if (which.isString()) // TypeIndex::String
        {
            const ColumnString * col_str = checkAndGetColumn<ColumnString>(column);
            const auto & offsets = col_str->getOffsets();
            ColumnString::Offset prev_offset = 0;
            for (size_t i = 0; i < res_size; ++i)
            {
                ColumnString::Offset current_offset = offsets[i]; // to ensure offsets[i] not aliased with vec_res[i].
                res.set(i, current_offset - prev_offset + sizeof(offsets[0]));
                prev_offset = current_offset;
            }
            return true;
        }
        else if (which.isArray()) // TypeIndex::Array
        {
            const ColumnArray * col_arr = checkAndGetColumn<ColumnArray>(column);
            const DataTypeArray * type_arr = checkAndGetDataType<DataTypeArray>(data_type);
            return byteSizeForArrayByDataType(type_arr, col_arr, res)
                   || byteSizeForArray(type_arr, col_arr, res);
        }
        else if (which.isNullable()) // TypeIndex::Nullable
        {
            const ColumnNullable * col_null = checkAndGetColumn<ColumnNullable>(column);
            const DataTypeNullable * type_null = checkAndGetDataType<DataTypeNullable>(data_type);
            for (size_t i = 0; i < res_size; ++i)
            {
                byte_size = sizeof(bool);
                if (!col_null->isNullAt(i))
                    byte_size += byteSizeForNestedItem(type_null->getNestedType().get(), &col_null->getNestedColumn(), i);
                res.set(i, byte_size);
            }
            return true;
        }
        else if (which.isTuple()) // TypeIndex::Tuple
        {
            const ColumnTuple * col_tuple = checkAndGetColumn<ColumnTuple>(column);
            const DataTypeTuple * type_tuple = checkAndGetDataType<DataTypeTuple>(data_type);

            size_t tuple_size = col_tuple->tupleSize();
            for (size_t col_idx = 0; col_idx < tuple_size; ++col_idx)
            {
                const IColumn * type_nested = col_tuple->getColumnPtr(col_idx).get();
                const IDataType * col_nested = type_tuple->getElements()[col_idx].get();
                ResultHolder res_nested(res.vec_res, res.is_first_argument && col_idx == 0);
                byteSizeOne(col_nested, type_nested, res_nested);
            }
            return true;
        }
        else if (which.isLowCardinality()) // TypeIndex::LowCardinality
        {
            const ColumnLowCardinality * col_low = checkAndGetColumn<ColumnLowCardinality>(column);
            byte_size = col_low->getSizeOfIndexType();
            res.set(byte_size);
            return true;
        }
        return false;
    }

    static bool byteSizeForConstColumn(const IDataType * data_type, const IColumn * column, UInt64 & byte_size)
    {
        if (const ColumnConst * col_const = checkAndGetColumn<ColumnConst>(column))
        {
            const IColumn * col_data = &col_const->getDataColumn();
            byte_size = byteSizeForNestedItem(data_type, col_data, 0);
            return true;
        }
        return false;
    }

    static bool byteSizeForArrayByDataType(const DataTypeArray * data_type, const ColumnArray * column, ResultHolder & res)
    {
        UInt64 byte_size = 0;
        if (byteSizeByDataType(data_type->getNestedType().get(), byte_size))
        {
            size_t res_size = res.size();
            const auto & offsets = column->getOffsets();
            ColumnArray::Offset prev_offset = 0;
            for (size_t i = 0; i < res_size; ++i)
            {
                size_t array_size = offsets[i] - prev_offset;
                res.set(i, array_size * byte_size + sizeof(offsets[0]));
                prev_offset += array_size;
            }
            return true;
        }
        return false;
    }

    static UInt64 byteSizeForNestedItem(const IDataType * data_type, const IColumn * column, size_t idx)
    {
        WhichDataType which(data_type);
        UInt64 byte_size = 0;

        if (byteSizeByDataType(data_type, byte_size))
            return byte_size;
        else if (which.isString())
        {
            const ColumnString * col_str = checkAndGetColumn<ColumnString>(column);
            return col_str->getDataAtWithTerminatingZero(idx).size + sizeof(col_str->getOffsets()[0]);
        }
        else if (which.isArray())
        {
            const ColumnArray * col_arr = checkAndGetColumn<ColumnArray>(column);
            const DataTypeArray * type_arr = checkAndGetDataType<DataTypeArray>(data_type);
            const auto & offsets = col_arr->getOffsets();
            ColumnArray::Offset current_offset = idx == 0 ? 0 : offsets[idx - 1];
            size_t array_size = offsets[idx] - current_offset;
            for (size_t i = 0; i < array_size; ++i)
                byte_size += byteSizeForNestedItem(type_arr->getNestedType().get(), &col_arr->getData(), current_offset + i);
            return byte_size + sizeof(offsets[0]);
        }
        else if (which.isNullable())
        {
            const ColumnNullable * col_null = checkAndGetColumn<ColumnNullable>(column);
            const DataTypeNullable * type_null = checkAndGetDataType<DataTypeNullable>(data_type);
            byte_size = sizeof(bool);
            if (!col_null->isNullAt(idx))
                byte_size += byteSizeForNestedItem(type_null->getNestedType().get(), &col_null->getNestedColumn(), idx);
            return byte_size;
        }
        else if (which.isTuple())
        {
            const ColumnTuple * col_tuple = checkAndGetColumn<ColumnTuple>(column);
            const DataTypeTuple * type_tuple = checkAndGetDataType<DataTypeTuple>(data_type);

            byte_size = 0;
            size_t tuple_size = col_tuple->tupleSize();
            for (size_t col_idx = 0; col_idx < tuple_size; ++col_idx)
            {
                const IColumn * col_nested = col_tuple->getColumnPtr(col_idx).get();
                const IDataType * type_nested = type_tuple->getElements()[col_idx].get();
                byte_size += byteSizeForNestedItem(type_nested, col_nested, idx);
            }
            return byte_size;
        }
        else if (which.isLowCardinality())
        {
            const ColumnLowCardinality * col_low = checkAndGetColumn<ColumnLowCardinality>(column);
            return col_low->getSizeOfIndexType();
        }

        throw Exception("byteSize for \"" + data_type->getName() + "\" is not supported.",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    static bool byteSizeForArray(const DataTypeArray * data_type, const ColumnArray * column, ResultHolder & res)
    {
        const IColumn * col_nested = &column->getData();
        const IDataType * type_nested = data_type->getNestedType().get();
        size_t res_size = res.size();
        const auto & offsets = column->getOffsets();

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < res_size; ++i)
        {
            UInt64 byte_size = 0;
            size_t array_size = offsets[i] - current_offset;
            for (size_t j = 0; j < array_size; ++j)
                byte_size += byteSizeForNestedItem(type_nested, col_nested, current_offset + j);
            res.set(i, byte_size + sizeof(offsets[0]));
            current_offset += array_size;
        }
        return true;
    }
};

}

void registerFunctionByteSize(FunctionFactory & factory)
{
    factory.registerFunction<FunctionByteSize>();
}

}
