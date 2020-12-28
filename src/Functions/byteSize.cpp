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
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>

#include <common/logger_useful.h>
#include <ext/range.h>
#include <Poco/Logger.h>

namespace DB
{

namespace
{

template <typename T> struct ByteSizeForNative { static constexpr const UInt64 value = sizeof(typename NativeType<T>::Type); };

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
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
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
        auto result_col = ColumnUInt64::create(input_rows_count, 0);
        auto & vec_res = result_col->getData();
        for (const auto & arg : arguments)
        {
            const IColumn * column = arg.column.get();
            const IDataType * data_type = arg.type.get();
            byteSizeOne(data_type, column, vec_res);
        }
        return result_col;
    }

private:
    static void byteSizeOne(const IDataType * data_type, const IColumn * column, ColumnUInt64::Container & vec_res)
    {
        size_t vec_size = vec_res.size();

        UInt64 byte_size = 0;
        if (byteSizeByDataType(data_type, byte_size))
        {
            for (size_t i = 0; i < vec_size; ++i)
                vec_res[i] += byte_size;
        }
        else if (byteSizeByColumn(data_type, column, vec_res))
            ;
        else
            LOG_WARNING(&Poco::Logger::get("FunctionByteSize"),
                        "byteSize for \"{}\" is not supported.", data_type->getName());
    }

    static bool byteSizeByDataType(const IDataType * data_type, UInt64 & byte_size)
    {
        TypeIndex type_id = data_type->getTypeId();
        if (byteSizeByTypeId(type_id, byte_size))
            return true;
        else if (isFixedString(type_id)) {
            byte_size = checkAndGetDataType<DataTypeFixedString>(data_type)->getN();
            return true;
        }
        return false;
    }

    static bool byteSizeByTypeId(TypeIndex type_id, UInt64 & byte_size)
    {
        switch (type_id)
        {
            case TypeIndex::Nothing:    byte_size = 0; break;
            case TypeIndex::UInt8:      byte_size = ByteSizeForNative<UInt8>::value; break;
            case TypeIndex::UInt16:     byte_size = ByteSizeForNative<UInt16>::value; break;
            case TypeIndex::UInt32:     byte_size = ByteSizeForNative<UInt32>::value; break;
            case TypeIndex::UInt64:     byte_size = ByteSizeForNative<UInt64>::value; break;
            case TypeIndex::UInt128:    byte_size = ByteSizeForNative<UInt128>::value; break;
            case TypeIndex::UInt256:    byte_size = ByteSizeForNative<UInt256>::value; break;
            case TypeIndex::Int8:       byte_size = ByteSizeForNative<Int8>::value; break;
            case TypeIndex::Int16:      byte_size = ByteSizeForNative<Int16>::value; break;
            case TypeIndex::Int32:      byte_size = ByteSizeForNative<Int32>::value; break;
            case TypeIndex::Int64:      byte_size = ByteSizeForNative<Int64>::value; break;
            case TypeIndex::Int128:     byte_size = ByteSizeForNative<Int128>::value; break;
            case TypeIndex::Int256:     byte_size = ByteSizeForNative<Int256>::value; break;
            case TypeIndex::Float32:    byte_size = ByteSizeForNative<Float32>::value; break;
            case TypeIndex::Float64:    byte_size = ByteSizeForNative<Float64>::value; break;
            case TypeIndex::Date:       byte_size = ByteSizeForNative<DataTypeDate>::value; break;
            case TypeIndex::DateTime:   byte_size = ByteSizeForNative<DataTypeDateTime>::value; break;
            case TypeIndex::DateTime64: byte_size = ByteSizeForNative<DataTypeDateTime64>::value; break;
            case TypeIndex::Enum8:      byte_size = ByteSizeForNative<DataTypeEnum8>::value; break;
            case TypeIndex::Enum16:     byte_size = ByteSizeForNative<DataTypeEnum16>::value; break;
            case TypeIndex::Decimal32:  byte_size = ByteSizeForNative<Decimal32>::value; break;
            case TypeIndex::Decimal64:  byte_size = ByteSizeForNative<Decimal64>::value; break;
            case TypeIndex::Decimal128: byte_size = ByteSizeForNative<Decimal128>::value; break;
            case TypeIndex::Decimal256: byte_size = ByteSizeForNative<Decimal256>::value; break;
            case TypeIndex::UUID:       byte_size = ByteSizeForNative<DataTypeUUID>::value; break;
            // case TypeIndex::Interval:            internal use only.
            // case TypeIndex::Set:                 internal use only.
            // case TypeIndex::Function:            internal use only.
            // case TypeIndex::AggregateFunction:   internal use only.
            default: return false;
        }

        return true;
    }

    static bool byteSizeByColumn(const IDataType * data_type, const IColumn * column, ColumnUInt64::Container & vec_res)
    {
        WhichDataType which(data_type);
        size_t vec_size = vec_res.size();

        if (which.isString()) // TypeIndex::String
        {
            const ColumnString * col_str = checkAndGetColumn<ColumnString>(column);
            const auto & offsets = col_str->getOffsets();
            ColumnString::Offset prev_offset = 0;
            for (size_t i = 0; i < vec_size; ++i)
            {
                vec_res[i] += offsets[i] - prev_offset + sizeof(offsets[0]);
                prev_offset = offsets[i];
            }
            return true;
        }
        else if (which.isArray()) // TypeIndex::Array
        {
            if (byteSizeForConstArray(column, vec_res))
                return true;

            const ColumnArray * col_arr = checkAndGetColumn<ColumnArray>(column);
            return byteSizeForArrayByDataType(col_arr, vec_res)
                   || byteSizeForArray(col_arr, vec_res);
        }
        else if (which.isNullable()) // TypeIndex::Nullable
        {
            const ColumnNullable * col_null = checkAndGetColumn<ColumnNullable>(column);
            for (size_t i = 0; i < vec_size; ++i)
            {
                UInt64 byte_size = sizeof(bool);
                if (!col_null->isNullAt(i))
                    byte_size += byteSizeForNestedItem(col_null->getNestedColumn(), i);
                vec_res[i] += byte_size;
            }
            return true;
        }
        else if (which.isTuple()) // TypeIndex::Tuple
        {
            const DataTypeTuple * data_type_tuple = checkAndGetDataType<DataTypeTuple>(data_type);
            const ColumnTuple * col_tuple = checkAndGetColumn<ColumnTuple>(column);

            size_t tuple_size = col_tuple->tupleSize();
            for (size_t col_idx = 0; col_idx < tuple_size; ++col_idx)
            {
                const IDataType * nested_data_type = data_type_tuple->getElements()[col_idx].get();
                const IColumn * nested_col = col_tuple->getColumnPtr(col_idx).get();
                byteSizeOne(nested_data_type, nested_col, vec_res);
            }
            return true;
        }
        else if (which.isLowCardinality()) // TypeIndex::LowCardinality
        {
            const ColumnLowCardinality * col_low = checkAndGetColumn<ColumnLowCardinality>(column);
            size_t byte_size = col_low->getSizeOfIndexType();
            for (size_t i = 0; i < vec_size; ++i)
                vec_res[i] += byte_size;
            return true;
        }

        return false;
    }

    static bool byteSizeForArrayByDataType(const ColumnArray * col_arr, ColumnUInt64::Container & vec_res)
    {
        TypeIndex type_id = col_arr->getData().getDataType();
        UInt64 byte_size = 0;
        if (byteSizeByTypeId(type_id, byte_size))
        {
            size_t vec_size = vec_res.size();
            const auto & offsets = col_arr->getOffsets();
            ColumnArray::Offset prev_offset = 0;
            for (size_t i = 0; i < vec_size; ++i)
            {
                size_t array_size = offsets[i] - prev_offset;
                vec_res[i] += array_size * byte_size + sizeof(offsets[0]);
                prev_offset = offsets[i];
            }
            return true;
        }
        return false;
    }

    static inline bool byteSizeForConstArray(const IColumn * column, ColumnUInt64::Container & vec_res) {
        const ColumnConst * col_arr = checkAndGetColumnConst<ColumnArray>(column);
        if (!col_arr)
            return false;
        size_t vec_size = vec_res.size();
        const UInt64 byte_size = col_arr->byteSize();
        for (size_t i = 0; i < vec_size; ++i)
            vec_res[i] += byte_size;
        return true;
    }

    static UInt64 byteSizeForNestedItem(const IColumn & column, size_t idx)
    {
        if (const ColumnString * col_str = checkAndGetColumn<ColumnString>(&column))
            return col_str->getDataAtWithTerminatingZero(idx).size + sizeof(col_str->getOffsets()[0]);
        else if (const ColumnArray * col_arr = checkAndGetColumn<ColumnArray>(&column))
        {
            UInt64 byte_size = 0;
            const auto & offsets = col_arr->getOffsets();
            ColumnArray::Offset current_offset = idx == 0 ? 0 : offsets[idx - 1];
            size_t array_size = offsets[idx] - current_offset;
            for (size_t i = 0; i < array_size; ++i)
                byte_size += byteSizeForNestedItem(col_arr->getData(), current_offset + i);
            return byte_size + sizeof(offsets[0]);
        }
        else
            return column.getDataAtWithTerminatingZero(idx).size;
    }

    static bool byteSizeForArray(const ColumnArray * col_arr, ColumnUInt64::Container & vec_res)
    {
        const IColumn & col_nested = col_arr->getData();
        size_t vec_size = vec_res.size();
        const auto & offsets = col_arr->getOffsets();

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < vec_size; ++i)
        {
            UInt64 byte_size = 0;
            size_t array_size = offsets[i] - current_offset;
            for (size_t j = 0; j < array_size; ++j)
                byte_size += byteSizeForNestedItem(col_nested, current_offset + j);
            vec_res[i] += byte_size + sizeof(offsets[0]);
            current_offset = offsets[i];
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
