#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Core/Types.h>
#include <Common/UInt128.h>
#include <ext/range.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
}

namespace
{

template <typename T> struct ByteSizeForNative { static constexpr const UInt64 value = sizeof(typename NativeType<T>::Type); };
template <typename T> struct ByteSizeForNumberBase { static constexpr const UInt64 value = sizeof(typename NativeType<typename T::FieldType>::Type); };
template <typename T> struct ByteSizeForEnum { static constexpr const UInt64 value = sizeof(typename T::FieldType); };

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

    /// Get the function name.
    String getName() const override
    {
        return name;
    }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

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
            const ColumnPtr & column = arg.column;
            const IDataType * data_type = arg.type.get();
            WhichDataType which(data_type);

            byteSizeOne(data_type, column, vec_res);
        }
        return result_col;
    }

    static void byteSizeOne(const IDataType * data_type, const ColumnPtr & column, ColumnUInt64::Container & vec_res)
    {
        size_t vec_size = vec_res.size();

        UInt64 byte_size = 0;
        if (byteSizeByDataType(data_type, byte_size))
        {
            for (size_t i = 0; i < vec_size; ++i)
            {
                vec_res[i] += byte_size;
            }
        }
        else
        {
            byteSizeByColumn(data_type, column, vec_res);
        }
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
            case TypeIndex::Date:       byte_size = ByteSizeForNumberBase<DataTypeDate>::value; break;
            case TypeIndex::DateTime:   byte_size = ByteSizeForNumberBase<DataTypeDateTime>::value; break;
            case TypeIndex::DateTime64: byte_size = ByteSizeForNumberBase<DataTypeDateTime64>::value; break;
//            case TypeIndex::String:     return TypeName<String>::get();
//            case TypeIndex::FixedString:
            case TypeIndex::Enum8:      byte_size = ByteSizeForEnum<DataTypeEnum8>::value; break;
            case TypeIndex::Enum16:     byte_size = ByteSizeForEnum<DataTypeEnum16>::value; break;
            case TypeIndex::Decimal32:  byte_size = ByteSizeForNative<Decimal32>::value; break;
            case TypeIndex::Decimal64:  byte_size = ByteSizeForNative<Decimal64>::value; break;
            case TypeIndex::Decimal128: byte_size = ByteSizeForNative<Decimal128>::value; break;
            case TypeIndex::Decimal256: byte_size = ByteSizeForNative<Decimal256>::value; break;
            case TypeIndex::UUID:       byte_size = ByteSizeForNumberBase<DataTypeUUID>::value; break;
//            case TypeIndex::Array:      return "Array";
//            case TypeIndex::Tuple:      return "Tuple";
//            case TypeIndex::Set:        return "Set";
//            case TypeIndex::Interval:   byte_size = ByteSizeForNumberBase<DataTypeInterval>::value; break;
//            case TypeIndex::Nullable:   return "Nullable";
//            case TypeIndex::Function:   return "Function";
//            case TypeIndex::AggregateFunction: return "AggregateFunction";
//            case TypeIndex::LowCardinality: return "LowCardinality";
            default: return false;
        }

        return true;
    }

    static bool byteSizeByDataType(const IDataType * data_type, UInt64 & byte_size)
    {
        TypeIndex type_id = data_type->getTypeId();
        if (byteSizeByTypeId(type_id, byte_size))
            return true;

        switch (type_id)
        {
            case TypeIndex::FixedString:
                byte_size = typeid_cast<const DataTypeFixedString *>(data_type)->getN();
                break;
            default: return false;
        }
        return true;
    }

#define INTEGRAL_TPL_PACK UInt8, UInt16, UInt32, UInt64, UInt128, UInt256, \
                          Int8, Int16, Int32, Int64, Int128, Int256, \
                          Float32, Float64
                          // The following is not supported by ColumnVector:
                          //   Decimal32, Decimal64, Decimal128, Decimal256
                          //   DataTypeEnum8, DataTypeEnum16, DataTypeDate, DataTypeDateTime, DataTypeDateTime64
                          //   DataTypeUUID, DataTypeInterval

    static bool byteSizeByColumn(const IDataType * data_type, const ColumnPtr & column, ColumnUInt64::Container & vec_res)
    {
        WhichDataType which(data_type);
        size_t vec_size = vec_res.size();

        if (which.isString()) // TypeIndex::String
        {
            const ColumnString * col_str = checkAndGetColumn<ColumnString>(column.get());
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
            if (byteSizeForConstArray(data_type, column, vec_res))
                return true;

            const ColumnArray * col_arr = checkAndGetColumn<ColumnArray>(column.get());
            return byteSizeForIntegralArray<INTEGRAL_TPL_PACK>(col_arr, data_type, column, vec_res)
                || byteSizeForStringArray(col_arr, column, vec_res)
                || byteSizeForGenericArray(col_arr, column, vec_res);
        }

        return false;
    }

    static inline bool byteSizeForConstArray(const IDataType * /*data_type*/, const ColumnPtr & column, ColumnUInt64::Container & vec_res) {
        const ColumnConst * col_arr = checkAndGetColumnConst<ColumnArray>(column.get());
        if (!col_arr)
            return false;
        size_t vec_size = vec_res.size();
        const UInt64 byte_size = col_arr->byteSize();
        for (size_t i = 0; i < vec_size; ++i)
            vec_res[i] += byte_size;
        return true;
    }

#undef INTEGRAL_TPL_PACK

    template <class ...Integral>
    static inline bool byteSizeForIntegralArray(const ColumnArray * col_arr, const IDataType * data_type, const ColumnPtr & column, ColumnUInt64::Container & vec_res)
    {
        return (byteSizeForIntegralArrayExpanded<Integral>(col_arr, data_type, column, vec_res) || ...);
    }

    template <class NestedType>
    static bool byteSizeForIntegralArrayExpanded(const ColumnArray * col_arr, const IDataType * /*data_type*/, const ColumnPtr & /*column*/, ColumnUInt64::Container & vec_res)
    {
        const ColumnVector<NestedType> * col_nested = checkAndGetColumn<ColumnVector<NestedType>>(&(col_arr->getData()));
        if (!col_nested)
            return false;

        size_t vec_size = vec_res.size();
        const auto & offsets = col_arr->getOffsets();

        ColumnArray::Offset prev_offset = 0;
        for (size_t i = 0; i < vec_size; ++i)
        {
            UInt64 byte_size = 0;
            byteSizeByTypeId(TypeId<NestedType>::value, byte_size);

            size_t array_size = offsets[i] - prev_offset;
            vec_res[i] += array_size * byte_size + sizeof(offsets[0]);
            prev_offset = offsets[i];
        }
        return true;
    }

    static bool byteSizeForStringArray(const ColumnArray * col_arr, const ColumnPtr & /*column*/, ColumnUInt64::Container & vec_res)
    {
        const ColumnString * col_nested = checkAndGetColumn<ColumnString>(&(col_arr->getData()));
        if (!col_nested)
            return false;

        size_t vec_size = vec_res.size();
        const auto & offsets = col_arr->getOffsets();
        const auto & string_offsets = col_nested->getOffsets();

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < vec_size; ++i)
        {
            UInt64 byte_size = 0;
            size_t array_size = offsets[i] - current_offset;
            for (size_t j = 0; j < array_size; ++j)
            {
                const ColumnArray::Offset string_pos = current_offset + j == 0
                                                       ? 0
                                                       : string_offsets[current_offset + j - 1];
                byte_size += string_offsets[current_offset + j] - string_pos;
            }
            vec_res[i] += byte_size + sizeof(offsets[0]) + array_size * sizeof(string_offsets[0]);
            current_offset = offsets[i];
        }
        return true;
    }

    static bool byteSizeForGenericArray(const ColumnArray * col_arr, const ColumnPtr & /*column*/, ColumnUInt64::Container & vec_res)
    {
        const IColumn & col_nested = col_arr->getData();
        size_t vec_size = vec_res.size();
        const auto & offsets = col_arr->getOffsets();

        ColumnArray::Offset prev_offset = 0;
        for (size_t i = 0; i < vec_size; ++i)
        {
            size_t array_size = offsets[i] - prev_offset;
            UInt64 byte_size = 0;
            for (size_t j = 0; j < array_size; ++j)
                byte_size += col_nested.getDataAt(prev_offset + j).size;
            vec_res[i] += byte_size;
            prev_offset = offsets[i];
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
