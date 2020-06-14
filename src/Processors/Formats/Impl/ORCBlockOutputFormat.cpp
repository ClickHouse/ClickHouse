#include <Processors/Formats/Impl/ORCBlockOutputFormat.h>

#include <Common/assert_cast.h>
#include <Formats/FormatFactory.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeArray.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

ORCOutputStream::ORCOutputStream(WriteBuffer & out_) : out(out_) {}

uint64_t ORCOutputStream::getLength() const
{
    return out.count();
}

uint64_t ORCOutputStream::getNaturalWriteSize() const
{
    out.nextIfAtEnd();
    return out.available();
}

void ORCOutputStream::write(const void* buf, size_t length)
{
    out.write(static_cast<const char *>(buf), length);
}

ORCBlockOutputFormat::ORCBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_)
    : IOutputFormat(header_, out_), format_settings{format_settings_}, output_stream(out_), data_types(header_.getDataTypes())
{
    schema = orc::createStructType();
    size_t columns_count = header_.columns();
    for (size_t i = 0; i != columns_count; ++i)
    {
        schema->addStructField(header_.safeGetByPosition(i).name, getORCType(data_types[i]));
    }
    writer = orc::createWriter(*schema, &output_stream, options);
}

ORC_UNIQUE_PTR<orc::Type> ORCBlockOutputFormat::getORCType(const DataTypePtr & type)
{
    switch (type->getTypeId())
    {
        case TypeIndex::UInt8: [[fallthrough]];
        case TypeIndex::Int8:
        {
            return orc::createPrimitiveType(orc::TypeKind::BYTE);
        }
        case TypeIndex::UInt16: [[fallthrough]];
        case TypeIndex::Int16:
        {
            return orc::createPrimitiveType(orc::TypeKind::SHORT);
        }
        case TypeIndex::UInt32: [[fallthrough]];
        case TypeIndex::Int32:
        {
            return orc::createPrimitiveType(orc::TypeKind::INT);
        }
        case TypeIndex::UInt64: [[fallthrough]];
        case TypeIndex::Int64:
        {
            return orc::createPrimitiveType(orc::TypeKind::LONG);
        }
        case TypeIndex::Float32:
        {
            return orc::createPrimitiveType(orc::TypeKind::FLOAT);
        }
        case TypeIndex::Float64:
        {
            return orc::createPrimitiveType(orc::TypeKind::DOUBLE);
        }
        case TypeIndex::Date:
        {
            return orc::createPrimitiveType(orc::TypeKind::DATE);
        }
        case TypeIndex::DateTime: [[fallthrough]];
        case TypeIndex::DateTime64:
        {
            return orc::createPrimitiveType(orc::TypeKind::TIMESTAMP);
        }
        case TypeIndex::FixedString: [[fallthrough]];
        case TypeIndex::String:
        {
            return orc::createPrimitiveType(orc::TypeKind::STRING);
        }
        case TypeIndex::Nullable:
        {
            return getORCType(removeNullable(type));
        }
        /*
        case TypeIndex::Array:
        {
            const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
            return orc::createListType(getORCType(array_type->getNestedType()));
        }
         */
        case TypeIndex::Decimal32:
        {
            const auto * decimal_type = typeid_cast<const DataTypeDecimal<Decimal32> *>(type.get());
            return orc::createDecimalType(decimal_type->getPrecision(), decimal_type->getScale());
        }
        case TypeIndex::Decimal64:
        {
            const auto * decimal_type = typeid_cast<const DataTypeDecimal<Decimal64> *>(type.get());
            return orc::createDecimalType(decimal_type->getPrecision(), decimal_type->getScale());
        }
        case TypeIndex::Decimal128:
        {
            const auto * decimal_type = typeid_cast<const DataTypeDecimal<Decimal128> *>(type.get());
            return orc::createDecimalType(decimal_type->getPrecision(), decimal_type->getScale());
        }
        default:
        {
            throw Exception("Type " + type->getName() + " is not supported for ORC output format", ErrorCodes::ILLEGAL_COLUMN);
        }
    }
}

template <typename NumberType, typename NumberVectorBatch>
void ORCBlockOutputFormat::ORCBlockOutputFormat::writeNumbers(
        orc::ColumnVectorBatch * orc_column,
        const IColumn & column,
        const PaddedPODArray<UInt8> * null_bytemap,
        size_t rows_num)
{
    NumberVectorBatch * number_orc_column = dynamic_cast<NumberVectorBatch *>(orc_column);
    const auto & number_column = assert_cast<const ColumnVector<NumberType> &>(column);
    number_orc_column->resize(rows_num);

    for (size_t i = 0; i != rows_num; ++i)
    {
        if (null_bytemap && (*null_bytemap)[i])
        {
            number_orc_column->notNull[i] = 0;
            continue;
        }
        number_orc_column->data[i] = number_column.getElement(i);
    }
    number_orc_column->numElements = rows_num;
}

template <typename Decimal, typename DecimalVectorBatch, typename ConvertFunc>
void ORCBlockOutputFormat::ORCBlockOutputFormat::writeDecimals(
        orc::ColumnVectorBatch * orc_column,
        const IColumn & column,
        DataTypePtr & type,
        const PaddedPODArray<UInt8> * null_bytemap,
        size_t rows_num,
        ConvertFunc convert)
{
    DecimalVectorBatch *decimal_orc_column = dynamic_cast<DecimalVectorBatch *>(orc_column);
    const auto & decimal_column = assert_cast<const ColumnDecimal<Decimal> &>(column);
    const auto * decimal_type = typeid_cast<const DataTypeDecimal<Decimal> *>(type.get());
    decimal_orc_column->precision = decimal_type->getPrecision();
    decimal_orc_column->scale = decimal_type->getScale();
    decimal_orc_column->resize(rows_num);
    for (size_t i = 0; i != rows_num; ++i)
    {
        if (null_bytemap && (*null_bytemap)[i])
        {
            decimal_orc_column->notNull[i] = 0;
            continue;
        }
        decimal_orc_column->values[i] = convert(decimal_column.getElement(i).value);
    }
    decimal_orc_column->numElements = rows_num;
}

void ORCBlockOutputFormat::writeColumn(
        orc::ColumnVectorBatch * orc_column,
        const IColumn & column,
        DataTypePtr & type,
        const PaddedPODArray<UInt8> * null_bytemap,
        size_t rows_num)
{
    if (null_bytemap)
    {
        orc_column->hasNulls = true;
    }
    switch (type->getTypeId())
    {
        case TypeIndex::Int8:
        {
            writeNumbers<Int8, orc::LongVectorBatch>(orc_column, column, null_bytemap, rows_num);
            break;
        }
        case TypeIndex::UInt8:
        {
            writeNumbers<UInt8, orc::LongVectorBatch>(orc_column, column, null_bytemap, rows_num);
            break;
        }
        case TypeIndex::Int16:
        {
            writeNumbers<Int16, orc::LongVectorBatch>(orc_column, column, null_bytemap, rows_num);
            break;
        }
        case TypeIndex::Date: [[fallthrough]];
        case TypeIndex::UInt16:
        {
            writeNumbers<UInt16, orc::LongVectorBatch>(orc_column, column, null_bytemap, rows_num);
            break;
        }
        case TypeIndex::Int32:
        {
            writeNumbers<Int32, orc::LongVectorBatch>(orc_column, column, null_bytemap, rows_num);
            break;
        }
        case TypeIndex::UInt32:
        {
            writeNumbers<UInt32, orc::LongVectorBatch>(orc_column, column, null_bytemap, rows_num);
            break;
        }
        case TypeIndex::Int64:
        {
            writeNumbers<Int64, orc::LongVectorBatch>(orc_column, column, null_bytemap, rows_num);
            break;
        }
        case TypeIndex::UInt64:
        {
            writeNumbers<UInt64,orc::LongVectorBatch>(orc_column, column, null_bytemap, rows_num);
            break;
        }
        case TypeIndex::Float32:
        {
            writeNumbers<Float32, orc::DoubleVectorBatch>(orc_column, column, null_bytemap, rows_num);
            break;
        }
        case TypeIndex::Float64:
        {
            writeNumbers<Float64, orc::DoubleVectorBatch>(orc_column, column, null_bytemap, rows_num);
            break;
        }
        case TypeIndex::FixedString: [[fallthrough]];
        case TypeIndex::String:
        {
            orc::StringVectorBatch * string_orc_column = dynamic_cast<orc::StringVectorBatch *>(orc_column);
            const auto & string_column = assert_cast<const ColumnString &>(column);
            string_orc_column->resize(rows_num);

            for (size_t i = 0; i != rows_num; ++i)
            {
                if (null_bytemap && (*null_bytemap)[i])
                {
                    string_orc_column->notNull[i] = 0;
                    continue;
                }
                const StringRef & string = string_column.getDataAt(i);
                string_orc_column->data[i] = const_cast<char *>(string.data);
                string_orc_column->length[i] = string.size;
            }
            string_orc_column->numElements = rows_num;
            break;
        }
        case TypeIndex::DateTime:
        {
            orc::TimestampVectorBatch * timestamp_orc_column = dynamic_cast<orc::TimestampVectorBatch *>(orc_column);
            const auto & timestamp_column = assert_cast<const ColumnUInt32 &>(column);
            timestamp_orc_column->resize(rows_num);

            for (size_t i = 0; i != rows_num; ++i)
            {
                if (null_bytemap && (*null_bytemap)[i])
                {
                    timestamp_orc_column->notNull[i] = 0;
                    continue;
                }
                timestamp_orc_column->data[i] = timestamp_column.getElement(i);
                timestamp_orc_column->nanoseconds[i] = 0;
            }
            timestamp_orc_column->numElements = rows_num;
            break;
        }
        case TypeIndex::DateTime64:
        {
            orc::TimestampVectorBatch * timestamp_orc_column = dynamic_cast<orc::TimestampVectorBatch *>(orc_column);
            const auto & timestamp_column = assert_cast<const DataTypeDateTime64::ColumnType &>(column);
            const auto * timestamp_type = assert_cast<const DataTypeDateTime64 *>(type.get());

            UInt32 scale = timestamp_type->getScale();
            timestamp_orc_column->resize(rows_num);

            for (size_t i = 0; i != rows_num; ++i)
            {
                if (null_bytemap && (*null_bytemap)[i])
                {
                    timestamp_orc_column->notNull[i] = 0;
                    continue;
                }
                UInt64 value = timestamp_column.getElement(i);
                timestamp_orc_column->data[i] = value / std::pow(10, scale);
                timestamp_orc_column->nanoseconds[i] = (value % UInt64(std::pow(10, scale))) * std::pow(10, 9 - scale);
            }
            timestamp_orc_column->numElements = rows_num;
            break;
        }
        case TypeIndex::Decimal32:;
        {
            writeDecimals<Decimal32, orc::Decimal64VectorBatch>(
                    orc_column,
                    column,
                    type,
                    null_bytemap,
                    rows_num,
                    [](Int32 value){ return value; });
            break;
        }
        case TypeIndex::Decimal64:
        {
            writeDecimals<Decimal64, orc::Decimal64VectorBatch>(
                    orc_column,
                    column,
                    type,
                    null_bytemap,
                    rows_num,
                    [](Int64 value){ return value; });
            break;
        }
        case TypeIndex::Decimal128:
        {
            writeDecimals<Decimal128, orc::Decimal128VectorBatch>(
                    orc_column,
                    column,
                    type,
                    null_bytemap,
                    rows_num,
                    [](Int128 value){ return orc::Int128(value >> 64, (value << 64) >> 64); });
            break;
        }
        case TypeIndex::Nullable:
        {
            const auto & nullable_column = assert_cast<const ColumnNullable &>(column);
            const PaddedPODArray<UInt8> & new_null_bytemap = assert_cast<const ColumnVector<UInt8> &>(*nullable_column.getNullMapColumnPtr()).getData();
            auto nested_type = removeNullable(type);
            writeColumn(orc_column, nullable_column.getNestedColumn(), nested_type, &new_null_bytemap, rows_num);
            break;
        }
        /* Doesn't work
        case TypeIndex::Array:
        {
            orc::ListVectorBatch * list_orc_column = dynamic_cast<orc::ListVectorBatch *>(orc_column);
            const auto & list_column = assert_cast<const ColumnArray &>(column);
            auto nested_type = assert_cast<const DataTypeArray &>(*type).getNestedType();
            const ColumnArray::Offsets & offsets = list_column.getOffsets();
            list_orc_column->resize(rows_num);
            list_orc_column->offsets[0] = 0;
            for (size_t i = 0; i != rows_num; ++i)
            {
                list_orc_column->offsets[i + 1] = offsets[i];
            }
            const IColumn & nested_column = list_column.getData();
            orc::ColumnVectorBatch * nested_orc_column = list_orc_column->elements.get();
            writeColumn(nested_orc_column, nested_column, nested_type, null_bytemap, nested_column.size());
            list_orc_column->numElements = rows_num;
            break;
        }
         */
        default:
            throw Exception("Type " + type->getName() + " is not supported for ORC output format", ErrorCodes::ILLEGAL_COLUMN);
    }
}

void ORCBlockOutputFormat::consume(Chunk chunk)
{
    size_t columns_num = chunk.getNumColumns();
    size_t rows_num = chunk.getNumRows();
    ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = writer->createRowBatch(rows_num);
    orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch.get());
    for (size_t i = 0; i != columns_num; ++i)
    {
        writeColumn(root->fields[i], *chunk.getColumns()[i], data_types[i], nullptr, rows_num);
    }
    root->numElements = rows_num;
    writer->add(*batch);
}

void ORCBlockOutputFormat::finalize()
{
    writer->close();
}

void registerOutputFormatProcessorORC(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("ORC", [](
            WriteBuffer & buf,
            const Block & sample,
            FormatFactory::WriteCallback,
            const FormatSettings & format_settings)
    {
        return std::make_shared<ORCBlockOutputFormat>(buf, sample, format_settings);
    });
}

}
