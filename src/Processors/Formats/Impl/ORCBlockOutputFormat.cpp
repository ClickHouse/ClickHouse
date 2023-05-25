#include <Processors/Formats/Impl/ORCBlockOutputFormat.h>

#if USE_ORC

#include <Common/assert_cast.h>
#include <Formats/FormatFactory.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnMap.h>

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

orc::CompressionKind getORCCompression(FormatSettings::ORCCompression method)
{
    if (method == FormatSettings::ORCCompression::NONE)
        return orc::CompressionKind::CompressionKind_NONE;

#if USE_SNAPPY
    if (method == FormatSettings::ORCCompression::SNAPPY)
        return orc::CompressionKind::CompressionKind_SNAPPY;
#endif

    if (method == FormatSettings::ORCCompression::ZSTD)
        return orc::CompressionKind::CompressionKind_ZSTD;

    if (method == FormatSettings::ORCCompression::LZ4)
        return orc::CompressionKind::CompressionKind_LZ4;

    if (method == FormatSettings::ORCCompression::ZLIB)
        return orc::CompressionKind::CompressionKind_ZLIB;

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported compression method");
}

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
    : IOutputFormat(header_, out_), format_settings{format_settings_}, output_stream(out_)
{
    for (const auto & type : header_.getDataTypes())
        data_types.push_back(recursiveRemoveLowCardinality(type));
}

std::unique_ptr<orc::Type> ORCBlockOutputFormat::getORCType(const DataTypePtr & type)
{
    switch (type->getTypeId())
    {
        case TypeIndex::UInt8:
        {
            if (isBool(type))
                return orc::createPrimitiveType(orc::TypeKind::BOOLEAN);
            return orc::createPrimitiveType(orc::TypeKind::BYTE);
        }
        case TypeIndex::Enum8: [[fallthrough]];
        case TypeIndex::Int8:
        {
            return orc::createPrimitiveType(orc::TypeKind::BYTE);
        }
        case TypeIndex::Enum16: [[fallthrough]];
        case TypeIndex::UInt16: [[fallthrough]];
        case TypeIndex::Int16:
        {
            return orc::createPrimitiveType(orc::TypeKind::SHORT);
        }
        case TypeIndex::UInt32: [[fallthrough]];
        case TypeIndex::IPv4: [[fallthrough]];
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
        case TypeIndex::Date32: [[fallthrough]];
        case TypeIndex::Date:
        {
            return orc::createPrimitiveType(orc::TypeKind::DATE);
        }
        case TypeIndex::DateTime: [[fallthrough]];
        case TypeIndex::DateTime64:
        {
            return orc::createPrimitiveType(orc::TypeKind::TIMESTAMP);
        }
        case TypeIndex::Int128: [[fallthrough]];
        case TypeIndex::UInt128: [[fallthrough]];
        case TypeIndex::Int256: [[fallthrough]];
        case TypeIndex::UInt256: [[fallthrough]];
        case TypeIndex::Decimal256:
            return orc::createPrimitiveType(orc::TypeKind::BINARY);
        case TypeIndex::FixedString: [[fallthrough]];
        case TypeIndex::String:
        {
            if (format_settings.orc.output_string_as_string)
                return orc::createPrimitiveType(orc::TypeKind::STRING);
            return orc::createPrimitiveType(orc::TypeKind::BINARY);
        }
        case TypeIndex::IPv6:
        {
            return orc::createPrimitiveType(orc::TypeKind::BINARY);
        }
        case TypeIndex::Nullable:
        {
            return getORCType(removeNullable(type));
        }
        case TypeIndex::Array:
        {
            const auto * array_type = assert_cast<const DataTypeArray *>(type.get());
            return orc::createListType(getORCType(array_type->getNestedType()));
        }
        case TypeIndex::Decimal32:
        {
            const auto * decimal_type = assert_cast<const DataTypeDecimal<Decimal32> *>(type.get());
            return orc::createDecimalType(decimal_type->getPrecision(), decimal_type->getScale());
        }
        case TypeIndex::Decimal64:
        {
            const auto * decimal_type = assert_cast<const DataTypeDecimal<Decimal64> *>(type.get());
            return orc::createDecimalType(decimal_type->getPrecision(), decimal_type->getScale());
        }
        case TypeIndex::Decimal128:
        {
            const auto * decimal_type = assert_cast<const DataTypeDecimal<Decimal128> *>(type.get());
            return orc::createDecimalType(decimal_type->getPrecision(), decimal_type->getScale());
        }
        case TypeIndex::Tuple:
        {
            const auto * tuple_type = assert_cast<const DataTypeTuple *>(type.get());
            const auto & nested_names = tuple_type->getElementNames();
            const auto & nested_types = tuple_type->getElements();
            auto struct_type = orc::createStructType();
            for (size_t i = 0; i < nested_types.size(); ++i)
                struct_type->addStructField(nested_names[i], getORCType(nested_types[i]));
            return struct_type;
        }
        case TypeIndex::Map:
        {
            const auto * map_type = assert_cast<const DataTypeMap *>(type.get());
            return orc::createMapType(
                getORCType(map_type->getKeyType()),
                getORCType(map_type->getValueType())
                );
        }
        default:
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Type {} is not supported for ORC output format", type->getName());
        }
    }
}

template <typename NumberType, typename NumberVectorBatch, typename ConvertFunc>
void ORCBlockOutputFormat::writeNumbers(
        orc::ColumnVectorBatch & orc_column,
        const IColumn & column,
        const PaddedPODArray<UInt8> * null_bytemap,
        ConvertFunc convert)
{
    NumberVectorBatch & number_orc_column = dynamic_cast<NumberVectorBatch &>(orc_column);
    const auto & number_column = assert_cast<const ColumnVector<NumberType> &>(column);
    number_orc_column.resize(number_column.size());

    for (size_t i = 0; i != number_column.size(); ++i)
    {
        if (null_bytemap && (*null_bytemap)[i])
        {
            number_orc_column.notNull[i] = 0;
            continue;
        }

        number_orc_column.notNull[i] = 1;
        number_orc_column.data[i] = convert(number_column.getElement(i));
    }
    number_orc_column.numElements = number_column.size();
}

template <typename Decimal, typename DecimalVectorBatch, typename ConvertFunc>
void ORCBlockOutputFormat::writeDecimals(
        orc::ColumnVectorBatch & orc_column,
        const IColumn & column,
        DataTypePtr & type,
        const PaddedPODArray<UInt8> * null_bytemap,
        ConvertFunc convert)
{
    DecimalVectorBatch & decimal_orc_column = dynamic_cast<DecimalVectorBatch &>(orc_column);
    const auto & decimal_column = assert_cast<const ColumnDecimal<Decimal> &>(column);
    const auto * decimal_type = assert_cast<const DataTypeDecimal<Decimal> *>(type.get());
    decimal_orc_column.precision = decimal_type->getPrecision();
    decimal_orc_column.scale = decimal_type->getScale();
    decimal_orc_column.resize(decimal_column.size());
    for (size_t i = 0; i != decimal_column.size(); ++i)
    {
        if (null_bytemap && (*null_bytemap)[i])
        {
            decimal_orc_column.notNull[i] = 0;
            continue;
        }

        decimal_orc_column.notNull[i] = 1;
        decimal_orc_column.values[i] = convert(decimal_column.getElement(i).value);
    }
    decimal_orc_column.numElements = decimal_column.size();
}

template <typename ColumnType>
void ORCBlockOutputFormat::writeStrings(
        orc::ColumnVectorBatch & orc_column,
        const IColumn & column,
        const PaddedPODArray<UInt8> * null_bytemap)
{
    orc::StringVectorBatch & string_orc_column = dynamic_cast<orc::StringVectorBatch &>(orc_column);
    const auto & string_column = assert_cast<const ColumnType &>(column);
    string_orc_column.resize(string_column.size());

    for (size_t i = 0; i != string_column.size(); ++i)
    {
        if (null_bytemap && (*null_bytemap)[i])
        {
            string_orc_column.notNull[i] = 0;
            continue;
        }

        string_orc_column.notNull[i] = 1;
        const std::string_view & string = string_column.getDataAt(i).toView();
        string_orc_column.data[i] = const_cast<char *>(string.data());
        string_orc_column.length[i] = string.size();
    }
    string_orc_column.numElements = string_column.size();
}

template <typename ColumnType, typename GetSecondsFunc, typename GetNanosecondsFunc>
void ORCBlockOutputFormat::writeDateTimes(
        orc::ColumnVectorBatch & orc_column,
        const IColumn & column,
        const PaddedPODArray<UInt8> * null_bytemap,
        GetSecondsFunc get_seconds,
        GetNanosecondsFunc get_nanoseconds)
{
    orc::TimestampVectorBatch & timestamp_orc_column = dynamic_cast<orc::TimestampVectorBatch &>(orc_column);
    const auto & timestamp_column = assert_cast<const ColumnType &>(column);
    timestamp_orc_column.resize(timestamp_column.size());

    for (size_t i = 0; i != timestamp_column.size(); ++i)
    {
        if (null_bytemap && (*null_bytemap)[i])
        {
            timestamp_orc_column.notNull[i] = 0;
            continue;
        }

        timestamp_orc_column.notNull[i] = 1;
        timestamp_orc_column.data[i] = static_cast<int64_t>(get_seconds(timestamp_column.getElement(i)));
        timestamp_orc_column.nanoseconds[i] = static_cast<int64_t>(get_nanoseconds(timestamp_column.getElement(i)));
    }
    timestamp_orc_column.numElements = timestamp_column.size();
}

void ORCBlockOutputFormat::writeColumn(
    orc::ColumnVectorBatch & orc_column,
    const IColumn & column,
    DataTypePtr & type,
    const PaddedPODArray<UInt8> * null_bytemap)
{
    orc_column.notNull.resize(column.size());
    if (null_bytemap)
        orc_column.hasNulls = true;

    switch (type->getTypeId())
    {
        case TypeIndex::Enum8: [[fallthrough]];
        case TypeIndex::Int8:
        {
            /// Note: Explicit cast to avoid clang-tidy error: 'signed char' to 'long' conversion; consider casting to 'unsigned char' first.
            writeNumbers<Int8, orc::LongVectorBatch>(orc_column, column, null_bytemap, [](const Int8 & value){ return static_cast<int64_t>(value); });
            break;
        }
        case TypeIndex::UInt8:
        {
            writeNumbers<UInt8, orc::LongVectorBatch>(orc_column, column, null_bytemap, [](const UInt8 & value){ return value; });
            break;
        }
        case TypeIndex::Enum16: [[fallthrough]];
        case TypeIndex::Int16:
        {
            writeNumbers<Int16, orc::LongVectorBatch>(orc_column, column, null_bytemap, [](const Int16 & value){ return value; });
            break;
        }
        case TypeIndex::Date: [[fallthrough]];
        case TypeIndex::UInt16:
        {
            writeNumbers<UInt16, orc::LongVectorBatch>(orc_column, column, null_bytemap, [](const UInt16 & value){ return value; });
            break;
        }
        case TypeIndex::Date32: [[fallthrough]];
        case TypeIndex::Int32:
        {
            writeNumbers<Int32, orc::LongVectorBatch>(orc_column, column, null_bytemap, [](const Int32 & value){ return value; });
            break;
        }
        case TypeIndex::UInt32:
        {
            writeNumbers<UInt32, orc::LongVectorBatch>(orc_column, column, null_bytemap, [](const UInt32 & value){ return value; });
            break;
        }
        case TypeIndex::IPv4:
        {
            writeNumbers<IPv4, orc::LongVectorBatch>(orc_column, column, null_bytemap, [](const IPv4 & value){ return value.toUnderType(); });
            break;
        }
        case TypeIndex::Int64:
        {
            writeNumbers<Int64, orc::LongVectorBatch>(orc_column, column, null_bytemap, [](const Int64 & value){ return value; });
            break;
        }
        case TypeIndex::UInt64:
        {
            writeNumbers<UInt64,orc::LongVectorBatch>(orc_column, column, null_bytemap, [](const UInt64 & value){ return value; });
            break;
        }
        case TypeIndex::Int128:
        {
            writeStrings<ColumnInt128>(orc_column, column, null_bytemap);
            break;
        }
        case TypeIndex::UInt128:
        {
            writeStrings<ColumnUInt128>(orc_column, column, null_bytemap);
            break;
        }
        case TypeIndex::Int256:
        {
            writeStrings<ColumnInt256>(orc_column, column, null_bytemap);
            break;
        }
        case TypeIndex::UInt256:
        {
            writeStrings<ColumnUInt256>(orc_column, column, null_bytemap);
            break;
        }
        case TypeIndex::Float32:
        {
            writeNumbers<Float32, orc::DoubleVectorBatch>(orc_column, column, null_bytemap, [](const Float32 & value){ return value; });
            break;
        }
        case TypeIndex::Float64:
        {
            writeNumbers<Float64, orc::DoubleVectorBatch>(orc_column, column, null_bytemap, [](const Float64 & value){ return value; });
            break;
        }
        case TypeIndex::FixedString:
        {
            writeStrings<ColumnFixedString>(orc_column, column, null_bytemap);
            break;
        }
        case TypeIndex::String:
        {
            writeStrings<ColumnString>(orc_column, column, null_bytemap);
            break;
        }
        case TypeIndex::IPv6:
        {
            writeStrings<ColumnIPv6>(orc_column, column, null_bytemap);
            break;
        }
        case TypeIndex::DateTime:
        {
            writeDateTimes<ColumnUInt32>(
                    orc_column,
                    column, null_bytemap,
                    [](UInt32 value){ return value; },
                    [](UInt32){ return 0; });
            break;
        }
        case TypeIndex::DateTime64:
        {
            const auto * timestamp_type = assert_cast<const DataTypeDateTime64 *>(type.get());
            UInt32 scale = timestamp_type->getScale();
            writeDateTimes<DataTypeDateTime64::ColumnType>(
                    orc_column,
                    column, null_bytemap,
                    [scale](UInt64 value){ return value / std::pow(10, scale); },
                    [scale](UInt64 value){ return (value % UInt64(std::pow(10, scale))) * std::pow(10, 9 - scale); });
            break;
        }
        case TypeIndex::Decimal32:;
        {
            writeDecimals<Decimal32, orc::Decimal64VectorBatch>(
                    orc_column,
                    column,
                    type,
                    null_bytemap,
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
                    [](Int128 value){ return orc::Int128(value >> 64, (value << 64) >> 64); });
            break;
        }
        case TypeIndex::Decimal256:
        {
            writeStrings<ColumnDecimal<Decimal256>>(orc_column, column, null_bytemap);
            break;
        }
        case TypeIndex::Nullable:
        {
            const auto & nullable_column = assert_cast<const ColumnNullable &>(column);
            const PaddedPODArray<UInt8> & new_null_bytemap = assert_cast<const ColumnVector<UInt8> &>(*nullable_column.getNullMapColumnPtr()).getData();
            auto nested_type = removeNullable(type);
            writeColumn(orc_column, nullable_column.getNestedColumn(), nested_type, &new_null_bytemap);
            break;
        }
        case TypeIndex::Array:
        {
            orc::ListVectorBatch & list_orc_column = dynamic_cast<orc::ListVectorBatch &>(orc_column);
            const auto & list_column = assert_cast<const ColumnArray &>(column);
            auto nested_type = assert_cast<const DataTypeArray &>(*type).getNestedType();
            const ColumnArray::Offsets & offsets = list_column.getOffsets();

            size_t column_size = list_column.size();
            list_orc_column.resize(column_size);

            /// The length of list i in ListVectorBatch is offsets[i+1] - offsets[i].
            list_orc_column.offsets[0] = 0;
            for (size_t i = 0; i != column_size; ++i)
            {
                list_orc_column.offsets[i + 1] = offsets[i];
                list_orc_column.notNull[i] = 1;
            }

            orc::ColumnVectorBatch & nested_orc_column = *list_orc_column.elements;
            writeColumn(nested_orc_column, list_column.getData(), nested_type, null_bytemap);
            list_orc_column.numElements = column_size;
            break;
        }
        case TypeIndex::Tuple:
        {
            orc::StructVectorBatch & struct_orc_column = dynamic_cast<orc::StructVectorBatch &>(orc_column);
            const auto & tuple_column = assert_cast<const ColumnTuple &>(column);
            auto nested_types = assert_cast<const DataTypeTuple *>(type.get())->getElements();
            for (size_t i = 0; i != tuple_column.size(); ++i)
                struct_orc_column.notNull[i] = 1;
            for (size_t i = 0; i != tuple_column.tupleSize(); ++i)
                writeColumn(*struct_orc_column.fields[i], tuple_column.getColumn(i), nested_types[i], null_bytemap);
            break;
        }
        case TypeIndex::Map:
        {
            orc::MapVectorBatch & map_orc_column = dynamic_cast<orc::MapVectorBatch &>(orc_column);
            const auto & list_column = assert_cast<const ColumnMap &>(column).getNestedColumn();
            const auto & map_type = assert_cast<const DataTypeMap &>(*type);
            const ColumnArray::Offsets & offsets = list_column.getOffsets();

            size_t column_size = list_column.size();

            map_orc_column.resize(list_column.size());
            /// The length of list i in ListVectorBatch is offsets[i+1] - offsets[i].
            map_orc_column.offsets[0] = 0;
            for (size_t i = 0; i != column_size; ++i)
            {
                map_orc_column.offsets[i + 1] = offsets[i];
                map_orc_column.notNull[i] = 1;
            }
            const auto nested_columns = assert_cast<const ColumnTuple *>(list_column.getDataPtr().get())->getColumns();

            orc::ColumnVectorBatch & keys_orc_column = *map_orc_column.keys;
            auto key_type = map_type.getKeyType();
            writeColumn(keys_orc_column, *nested_columns[0], key_type, null_bytemap);

            orc::ColumnVectorBatch & values_orc_column = *map_orc_column.elements;
            auto value_type = map_type.getValueType();
            writeColumn(values_orc_column, *nested_columns[1], value_type, null_bytemap);

            map_orc_column.numElements = column_size;
            break;
        }
        default:
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Type {} is not supported for ORC output format", type->getName());
    }
}

size_t ORCBlockOutputFormat::getColumnSize(const IColumn & column, DataTypePtr & type)
{
    if (type->getTypeId() == TypeIndex::Array)
    {
        auto nested_type = assert_cast<const DataTypeArray &>(*type).getNestedType();
        const IColumn & nested_column = assert_cast<const ColumnArray &>(column).getData();
        return std::max(column.size(), getColumnSize(nested_column, nested_type));
    }

    return column.size();
}

size_t ORCBlockOutputFormat::getMaxColumnSize(Chunk & chunk)
{
    size_t columns_num = chunk.getNumColumns();
    size_t max_column_size = 0;
    for (size_t i = 0; i != columns_num; ++i)
        max_column_size = std::max(max_column_size, getColumnSize(*chunk.getColumns()[i], data_types[i]));
    return max_column_size;
}

void ORCBlockOutputFormat::consume(Chunk chunk)
{
    if (!writer)
        prepareWriter();

    size_t columns_num = chunk.getNumColumns();
    size_t rows_num = chunk.getNumRows();

    /// getMaxColumnSize is needed to write arrays.
    /// The size of the batch must be no less than total amount of array elements
    /// and no less than the number of rows (ORC writes a null bit for every row).
    std::unique_ptr<orc::ColumnVectorBatch> batch = writer->createRowBatch(getMaxColumnSize(chunk));
    orc::StructVectorBatch & root = dynamic_cast<orc::StructVectorBatch &>(*batch);

    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = recursiveRemoveLowCardinality(column);

    for (size_t i = 0; i != columns_num; ++i)
        writeColumn(*root.fields[i], *columns[i], data_types[i], nullptr);

    root.numElements = rows_num;
    writer->add(*batch);
}

void ORCBlockOutputFormat::finalizeImpl()
{
    if (!writer)
        prepareWriter();

    writer->close();
}

void ORCBlockOutputFormat::resetFormatterImpl()
{
    writer.reset();
}

void ORCBlockOutputFormat::prepareWriter()
{
    const Block & header = getPort(PortKind::Main).getHeader();
    schema = orc::createStructType();
    options.setCompression(getORCCompression(format_settings.orc.output_compression_method));
    size_t columns_count = header.columns();
    for (size_t i = 0; i != columns_count; ++i)
        schema->addStructField(header.safeGetByPosition(i).name, getORCType(recursiveRemoveLowCardinality(data_types[i])));
    writer = orc::createWriter(*schema, &output_stream, options);
}

void registerOutputFormatORC(FormatFactory & factory)
{
    factory.registerOutputFormat("ORC", [](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & format_settings)
    {
        return std::make_shared<ORCBlockOutputFormat>(buf, sample, format_settings);
    });
    factory.markFormatHasNoAppendSupport("ORC");
    factory.markOutputFormatPrefersLargeBlocks("ORC");
}

}

#else

namespace DB
{
    class FormatFactory;
    void registerOutputFormatORC(FormatFactory &)
    {
    }
}

#endif
