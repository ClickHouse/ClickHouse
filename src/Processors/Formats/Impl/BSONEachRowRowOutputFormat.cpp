#include <Processors/Formats/Impl/BSONEachRowRowOutputFormat.h>

#include <Formats/FormatFactory.h>
#include <Formats/BSONTypes.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnDecimal.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>

#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>

#include <Processors/Port.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
}

/// In BSON all names should be valid UTF8 sequences
static String toValidUTF8String(const String & name, const FormatSettings & settings)
{
    WriteBufferFromOwnString buf;
    {
        WriteBufferValidUTF8 validating_buf(buf);
        writeJSONString(name, validating_buf, settings);
    }
    /// Return value without quotes
    return buf.str().substr(1, buf.str().size() - 2);
}

BSONEachRowRowOutputFormat::BSONEachRowRowOutputFormat(
    WriteBuffer & out_, const Block & header_, const FormatSettings & settings_)
    : IRowOutputFormat(header_, out_), settings(settings_)
{
    const auto & sample = getPort(PortKind::Main).getHeader();
    fields.reserve(sample.columns());
    for (const auto & field : sample.getNamesAndTypes())
        fields.emplace_back(toValidUTF8String(field.name, settings), field.type);
}

static void writeBSONSize(size_t size, WriteBuffer & buf)
{
    if (size > MAX_BSON_SIZE)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Too large document/value size: {}. Maximum allowed size: {}.", size, MAX_BSON_SIZE);

    writeBinaryLittleEndian(BSONSizeT(size), buf);
}

template <typename Type>
static void writeBSONType(Type type, WriteBuffer & buf)
{
    UInt8 value = UInt8(type);
    writeBinary(value, buf);
}

static void writeBSONTypeAndKeyName(BSONType type, const String & name, WriteBuffer & buf)
{
    writeBSONType(type, buf);
    writeString(name, buf);
    writeChar(0x00, buf);
}

template <typename ColumnType, typename ValueType>
static void writeBSONNumber(BSONType type, const IColumn & column, size_t row_num, const String & name, WriteBuffer & buf)
{
    writeBSONTypeAndKeyName(type, name, buf);
    writeBinaryLittleEndian(ValueType(assert_cast<const ColumnType &>(column).getElement(row_num)), buf);
}

template <typename StringColumnType>
static void writeBSONString(const IColumn & column, size_t row_num, const String & name, WriteBuffer & buf, bool as_bson_string)
{
    const auto & string_column = assert_cast<const StringColumnType &>(column);
    StringRef data = string_column.getDataAt(row_num);
    if (as_bson_string)
    {
        writeBSONTypeAndKeyName(BSONType::STRING, name, buf);
        writeBSONSize(data.size + 1, buf);
        writeString(data, buf);
        writeChar(0x00, buf);
    }
    else
    {
        writeBSONTypeAndKeyName(BSONType::BINARY, name, buf);
        writeBSONSize(data.size, buf);
        writeBSONType(BSONBinarySubtype::BINARY, buf);
        writeString(data, buf);
    }
}

template <class ColumnType>
static void writeBSONBigInteger(const IColumn & column, size_t row_num, const String & name, WriteBuffer & buf)
{
    writeBSONTypeAndKeyName(BSONType::BINARY, name, buf);
    writeBSONSize(sizeof(typename ColumnType::ValueType), buf);
    writeBSONType(BSONBinarySubtype::BINARY, buf);
    writeBinaryLittleEndian(assert_cast<const ColumnType &>(column).getElement(row_num), buf);
}

size_t BSONEachRowRowOutputFormat::countBSONFieldSize(const IColumn & column, const DataTypePtr & data_type, size_t row_num, const String & name, const String & path, std::unordered_map<String, size_t> & nested_document_sizes)
{
    size_t size = 1; // Field type
    size += name.size() + 1; // Field name and \0
    switch (data_type->getTypeId())
    {
        case TypeIndex::Int8: [[fallthrough]];
        case TypeIndex::Int16: [[fallthrough]];
        case TypeIndex::UInt16: [[fallthrough]];
        case TypeIndex::Date: [[fallthrough]];
        case TypeIndex::Date32: [[fallthrough]];
        case TypeIndex::Decimal32: [[fallthrough]];
        case TypeIndex::IPv4: [[fallthrough]];
        case TypeIndex::Enum8: [[fallthrough]];
        case TypeIndex::Enum16: [[fallthrough]];
        case TypeIndex::Int32:
        {
            return size + sizeof(Int32);
        }
        case TypeIndex::UInt8:
        {
            if (isBool(data_type))
                return size + 1;

            return size + sizeof(Int32);
        }
        case TypeIndex::Float32: [[fallthrough]];
        case TypeIndex::Float64: [[fallthrough]];
        case TypeIndex::UInt32: [[fallthrough]];
        case TypeIndex::Int64: [[fallthrough]];
        case TypeIndex::UInt64: [[fallthrough]];
        case TypeIndex::DateTime: [[fallthrough]];
        case TypeIndex::Decimal64: [[fallthrough]];
        case TypeIndex::DateTime64:
        {
            return size + sizeof(UInt64);
        }
        case TypeIndex::Int128: [[fallthrough]];
        case TypeIndex::UInt128: [[fallthrough]];
        case TypeIndex::Decimal128:
        {
            return size + sizeof(BSONSizeT) + 1 + sizeof(UInt128); // Size of a binary + binary subtype + 16 bytes of value
        }
        case TypeIndex::Int256: [[fallthrough]];
        case TypeIndex::UInt256: [[fallthrough]];
        case TypeIndex::Decimal256:
        {
            return size + sizeof(BSONSizeT) + 1 + sizeof(UInt256); // Size of a binary + binary subtype + 32 bytes of value
        }
        case TypeIndex::String:
        {
            const auto & string_column = assert_cast<const ColumnString &>(column);
            return size + sizeof(BSONSizeT) + string_column.getDataAt(row_num).size + 1; // Size of data + data + \0 or BSON subtype (in case of BSON binary)
        }
        case TypeIndex::FixedString:
        {
            const auto & string_column = assert_cast<const ColumnFixedString &>(column);
            return size + sizeof(BSONSizeT) + string_column.getN() + 1; // Size of data + data + \0 or BSON subtype (in case of BSON binary)
        }
        case TypeIndex::IPv6:
        {
            return size + sizeof(BSONSizeT) + 1 + sizeof(IPv6); // Size of data + BSON binary subtype + 16 bytes of value
        }
        case TypeIndex::UUID:
        {
            return size + sizeof(BSONSizeT) + 1 + sizeof(UUID); // Size of data + BSON binary subtype + 16 bytes of value
        }
        case TypeIndex::LowCardinality:
        {
            const auto & lc_column = assert_cast<const ColumnLowCardinality &>(column);
            auto dict_type = assert_cast<const DataTypeLowCardinality *>(data_type.get())->getDictionaryType();
            auto dict_column = lc_column.getDictionary().getNestedColumn();
            size_t index = lc_column.getIndexAt(row_num);
            return countBSONFieldSize(*dict_column, dict_type, index, name, path, nested_document_sizes);
        }
        case TypeIndex::Nullable:
        {
            auto nested_type = removeNullable(data_type);
            const ColumnNullable & column_nullable = assert_cast<const ColumnNullable &>(column);
            if (column_nullable.isNullAt(row_num))
                return size; /// Null has no value, just type
            return countBSONFieldSize(column_nullable.getNestedColumn(), nested_type, row_num, name, path, nested_document_sizes);
        }
        case TypeIndex::Array:
        {
            size_t document_size = sizeof(BSONSizeT); // Size of a document

            const auto & nested_type = assert_cast<const DataTypeArray *>(data_type.get())->getNestedType();
            const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
            const IColumn & nested_column = column_array.getData();
            const ColumnArray::Offsets & offsets = column_array.getOffsets();
            size_t offset = offsets[row_num - 1];
            size_t array_size = offsets[row_num] - offset;

            String current_path = path + "." + name;
            for (size_t i = 0; i < array_size; ++i)
                document_size += countBSONFieldSize(nested_column, nested_type, offset + i, std::to_string(i), current_path, nested_document_sizes); // Add size of each value from array

            document_size += sizeof(BSON_DOCUMENT_END); // Add final \0
            nested_document_sizes[current_path] = document_size;
            return size + document_size;
        }
        case TypeIndex::Tuple:
        {
            size_t document_size = sizeof(BSONSizeT); // Size of a document

            const auto * tuple_type = assert_cast<const DataTypeTuple *>(data_type.get());
            const auto & nested_types = tuple_type->getElements();
            const auto & nested_names = tuple_type->getElementNames();
            const auto & tuple_column = assert_cast<const ColumnTuple &>(column);
            const auto & nested_columns = tuple_column.getColumns();

            String current_path = path + "." + name;
            for (size_t i = 0; i < nested_columns.size(); ++i)
            {
                String key_name = toValidUTF8String(nested_names[i], settings);
                document_size += countBSONFieldSize(*nested_columns[i], nested_types[i], row_num, key_name, current_path, nested_document_sizes); // Add size of each value from tuple
            }

            document_size += sizeof(BSON_DOCUMENT_END); // Add final \0
            nested_document_sizes[current_path] = document_size;
            return size + document_size;
        }
        case TypeIndex::Map:
        {
            size_t document_size = sizeof(BSONSizeT); // Size of a document

            const auto & map_type = assert_cast<const DataTypeMap &>(*data_type);
            const auto & key_type = map_type.getKeyType();
            const auto & value_type = map_type.getValueType();

            const auto & map_column = assert_cast<const ColumnMap &>(column);
            const auto & nested_column = map_column.getNestedColumn();
            const auto & key_value_columns = map_column.getNestedData().getColumns();
            const auto & key_column = key_value_columns[0];
            const auto & value_column = key_value_columns[1];
            const auto & offsets = nested_column.getOffsets();
            size_t offset = offsets[row_num - 1];
            size_t map_size = offsets[row_num] - offset;

            WriteBufferFromOwnString buf;
            String current_path = path + "." + name;
            for (size_t i = 0; i < map_size; ++i)
            {
                key_type->getDefaultSerialization()->serializeText(*key_column, offset + i, buf, settings);
                auto s = countBSONFieldSize(*value_column, value_type, offset + i, toValidUTF8String(buf.str(), settings), current_path, nested_document_sizes);
                document_size += s;
                buf.restart();
            }

            document_size += sizeof(BSON_DOCUMENT_END); // Add final \0
            nested_document_sizes[current_path] = document_size;
            return size + document_size;
        }
        default:
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Type {} is not supported in BSON output format", data_type->getName());
    }
}

void BSONEachRowRowOutputFormat::serializeField(const IColumn & column, const DataTypePtr & data_type, size_t row_num, const String & name, const String & path, std::unordered_map<String, size_t> & nested_document_sizes)
{
    switch (data_type->getTypeId())
    {
        case TypeIndex::Float32:
        {
            writeBSONNumber<ColumnFloat32, double>(BSONType::DOUBLE, column, row_num, name, out);
            break;
        }
        case TypeIndex::Float64:
        {
            writeBSONNumber<ColumnFloat64, double>(BSONType::DOUBLE, column, row_num, name, out);
            break;
        }
        case TypeIndex::Enum8: [[fallthrough]];
        case TypeIndex::Int8:
        {
            writeBSONNumber<ColumnInt8, Int32>(BSONType::INT32, column, row_num, name, out);
            break;
        }
        case TypeIndex::UInt8:
        {
            if (isBool(data_type))
                writeBSONNumber<ColumnUInt8, bool>(BSONType::BOOL, column, row_num, name, out);
            else
                writeBSONNumber<ColumnUInt8, Int32>(BSONType::INT32, column, row_num, name, out);
            break;
        }
        case TypeIndex::Enum16: [[fallthrough]];
        case TypeIndex::Int16:
        {
            writeBSONNumber<ColumnInt16, Int32>(BSONType::INT32, column, row_num, name, out);
            break;
        }
        case TypeIndex::Date: [[fallthrough]];
        case TypeIndex::UInt16:
        {
            writeBSONNumber<ColumnUInt16, Int32>(BSONType::INT32, column, row_num, name, out);
            break;
        }
        case TypeIndex::Date32: [[fallthrough]];
        case TypeIndex::Int32:
        {
            writeBSONNumber<ColumnInt32, Int32>(BSONType::INT32, column, row_num, name, out);
            break;
        }
        case TypeIndex::DateTime: [[fallthrough]];
        case TypeIndex::UInt32:
        {
            writeBSONNumber<ColumnUInt32, Int64>(BSONType::INT64, column, row_num, name, out);
            break;
        }
        case TypeIndex::Int64:
        {
            writeBSONNumber<ColumnInt64, Int64>(BSONType::INT64, column, row_num, name, out);
            break;
        }
        case TypeIndex::UInt64:
        {
            writeBSONNumber<ColumnUInt64, UInt64>(BSONType::INT64, column, row_num, name, out);
            break;
        }
        case TypeIndex::Int128:
        {
            writeBSONBigInteger<ColumnInt128>(column, row_num, name, out);
            break;
        }
        case TypeIndex::UInt128:
        {
            writeBSONBigInteger<ColumnUInt128>(column, row_num, name, out);
            break;
        }
        case TypeIndex::Int256:
        {
            writeBSONBigInteger<ColumnInt256>(column, row_num, name, out);
            break;
        }
        case TypeIndex::UInt256:
        {
            writeBSONBigInteger<ColumnUInt256>(column, row_num, name, out);
            break;
        }
        case TypeIndex::Decimal32:
        {
            writeBSONNumber<ColumnDecimal<Decimal32>, Decimal32>(BSONType::INT32, column, row_num, name, out);
            break;
        }
        case TypeIndex::DateTime64:
        {
            writeBSONNumber<ColumnDecimal<DateTime64>, Decimal64>(BSONType::DATETIME, column, row_num, name, out);
            break;
        }
        case TypeIndex::Decimal64:
        {
            writeBSONNumber<ColumnDecimal<Decimal64>, Decimal64>(BSONType::INT64, column, row_num, name, out);
            break;
        }
        case TypeIndex::Decimal128:
        {
            writeBSONBigInteger<ColumnDecimal<Decimal128>>(column, row_num, name, out);
            break;
        }
        case TypeIndex::Decimal256:
        {
            writeBSONBigInteger<ColumnDecimal<Decimal256>>(column, row_num, name, out);
            break;
        }
        case TypeIndex::String:
        {
            writeBSONString<ColumnString>(column, row_num, name, out, settings.bson.output_string_as_string);
            break;
        }
        case TypeIndex::FixedString:
        {
            writeBSONString<ColumnFixedString>(column, row_num, name, out, settings.bson.output_string_as_string);
            break;
        }
        case TypeIndex::IPv4:
        {
            writeBSONNumber<ColumnIPv4, Int32>(BSONType::INT32, column, row_num, name, out);
            break;
        }
        case TypeIndex::IPv6:
        {
            writeBSONTypeAndKeyName(BSONType::BINARY, name, out);
            writeBSONSize(sizeof(IPv6), out);
            writeBSONType(BSONBinarySubtype::BINARY, out);
            writeBinary(assert_cast<const ColumnIPv6 &>(column).getElement(row_num), out);
            break;
        }
        case TypeIndex::UUID:
        {
            writeBSONTypeAndKeyName(BSONType::BINARY, name, out);
            writeBSONSize(sizeof(UUID), out);
            writeBSONType(BSONBinarySubtype::UUID, out);
            writeBinaryLittleEndian(assert_cast<const ColumnUUID &>(column).getElement(row_num), out);
            break;
        }
        case TypeIndex::LowCardinality:
        {
            const auto & lc_column = assert_cast<const ColumnLowCardinality &>(column);
            auto dict_type = assert_cast<const DataTypeLowCardinality *>(data_type.get())->getDictionaryType();
            auto dict_column = lc_column.getDictionary().getNestedColumn();
            size_t index = lc_column.getIndexAt(row_num);
            serializeField(*dict_column, dict_type, index, name, path, nested_document_sizes);
            break;
        }
        case TypeIndex::Nullable:
        {
            auto nested_type = removeNullable(data_type);
            const ColumnNullable & column_nullable = assert_cast<const ColumnNullable &>(column);
            if (!column_nullable.isNullAt(row_num))
                serializeField(column_nullable.getNestedColumn(), nested_type, row_num, name, path, nested_document_sizes);
            else
                writeBSONTypeAndKeyName(BSONType::NULL_VALUE, name, out);
            break;
        }
        case TypeIndex::Array:
        {
            const auto & nested_type = assert_cast<const DataTypeArray *>(data_type.get())->getNestedType();
            const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
            const IColumn & nested_column = column_array.getData();
            const ColumnArray::Offsets & offsets = column_array.getOffsets();
            size_t offset = offsets[row_num - 1];
            size_t array_size = offsets[row_num] - offset;

            writeBSONTypeAndKeyName(BSONType::ARRAY, name, out);

            String current_path = path + "." + name;
            size_t document_size = nested_document_sizes[current_path];
            writeBSONSize(document_size, out);

            for (size_t i = 0; i < array_size; ++i)
                serializeField(nested_column, nested_type, offset + i, std::to_string(i), current_path, nested_document_sizes);

            writeChar(BSON_DOCUMENT_END, out);
            break;
        }
        case TypeIndex::Tuple:
        {
            const auto * tuple_type = assert_cast<const DataTypeTuple *>(data_type.get());
            const auto & nested_types = tuple_type->getElements();
            const auto & nested_names = tuple_type->getElementNames();
            const auto & tuple_column = assert_cast<const ColumnTuple &>(column);
            const auto & nested_columns = tuple_column.getColumns();

            BSONType bson_type =  tuple_type->haveExplicitNames() ? BSONType::DOCUMENT : BSONType::ARRAY;
            writeBSONTypeAndKeyName(bson_type, name, out);

            String current_path = path + "." + name;
            size_t document_size = nested_document_sizes[current_path];
            writeBSONSize(document_size, out);

            for (size_t i = 0; i < nested_columns.size(); ++i)
                serializeField(*nested_columns[i], nested_types[i], row_num, toValidUTF8String(nested_names[i], settings), current_path, nested_document_sizes);

            writeChar(BSON_DOCUMENT_END, out);
            break;
        }
        case TypeIndex::Map:
        {
            const auto & map_type = assert_cast<const DataTypeMap &>(*data_type);
            const auto & key_type = map_type.getKeyType();
            const auto & value_type = map_type.getValueType();

            const auto & map_column = assert_cast<const ColumnMap &>(column);
            const auto & nested_column = map_column.getNestedColumn();
            const auto & key_value_columns = map_column.getNestedData().getColumns();
            const auto & key_column = key_value_columns[0];
            const auto & value_column = key_value_columns[1];
            const auto & offsets = nested_column.getOffsets();
            size_t offset = offsets[row_num - 1];
            size_t map_size = offsets[row_num] - offset;

            writeBSONTypeAndKeyName(BSONType::DOCUMENT, name, out);

            String current_path = path + "." + name;
            size_t document_size = nested_document_sizes[current_path];
            writeBSONSize(document_size, out);

            WriteBufferFromOwnString buf;
            for (size_t i = 0; i < map_size; ++i)
            {
                key_type->getDefaultSerialization()->serializeText(*key_column, offset + i, buf, settings);
                serializeField(*value_column, value_type, offset + i, toValidUTF8String(buf.str(), settings), current_path, nested_document_sizes);
                buf.restart();
            }

            writeChar(BSON_DOCUMENT_END, out);
            break;
        }
        default:
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Type {} is not supported in BSON output format", data_type->getName());
    }
}

void BSONEachRowRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    /// We should calculate and write document size before its content
    size_t document_size = sizeof(BSONSizeT);
    /// Remember calculated sizes for nested documents (map document path -> size), so we won't need
    /// to recalculate it while serializing.
    std::unordered_map<String, size_t> nested_document_sizes;
    for (size_t i = 0; i != columns.size(); ++i)
        document_size += countBSONFieldSize(*columns[i], fields[i].type, row_num, fields[i].name, "$", nested_document_sizes);
    document_size += sizeof(BSON_DOCUMENT_END);

    size_t document_start = out.count();
    writeBSONSize(document_size, out);

    for (size_t i = 0; i != columns.size(); ++i)
        serializeField(*columns[i], fields[i].type, row_num, fields[i].name, "$", nested_document_sizes);

    writeChar(BSON_DOCUMENT_END, out);

    size_t actual_document_size = out.count() - document_start;
    if (actual_document_size != document_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The actual size of the BSON document does not match the estimated size: {} != {}",
            actual_document_size,
            document_size);
}

void registerOutputFormatBSONEachRow(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "BSONEachRow",
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & _format_settings)
        { return std::make_shared<BSONEachRowRowOutputFormat>(buf, sample, _format_settings); });
    factory.markOutputFormatSupportsParallelFormatting("BSONEachRow");
    factory.markOutputFormatNotTTYFriendly("BSONEachRow");
    factory.setContentType("BSONEachRow", "application/octet-stream");
}

}
