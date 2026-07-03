#include <Core/MySQL/PacketsProtocolText.h>

#include <Core/MySQL/MySQLUtils.h>
#include <Columns/ColumnNullable.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/assert_cast.h>
#include <Core/MySQL/IMySQLWritePacket.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesDecimal.h>


namespace DB
{

namespace MySQLProtocol
{

namespace ProtocolText
{

ResultSetRow::ResultSetRow(const Serializations & serializations, const DataTypes & data_types, const Columns & columns_, size_t row_num_)
    : columns(columns_), row_num(row_num_)
{
    FormatSettings format_settings = {.bool_true_representation = "1", .bool_false_representation = "0"};

    for (size_t i = 0; i < columns.size(); ++i)
    {
        DataTypePtr data_type = removeLowCardinalityAndNullable(data_types[i]);
        TypeIndex type_index = data_type->getTypeId();
        if (columns[i]->isNullAt(row_num))
        {
            payload_size += 1;
            serialized.emplace_back("\xfb");
        }
        // Arbitrary precision DateTime64 needs to be forced into precision 6, as it is the maximum that MySQL supports
        else if (type_index == TypeIndex::DateTime64)
        {
            WriteBufferFromOwnString ostr;
            ColumnPtr col = columns[i]->convertToFullIfNeeded();
            if (col->isNullable())
                col = assert_cast<const ColumnNullable &>(*col).getNestedColumnPtr();
            auto components = MySQLUtils::getNormalizedDateTime64Components(data_type, col, row_num);
            writeDateTimeText<'-', ':', ' '>(LocalDateTime(components.whole, DateLUT::instance(getDateTimeTimezone(*data_type))), ostr);
            ostr.write('.');
            writeDateTime64FractionalText<DateTime64>(components.fractional, 6, ostr);
            payload_size += getLengthEncodedStringSize(ostr.str());
            serialized.push_back(std::move(ostr.str()));
        }
        else
        {
            WriteBufferFromOwnString ostr;
            serializations[i]->serializeText(*columns[i], row_num, ostr, format_settings);
            payload_size += getLengthEncodedStringSize(ostr.str());
            serialized.push_back(std::move(ostr.str()));
        }
    }
}

size_t ResultSetRow::getPayloadSize() const
{
    return payload_size;
}

void ResultSetRow::writePayloadImpl(WriteBuffer & buffer) const
{
    for (size_t i = 0; i < columns.size(); ++i)
        if (columns[i]->isNullAt(row_num))
            buffer.write(serialized[i].data(), 1);
        else
            writeLengthEncodedString(serialized[i], buffer);
}

void ComFieldList::readPayloadImpl(ReadBuffer & payload)
{
    // Command byte has been already read from payload.
    readNullTerminated(table, payload);
    readStringUntilEOF(field_wildcard, payload);
}

ColumnDefinition::ColumnDefinition()
    : character_set(0x00), column_length(0), column_type(MYSQL_TYPE_DECIMAL), flags(0x00)
{
}

ColumnDefinition::ColumnDefinition(
    String schema_, String table_, String org_table_, String name_, String org_name_, uint16_t character_set_, uint32_t column_length_,
    ColumnType column_type_, uint16_t flags_, uint8_t decimals_, bool with_defaults_)
    : schema(std::move(schema_)), table(std::move(table_)), org_table(std::move(org_table_)), name(std::move(name_)),
      org_name(std::move(org_name_)), character_set(character_set_), column_length(column_length_), column_type(column_type_),
      flags(flags_), decimals(decimals_), is_comm_field_list_response(with_defaults_)
{
}

ColumnDefinition::ColumnDefinition(
    String name_, uint16_t character_set_, uint32_t column_length_, ColumnType column_type_, uint16_t flags_, uint8_t decimals_)
    : ColumnDefinition("", "", "", std::move(name_), "", character_set_, column_length_, column_type_, flags_, decimals_)
{
}

size_t ColumnDefinition::getPayloadSize() const
{
    return 12 +
           getLengthEncodedStringSize("def") +
           getLengthEncodedStringSize(schema) +
           getLengthEncodedStringSize(table) +
           getLengthEncodedStringSize(org_table) +
           getLengthEncodedStringSize(name) +
           getLengthEncodedStringSize(org_name) +
           getLengthEncodedNumberSize(next_length) +
           is_comm_field_list_response;
}

void ColumnDefinition::readPayloadImpl(ReadBuffer & payload)
{
    String def;
    readLengthEncodedString(def, payload);
    assert(def == "def");
    readLengthEncodedString(schema, payload);
    readLengthEncodedString(table, payload);
    readLengthEncodedString(org_table, payload);
    readLengthEncodedString(name, payload);
    readLengthEncodedString(org_name, payload);
    next_length = readLengthEncodedNumber(payload);
    payload.readStrict(reinterpret_cast<char *>(&character_set), 2);
    payload.readStrict(reinterpret_cast<char *>(&column_length), 4);
    payload.readStrict(reinterpret_cast<char *>(&column_type), 1);
    payload.readStrict(reinterpret_cast<char *>(&flags), 2);
    payload.readStrict(reinterpret_cast<char *>(&decimals), 1);
    payload.ignore(2);
}

void ColumnDefinition::writePayloadImpl(WriteBuffer & buffer) const
{
    writeLengthEncodedString(std::string("def"), buffer); /// always "def"
    writeLengthEncodedString(schema, buffer);
    writeLengthEncodedString(table, buffer);
    writeLengthEncodedString(org_table, buffer);
    writeLengthEncodedString(name, buffer);
    writeLengthEncodedString(org_name, buffer);
    writeLengthEncodedNumber(next_length, buffer);
    buffer.write(reinterpret_cast<const char *>(&character_set), 2);
    buffer.write(reinterpret_cast<const char *>(&column_length), 4);
    buffer.write(reinterpret_cast<const char *>(&column_type), 1);
    buffer.write(reinterpret_cast<const char *>(&flags), 2);
    buffer.write(reinterpret_cast<const char *>(&decimals), 1);
    writeChar(0x0, 2, buffer);
    if (is_comm_field_list_response)
    {
        /// We should write length encoded int with string size
        /// followed by string with some "default values" (possibly it's column defaults).
        /// But we just send NULL for simplicity.
        writeChar(0xfb, buffer);
    }
}

ColumnDefinition getColumnDefinition(const String & column_name, const DataTypePtr & data_type)
{
    ColumnType column_type;
    CharacterSet charset = CharacterSet::binary;
    int flags = 0;
    uint8_t decimals = 0;
    DataTypePtr normalized_data_type = removeLowCardinalityAndNullable(data_type);
    TypeIndex type_index = normalized_data_type->getTypeId();
    switch (type_index)
    {
        case TypeIndex::UInt8:
            column_type = ColumnType::MYSQL_TYPE_TINY;
            flags = ColumnDefinitionFlags::BINARY_FLAG | ColumnDefinitionFlags::UNSIGNED_FLAG;
            break;
        case TypeIndex::UInt16:
            column_type = ColumnType::MYSQL_TYPE_SHORT;
            flags = ColumnDefinitionFlags::BINARY_FLAG | ColumnDefinitionFlags::UNSIGNED_FLAG;
            break;
        case TypeIndex::UInt32:
            column_type = ColumnType::MYSQL_TYPE_LONG;
            flags = ColumnDefinitionFlags::BINARY_FLAG | ColumnDefinitionFlags::UNSIGNED_FLAG;
            break;
        case TypeIndex::UInt64:
            column_type = ColumnType::MYSQL_TYPE_LONGLONG;
            flags = ColumnDefinitionFlags::BINARY_FLAG | ColumnDefinitionFlags::UNSIGNED_FLAG;
            break;
        case TypeIndex::Int8:
            column_type = ColumnType::MYSQL_TYPE_TINY;
            flags = ColumnDefinitionFlags::BINARY_FLAG;
            break;
        case TypeIndex::Int16:
            column_type = ColumnType::MYSQL_TYPE_SHORT;
            flags = ColumnDefinitionFlags::BINARY_FLAG;
            break;
        case TypeIndex::Int32:
            column_type = ColumnType::MYSQL_TYPE_LONG;
            flags = ColumnDefinitionFlags::BINARY_FLAG;
            break;
        case TypeIndex::Int64:
            column_type = ColumnType::MYSQL_TYPE_LONGLONG;
            flags = ColumnDefinitionFlags::BINARY_FLAG;
            break;
        case TypeIndex::Float32:
            column_type = ColumnType::MYSQL_TYPE_FLOAT;
            flags = ColumnDefinitionFlags::BINARY_FLAG;
            decimals = 31;
            break;
        case TypeIndex::Float64:
            column_type = ColumnType::MYSQL_TYPE_DOUBLE;
            flags = ColumnDefinitionFlags::BINARY_FLAG;
            decimals = 31;
            break;
        case TypeIndex::Date:
        case TypeIndex::Date32:
            column_type = ColumnType::MYSQL_TYPE_DATE;
            flags = ColumnDefinitionFlags::BINARY_FLAG;
            break;
        case TypeIndex::DateTime:
        case TypeIndex::DateTime64:
            column_type = ColumnType::MYSQL_TYPE_DATETIME;
            flags = ColumnDefinitionFlags::BINARY_FLAG;
            break;
        case TypeIndex::Decimal32:
        case TypeIndex::Decimal64:
            column_type = ColumnType::MYSQL_TYPE_DECIMAL;
            flags = ColumnDefinitionFlags::BINARY_FLAG;
            break;
        case TypeIndex::Decimal128: {
            // MySQL Decimal has max 65 precision and 30 scale
            // Decimal256 (min scale is 39) is higher than the MySQL supported range and handled in the default case
            // See https://dev.mysql.com/doc/refman/8.0/en/precision-math-decimal-characteristics.html
            const auto & type = assert_cast<const DataTypeDecimal128 &>(*normalized_data_type);
            if (type.getPrecision() > 65 || type.getScale() > 30)
            {
                column_type = ColumnType::MYSQL_TYPE_STRING;
                charset = CharacterSet::utf8_general_ci;
            }
            else
            {
                column_type = ColumnType::MYSQL_TYPE_DECIMAL;
                flags = ColumnDefinitionFlags::BINARY_FLAG;
            }
            break;
        }
        default:
            column_type = ColumnType::MYSQL_TYPE_STRING;
            charset = CharacterSet::utf8_general_ci;
            break;
    }
    return ColumnDefinition(column_name, charset, 0, column_type, flags, decimals);
}

}

}

}
