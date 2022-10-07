#include <Processors/Formats/Impl/BSONEachRowRowOutputFormat.h>
#include <Processors/Formats/Impl/BSONUtils.h>

#include <Formats/FormatFactory.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>

#include <DataTypes/DataTypeDateTime64.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
}

BSONEachRowRowOutputFormat::BSONEachRowRowOutputFormat(
    WriteBuffer & out_, const Block & header_, const RowOutputFormatParams & params_, const FormatSettings & settings_)
    : IRowOutputFormat(header_, out_, params_), settings(settings_)
{
    const auto & sample = getPort(PortKind::Main).getHeader();
    size_t columns = sample.columns();
    fields.resize(columns);

    for (size_t i = 0; i < columns; ++i)
    {
        WriteBufferFromString buf(fields[i]);
        writeString(sample.getByPosition(i).name.c_str(), buf);
    }
}

void BSONEachRowRowOutputFormat::serializeField(const IColumn & column, size_t row_num, const String & name)
{
    switch (column.getDataType())
    {
        case TypeIndex::Float64: {
            writeChar(0x01, out);
            writeString(name, out);
            writeChar(0x00, out);
            writePODBinary<Float64>(assert_cast<const ColumnFloat64 &>(column).getElement(row_num), out);
            obj_size += BSON_TYPE + name.size() + BSON_ZERO + BSON_64;
            break;
        }
        case TypeIndex::String: {
            writeChar(0x02, out);
            writeString(name, out);
            writeChar(0x00, out);
            writePODBinary<UInt32>(assert_cast<const ColumnString &>(column).getDataAt(row_num).size + BSON_ZERO, out);
            writeString(assert_cast<const ColumnString &>(column).getDataAt(row_num), out);
            writeChar(0x00, out);
            obj_size += BSON_TYPE + name.size() + BSON_ZERO + BSON_32 + assert_cast<const ColumnString &>(column).getDataAt(row_num).size
                + BSON_ZERO;
            break;
        }
        // case TypeIndex::EMBDOCUMENT - Not supported
        case TypeIndex::Array: {
            writeChar(0x04, out);
            writeString(name, out);
            writeChar(0x00, out);

            BufferBase::Position array_begin;
            array_begin = out.position();
            writePODBinary<UInt32>(0, out);
            UInt32 array_size = BSON_32;

            const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
            const IColumn & nested_column = column_array.getData();
            const ColumnArray::Offsets & offsets = column_array.getOffsets();
            size_t offset = offsets[row_num - 1];
            size_t size = offsets[row_num] - offset;
            for (UInt32 i = 0; i < size; ++i)
            {
                auto size_before_field = obj_size;
                serializeField(nested_column, offset + i, std::to_string(i));
                array_size += obj_size - size_before_field;
            }
            writeChar(0x00, out);
            ++array_size;

            BufferBase::Position array_end = out.position();
            out.position() = array_begin;
            writePODBinary<UInt32>(array_size, out);
            out.position() = array_end;
            obj_size += BSON_TYPE + BSON_32 + name.size() + BSON_ZERO + BSON_ZERO;
            break;
        }
        // case TypeIndex::Binary:    - Not supported
        // case TypeIndex::UNDEFINED  - Depricated
        case TypeIndex::UUID: {
            writeChar(0x07, out);
            writeString(name, out);
            writeChar(0x00, out);
            writeString(assert_cast<const ColumnUUID &>(column).getDataAt(row_num), out);
            obj_size += BSON_TYPE + name.size() + BSON_ZERO + BSON_128;
            break;
        }
        case TypeIndex::UInt8: {
            writeChar(0x08, out);
            writeString(name, out);
            writeChar(0x00, out);
            writeString(assert_cast<const ColumnUInt8 &>(column).getDataAt(row_num), out);
            obj_size += BSON_TYPE + name.size() + BSON_ZERO + BSON_8;
            break;
        }
        case TypeIndex::DateTime64: {
            writeChar(0x09, out);
            writeString(name, out);
            writeChar(0x00, out);
            writePODBinary<UInt64>(assert_cast<const DataTypeDateTime64::ColumnType &>(column).getElement(row_num), out);
            obj_size += BSON_TYPE + name.size() + BSON_ZERO + BSON_64;
            break;
        }
        case TypeIndex::Nullable: {
            writeChar(0x0A, out);
            writeString(name, out);
            writeChar(0x00, out);
            obj_size += BSON_TYPE + name.size() + BSON_ZERO;
            break;
        }
        // case TypeIndex::Regexp - Not supported
        // case TypeIndex::DBPointer - Depricated
        // case TypeIndex::JSCODE - Not supported
        // case TypeIndex::Symbol - Depricated
        // case TypeIndex::JSCODE_w_scope - Depricated
        case TypeIndex::Int32: {
            writeChar(0x10, out);
            writeString(name, out);
            writeChar(0x00, out);
            writePODBinary<Int32>(assert_cast<const ColumnInt32 &>(column).getElement(row_num), out);
            obj_size += BSON_TYPE + name.size() + BSON_ZERO + BSON_32;
            break;
        }
        case TypeIndex::UInt64: {
            writeChar(0x11, out);
            writeString(name, out);
            writeChar(0x00, out);
            writePODBinary<UInt64>(assert_cast<const ColumnUInt64 &>(column).getElement(row_num), out);
            obj_size += BSON_TYPE + name.size() + BSON_ZERO + BSON_64;
            break;
        }
        case TypeIndex::Int64: {
            writeChar(0x12, out);
            writeString(name, out);
            writeChar(0x00, out);
            writePODBinary<Int64>(assert_cast<const ColumnInt64 &>(column).getElement(row_num), out);
            obj_size += BSON_TYPE + name.size() + BSON_ZERO + BSON_64;
            break;
        }
        case TypeIndex::Decimal128: {
            writeChar(0x13, out);
            writeString(name, out);
            writeChar(0x00, out);
            writePODBinary<Decimal128>(assert_cast<const ColumnDecimal<Decimal128> &>(column).getElement(row_num), out);
            obj_size += BSON_TYPE + name.size() + BSON_ZERO + BSON_128;
            break;
        }
        // case TypeIndex::MinKey - Not supported
        // case TypeIndex::MaxKey - Not supported
        default:
            throw Exception("Unrecognized Type", ErrorCodes::INCORRECT_DATA);
    }
    ++row_num;
}

void BSONEachRowRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    BufferBase::Position buf_begin;
    buf_begin = out.position();
    writePODBinary<UInt32>(0, out);
    obj_size += BSON_32;

    size_t columns_size = columns.size();
    for (size_t i = 0; i < columns_size; ++i)
    {
        BSONEachRowRowOutputFormat::serializeField(*columns[i], row_num, fields[i]);
    }
    writeChar(0x00, out);
    ++obj_size;

    BufferBase::Position buf_end = out.position();
    out.position() = buf_begin;
    writePODBinary<UInt32>(obj_size, out);
    out.position() = buf_end;
}

void registerOutputFormatBSONEachRow(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "BSONEachRow",
        [](WriteBuffer & buf, const Block & sample, const RowOutputFormatParams & params, const FormatSettings & _format_settings)
        { return std::make_shared<BSONEachRowRowOutputFormat>(buf, sample, params, _format_settings); });
    factory.markOutputFormatSupportsParallelFormatting("BSONEachRow");
}

}
