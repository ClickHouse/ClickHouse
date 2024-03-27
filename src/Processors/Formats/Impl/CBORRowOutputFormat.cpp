#include <Processors/Formats/Impl/CBORRowOutputFormat.h>
#include <Formats/FormatFactory.h>

#ifdef USE_CBOR

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnLowCardinality.h>

#include <IO/WriteHelpers.h>

namespace DB
{

CBOROutput::CBOROutput(WriteBuffer & buf) : buffer(buf) {}

unsigned char *CBOROutput::data() { return nullptr; }

unsigned int CBOROutput::size() { return 0; }

void CBOROutput::put_byte(unsigned char value)
{
    writeChar(static_cast<char>(value), buffer);
}

void CBOROutput::put_bytes(const unsigned char *data, int size)
{
     writeString(reinterpret_cast<const char *>(data), static_cast<size_t>(size), buffer);
}

CBORRowOutputFormat::CBORRowOutputFormat(const Block & header_, WriteBuffer & out_, const FormatSettings & format_settings_) :
IRowOutputFormat(header_, out_), cbor_out(out_), format_settings(format_settings_), encoder(cbor_out) {}

void CBORRowOutputFormat::writePrefix()
{
    cbor_out.put_byte(0x9f);
}

void CBORRowOutputFormat::writeSuffix()
{
    cbor_out.put_byte(0xff);
}

void CBORRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    size_t columns_size = columns.size();
    encoder.write_array(static_cast<int>(columns_size));
    for (size_t i = 0; i < columns_size; ++i)
    {
        serializeField(*columns[i], types[i], row_num);
    }
}

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

void CBORRowOutputFormat::serializeField(const IColumn & column, DataTypePtr data_type, size_t row_num)
{
    switch (data_type->getTypeId())
    {
        case TypeIndex::Nullable:
        {
            const ColumnNullable & column_nullable = assert_cast<const ColumnNullable &>(column);
            if (!column_nullable.isNullAt(row_num))
            {
                serializeField(column_nullable.getNestedColumn(), removeNullable(data_type), row_num);
            }
            else
            {
                encoder.write_null();
            }
                
            return;
        }
        case TypeIndex::Nothing:
        {
            encoder.write_null();
            return;
        }
        case TypeIndex::UInt8:
        {
            encoder.write_int(static_cast<unsigned int>(assert_cast<const ColumnUInt8 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::UInt16:
        {
            encoder.write_int(static_cast<unsigned int>(assert_cast<const ColumnUInt16 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::UInt32:
        {
            encoder.write_int(static_cast<unsigned int>(assert_cast<const ColumnUInt32 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::UInt64:
        {
            encoder.write_int(static_cast<unsigned long long>(assert_cast<const ColumnUInt64 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::UInt128:
        {
            encoder.write_tag(2);
            encoder.write_bytes(reinterpret_cast<const unsigned char*>(column.getDataAt(row_num).data), sizeof(UInt128));
            return;
        }
        case TypeIndex::UInt256:
        {   
            encoder.write_tag(2);
            encoder.write_bytes(reinterpret_cast<const unsigned char*>(column.getDataAt(row_num).data), sizeof(UInt256));
            return;
        }
        case TypeIndex::Enum8: [[fallthrough]];
        case TypeIndex::Int8:
        {
            encoder.write_int(static_cast<int>(assert_cast<const ColumnInt8 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::Enum16: [[fallthrough]];
        case TypeIndex::Int16:
        {
            encoder.write_int(static_cast<int>(assert_cast<const ColumnInt16 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::Int32:
        {
            encoder.write_int(static_cast<int>(assert_cast<const ColumnInt32 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::Int64:
        {
            encoder.write_int(static_cast<long long>(assert_cast<const ColumnInt64 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::Int128:
        {
            Int128 num = assert_cast<const ColumnInt128 &>(column).getElement(row_num);
            if (num >= 0)
            {
                encoder.write_tag(2);
            }
            else
            {
                encoder.write_tag(3);
                num = -(num + 1);
            }

            encoder.write_bytes(reinterpret_cast<const unsigned char*>(&num), sizeof(Int128));
            return;
        }
        case TypeIndex::Int256:
        {
            Int256 num = assert_cast<const ColumnInt256 &>(column).getElement(row_num);
            if (num >= 0)
            {
                encoder.write_tag(2);
            }
            else
            {
                encoder.write_tag(3);
                num = -(num + 1);
            }

            encoder.write_bytes(reinterpret_cast<const unsigned char*>(&num), sizeof(Int256));
            return;
        }
        case TypeIndex::Float32:
        {
            encoder.write_float(static_cast<float>(assert_cast<const ColumnFloat32 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::Float64:
        {
            encoder.write_double(static_cast<double>(assert_cast<const ColumnFloat64 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::String:
        {
            const std::string_view & str = assert_cast<const ColumnString &>(column).getDataAt(row_num).toView();
            encoder.write_string(str.data(), static_cast<unsigned>(str.size()));
            return;
        }
        case TypeIndex::FixedString:
        {
            const std::string_view & str = assert_cast<const ColumnFixedString &>(column).getDataAt(row_num).toView();
            encoder.write_string(str.data(), static_cast<unsigned>(str.size()));
            return;
        }
        case TypeIndex::Decimal32:
        {
            encoder.write_int(static_cast<int>(assert_cast<const ColumnDecimal<Decimal32> &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::Decimal64:
        {
            encoder.write_int(static_cast<long long>(assert_cast<const ColumnDecimal<Decimal64> &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::Decimal128:
        {
            Decimal128 num = assert_cast<const ColumnDecimal<Decimal128> &>(column).getElement(row_num);
            if (num >= Decimal128(0))
            {
                encoder.write_tag(2);
            }
            else
            {
                encoder.write_tag(3);
                num = -(num + Decimal128(1));
            }

            encoder.write_bytes(reinterpret_cast<const unsigned char*>(&num), sizeof(Decimal128));
            return;
        }
        case TypeIndex::Decimal256:
        {
            Decimal256 num = assert_cast<const ColumnDecimal<Decimal256> &>(column).getElement(row_num);
            if (num >= Decimal256(0))
            {
                encoder.write_tag(2);
            }
            else
            {
                encoder.write_tag(3);
                num = -(num + Decimal256(1));
            }

            encoder.write_bytes(reinterpret_cast<const unsigned char*>(&num), sizeof(Decimal256));
            return;
        }
        case TypeIndex::IPv4:
        {
            encoder.write_int(static_cast<unsigned int>(assert_cast<const ColumnIPv4 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::IPv6:
        {
            encoder.write_bytes(reinterpret_cast<const unsigned char*>(column.getDataAt(row_num).data), sizeof(IPv6));
            return;
        }
        case TypeIndex::Array:
        {
            auto nested_type = assert_cast<const DataTypeArray &>(*data_type).getNestedType();
            const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
            const IColumn & nested_column = column_array.getData();
            const ColumnArray::Offsets & offsets = column_array.getOffsets();
            size_t offset = offsets[row_num - 1];
            size_t size = offsets[row_num] - offset;
            encoder.write_array(static_cast<int>(size));
            for (size_t i = 0; i < size; ++i)
            {
                serializeField(nested_column, nested_type, offset + i);
            }
            return;
        }
        case TypeIndex::Tuple:
        {
            const auto & tuple_type = assert_cast<const DataTypeTuple &>(*data_type);
            const auto & nested_types = tuple_type.getElements();
            const ColumnTuple & column_tuple = assert_cast<const ColumnTuple &>(column);
            const auto & nested_columns = column_tuple.getColumns();
            encoder.write_array(static_cast<unsigned>(nested_types.size()));
            for (size_t i = 0; i < nested_types.size(); ++i)
                serializeField(*nested_columns[i], nested_types[i], row_num);
            return;
        }
        case TypeIndex::Map:
        {
            const auto & map_column = assert_cast<const ColumnMap &>(column);
            const auto & nested_column = map_column.getNestedColumn();
            const auto & key_value_columns = map_column.getNestedData().getColumns();
            const auto & key_column = key_value_columns[0];
            const auto & value_column = key_value_columns[1];

            const auto & map_type = assert_cast<const DataTypeMap &>(*data_type);
            const auto & offsets = nested_column.getOffsets();
            size_t offset = offsets[row_num - 1];
            size_t size = offsets[row_num] - offset;
            encoder.write_map(static_cast<unsigned>(size));
            for (size_t i = 0; i < size; ++i)
            {
                serializeField(*key_column, map_type.getKeyType(), offset + i);
                serializeField(*value_column, map_type.getValueType(), offset + i);
            }
            return;
        }
        case TypeIndex::LowCardinality:
        {
            const auto & lc_column = assert_cast<const ColumnLowCardinality &>(column);
            auto dict_type = assert_cast<const DataTypeLowCardinality *>(data_type.get())->getDictionaryType();
            auto dict_column = lc_column.getDictionary().getNestedColumn();
            size_t index = lc_column.getIndexAt(row_num);
            serializeField(*dict_column, dict_type, index);
            return;
        }
        case TypeIndex::UUID:
        {
            const auto & uuid_column = assert_cast<const ColumnUUID &>(column);
            WriteBufferFromOwnString buf;
            writeText(uuid_column.getElement(row_num), buf);
            std::string_view uuid_text = buf.stringView();
            encoder.write_string(uuid_text.data(), static_cast<unsigned>(uuid_text.size()));
            return;
        }
        default:
            break;
    }
    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Type {} is not supported for CBOR output format", data_type->getName());
}

void registerOutputFormatCBOR(FormatFactory & factory)
{
    factory.registerOutputFormat("CBOR", [](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & settings)
    {
        return std::make_shared<CBORRowOutputFormat>(sample, buf, settings);
    });

    factory.markOutputFormatSupportsParallelFormatting("CBOR");
}
}

#else

namespace DB
{
class FormatFactory;
void registerOutputFormatCBOR(FormatFactory &) {}
}

#endif

