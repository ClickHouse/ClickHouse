#include <Processors/Formats/Impl/MsgPackRowOutputFormat.h>

#if USE_MSGPACK

#include <Formats/FormatFactory.h>
#include <Common/assert_cast.h>

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnLowCardinality.h>

#include <Formats/MsgPackExtensionTypes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

MsgPackRowOutputFormat::MsgPackRowOutputFormat(WriteBuffer & out_, const Block & header_, const RowOutputFormatParams & params_, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_, params_), packer(out_), format_settings(format_settings_) {}

void MsgPackRowOutputFormat::serializeField(const IColumn & column, DataTypePtr data_type, size_t row_num)
{
    switch (data_type->getTypeId())
    {
        case TypeIndex::UInt8:
        {
            packer.pack_uint8(assert_cast<const ColumnUInt8 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::Date: [[fallthrough]];
        case TypeIndex::UInt16:
        {
            packer.pack_uint16(assert_cast<const ColumnUInt16 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::DateTime: [[fallthrough]];
        case TypeIndex::UInt32:
        {
            packer.pack_uint32(assert_cast<const ColumnUInt32 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::UInt64:
        {
            packer.pack_uint64(assert_cast<const ColumnUInt64 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::Int8:
        {
            packer.pack_int8(assert_cast<const ColumnInt8 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::Int16:
        {
            packer.pack_int16(assert_cast<const ColumnInt16 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::Int32:
        {
            packer.pack_int32(assert_cast<const ColumnInt32 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::Int64:
        {
            packer.pack_int64(assert_cast<const ColumnInt64 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::Float32:
        {
            packer.pack_float(assert_cast<const ColumnFloat32 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::Float64:
        {
            packer.pack_double(assert_cast<const ColumnFloat64 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::DateTime64:
        {
            packer.pack_uint64(assert_cast<const DataTypeDateTime64::ColumnType &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::String:
        {
            const std::string_view & string = assert_cast<const ColumnString &>(column).getDataAt(row_num).toView();
            packer.pack_bin(string.size());
            packer.pack_bin_body(string.data(), string.size());
            return;
        }
        case TypeIndex::FixedString:
        {
            const std::string_view & string = assert_cast<const ColumnFixedString &>(column).getDataAt(row_num).toView();
            packer.pack_bin(string.size());
            packer.pack_bin_body(string.data(), string.size());
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
            packer.pack_array(size);
            for (size_t i = 0; i < size; ++i)
            {
                serializeField(nested_column, nested_type, offset + i);
            }
            return;
         }
        case TypeIndex::Nullable:
        {
            auto nested_type = removeNullable(data_type);
            const ColumnNullable & column_nullable = assert_cast<const ColumnNullable &>(column);
            if (!column_nullable.isNullAt(row_num))
                serializeField(column_nullable.getNestedColumn(), nested_type, row_num);
            else
                packer.pack_nil();
            return;
        }
        case TypeIndex::Nothing:
        {
            packer.pack_nil();
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
            packer.pack_map(size);
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
            switch (format_settings.msgpack.output_uuid_representation)
            {
                case FormatSettings::MsgPackUUIDRepresentation::BIN:
                {
                    WriteBufferFromOwnString buf;
                    writeBinary(uuid_column.getElement(row_num), buf);
                    std::string_view uuid_bin = buf.stringRef().toView();
                    packer.pack_bin(uuid_bin.size());
                    packer.pack_bin_body(uuid_bin.data(), uuid_bin.size());
                    return;
                }
                case FormatSettings::MsgPackUUIDRepresentation::STR:
                {
                    WriteBufferFromOwnString buf;
                    writeText(uuid_column.getElement(row_num), buf);
                    std::string_view uuid_text = buf.stringRef().toView();
                    packer.pack_str(uuid_text.size());
                    packer.pack_bin_body(uuid_text.data(), uuid_text.size());
                    return;
                }
                case FormatSettings::MsgPackUUIDRepresentation::EXT:
                {
                    WriteBufferFromOwnString buf;
                    UUID value = uuid_column.getElement(row_num);
                    writeBinaryBigEndian(value.toUnderType().items[0], buf);
                    writeBinaryBigEndian(value.toUnderType().items[1], buf);
                    std::string_view uuid_ext = buf.stringRef().toView();
                    packer.pack_ext(sizeof(UUID), int8_t(MsgPackExtensionTypes::UUIDType));
                    packer.pack_ext_body(uuid_ext.data(), uuid_ext.size());
                    return;
                }
            }
        }
        default:
            break;
    }
    throw Exception("Type " + data_type->getName() + " is not supported for MsgPack output format", ErrorCodes::ILLEGAL_COLUMN);
}

void MsgPackRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    size_t columns_size = columns.size();
    for (size_t i = 0; i < columns_size; ++i)
    {
        serializeField(*columns[i], types[i], row_num);
    }
}


void registerOutputFormatMsgPack(FormatFactory & factory)
{
    factory.registerOutputFormat("MsgPack", [](
            WriteBuffer & buf,
            const Block & sample,
            const RowOutputFormatParams & params,
            const FormatSettings & settings)
    {
        return std::make_shared<MsgPackRowOutputFormat>(buf, sample, params, settings);
    });
    factory.markOutputFormatSupportsParallelFormatting("MsgPack");
}

}

#else

namespace DB
{
class FormatFactory;
void registerOutputFormatMsgPack(FormatFactory &)
{
}
}

#endif
