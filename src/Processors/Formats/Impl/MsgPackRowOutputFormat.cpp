#include <Processors/Formats/Impl/MsgPackRowOutputFormat.h>

#if USE_MSGPACK

#include <Formats/FormatFactory.h>
#include <Common/assert_cast.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

MsgPackRowOutputFormat::MsgPackRowOutputFormat(WriteBuffer & out_, const Block & header_, const RowOutputFormatParams & params_)
    : IRowOutputFormat(header_, out_, params_), packer(out_) {}

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
            const StringRef & string = assert_cast<const ColumnString &>(column).getDataAt(row_num);
            packer.pack_str(string.size);
            packer.pack_str_body(string.data, string.size);
            return;
        }
        case TypeIndex::FixedString:
        {
            const StringRef & string = assert_cast<const ColumnFixedString &>(column).getDataAt(row_num);
            packer.pack_str(string.size);
            packer.pack_str_body(string.data, string.size);
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
        default:
            break;
    }
    throw Exception("Type " + data_type->getName() + " is not supported for MsgPack output format", ErrorCodes::ILLEGAL_COLUMN);
}

void MsgPackRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    size_t num_columns = columns.size();
    for (size_t i = 0; i < num_columns; ++i)
    {
        serializeField(*columns[i], types[i], row_num);
    }
}


void registerOutputFormatProcessorMsgPack(FormatFactory & factory)
{

    factory.registerOutputFormatProcessor("MsgPack", [](
            WriteBuffer & buf,
            const Block & sample,
            const RowOutputFormatParams & params,
            const FormatSettings &)
    {
        return std::make_shared<MsgPackRowOutputFormat>(buf, sample, params);
    });
}

}

#else

namespace DB
{
class FormatFactory;
void registerOutputFormatProcessorMsgPack(FormatFactory &)
{
}
}

#endif
