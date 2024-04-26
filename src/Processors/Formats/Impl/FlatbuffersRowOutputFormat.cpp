#include <Processors/Formats/Impl/FlatbuffersRowOutputFormat.h>
#include <Formats/FormatFactory.h>

#ifdef USE_FLATBUFFERS

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

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

FlatbuffersRowOutputFormat::FlatbuffersRowOutputFormat(const Block & header_, WriteBuffer & out_, const FormatSettings & format_settings_) :
IRowOutputFormat(header_, out_) {}

void FlatbuffersRowOutputFormat:writePrefix()
{
    builder.StartVector();
}

void FlatbuffersRowOutputFormat::writeSuffix()
{
    builder.EndVector(0, false, false);
    builder.Finish();
    writeString(builder.GetBuffer().data(), builder.GetSize(), out);
}

void FlatbuffersRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    size_t columns_size = columns.size();
    size_t start = builder.StartVector();
    for (size_t i = 0; i < columns_size; ++i)
    {
        serializeField(*columns[i], types[i], row_num);
    }
    builder.EndVector(start, false, false);
}

void FlatbuffersRowOutputFormat::serializeField(const IColumn & column, DataTypePtr data_type, size_t row_num, const char* key, size_t key_size)
{
    if (key)
    {
        builder.Key(key, key_size);
    }

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
                builder.Null()
            }
                
            return;
        }
        case TypeIndex::Nothing:
        {
             builder.Null();
            return;
        }
        case TypeIndex::UInt8:
        {
            builder.UInt(static_cast<uint64_t>(assert_cast<const ColumnUInt8 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::UInt16:
        {
            bulder.UInt(static_cast<uint64_t>(assert_cast<const ColumnUInt16 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::DateTime: [[fallthrough]];
        case TypeIndex::UInt32:
        {
            bulder.UInt(static_cast<uint64_t>(assert_cast<const ColumnUInt32 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::UInt64:
        {
            bulder.UInt(static_cast<uint64_t>(assert_cast<const ColumnUInt64 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::UInt128:
        {
            builder.Blob(reinterpret_cast<const uint8_t*>(column.getDataAt(row_num).data), sizeof(UInt128))
            return;
        }
        case TypeIndex::UInt256:
        {   
            builder.Blob(reinterpret_cast<const uint8_t*>(column.getDataAt(row_num).data), sizeof(UInt256));
            return;
        }
        case TypeIndex::Enum8: [[fallthrough]];
        case TypeIndex::Int8:
        {
            builder.Int(static_cast<int64_t>(assert_cast<const ColumnInt8 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::Enum16: [[fallthrough]];
        case TypeIndex::Int16:
        {
            builder.Int(static_cast<int64_t>(assert_cast<const ColumnInt16 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::DateTime64: [[fallthrough]];
        case TypeIndex::Int32:
        {
            builder.Int(static_cast<int64_t>(assert_cast<const ColumnInt32 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::Int64:
        {
            builder.Int(static_cast<int64_t>(assert_cast<const ColumnInt64 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::Int128:
        {
            builder.Blob(reinterpret_cast<const uint8_t*>(column.getDataAt(row_num).data), sizeof(Int128))
            return;
        }
        case TypeIndex::Int256:
        {
            builder.Blob(reinterpret_cast<const uint8_t*>(column.getDataAt(row_num).data), sizeof(Int256))
            return;
        }
        case TypeIndex::Float32:
        {
            builder.Float(static_cast<float>(assert_cast<const ColumnFloat32 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::Float64:
        {
            builder.Double(static_cast<double>(assert_cast<const ColumnFloat64 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::String:
        {
            const std::string & str = assert_cast<const ColumnString &>(column).getElement(row_num);
            builder.String(str);
            return;
        }
        case TypeIndex::FixedString:
        {
            const std::string & str = assert_cast<const ColumnFixedString &>(column).getElement(row_num);
            builder.String(str);
            return;
        }
        case TypeIndex::Decimal32:
        {
            builder.Int(static_cast<int64_t>(assert_cast<const ColumnDecimal<Decimal32> &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::Decimal64:
        {
            builder.Int(static_cast<int64_t>(assert_cast<const ColumnDecimal<Decimal64> &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::Decimal128:
        {
            builder.Blob(reinterpret_cast<const uint8_t*>(column.getDataAt(row_num).data), sizeof(Decimal128));
            return;
        }
        case TypeIndex::Decimal256:
        {
            builder.Blob(reinterpret_cast<const uint8_t*>(column.getDataAt(row_num).data), sizeof(Decimal256));
            return;
        }
        case TypeIndex::IPv4:
        {
            builder.Int(static_cast<int64_t>(assert_cast<const ColumnIPv4 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::IPv6:
        {
            builder.Blob(reinterpret_cast<const uint8_t*>(column.getDataAt(row_num).data), sizeof(IPv6));
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
            size_t start = builder.StartVector();
            for (size_t i = 0; i < size; ++i)
            {
                serializeField(nested_column, nested_type, offset + i);
            }
            builder.EndVector(start, true, true);
            return;
        }
        case TypeIndex::Tuple:
        {
            const auto & tuple_type = assert_cast<const DataTypeTuple &>(*data_type);
            const auto & nested_types = tuple_type.getElements();
            const ColumnTuple & column_tuple = assert_cast<const ColumnTuple &>(column);
            const auto & nested_columns = column_tuple.getColumns();
            size_t start = builder.StartVector();
            for (size_t i = 0; i < nested_types.size(); ++i)
                serializeField(*nested_columns[i], nested_types[i], row_num);
            builder.EndVector(start, false, false);
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
            size_t start = builder.StartMap()
            for (size_t i = 0; i < size; ++i)
            {
                const char* key = reinterpret_cast<const char*>(key_column->getDataAt(offset + i).data);
                size_t key_size = map_type.getValueType()->getDefault();
                serializeField(*value_column, map_type.getValueType(), offset + i, key, key_size);
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
            std::string uuid_text = buf.str()
            builder.String(str);
            return;
        }
        default:
            break;
    }
    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Type {} is not supported for Flatbuffers output format", data_type->getName());
}

void registerOutputFormatFlatbuffers(FormatFactory & factory)
{
    factory.registerOutputFormat("Flatbuffers", [](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & settings)
    {
        return std::make_shared<CBORRowOutputFormat>(sample, buf, settings);
    });

    factory.markOutputFormatSupportsParallelFormatting("Flatbuffers");
}
}

#else

namespace DB
{
class FormatFactory;
void registerOutputFormatFlatbuffers(FormatFactory &) {}
}

#endif