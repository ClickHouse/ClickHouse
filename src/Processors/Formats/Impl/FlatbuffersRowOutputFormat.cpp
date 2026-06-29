#include <Processors/Formats/Impl/FlatbuffersRowOutputFormat.h>

#if USE_FLATBUFFERS

#include <Formats/FormatFactory.h>

#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <Core/UUID.h>

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnLowCardinality.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

FlatbuffersRowOutputFormat::FlatbuffersRowOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings &)
    : IRowOutputFormat(header_, out_)
{
}

void FlatbuffersRowOutputFormat::writePrefix()
{
    /// The whole result is a single vector of rows.
    root_start = builder.StartVector();
}

void FlatbuffersRowOutputFormat::writeSuffix()
{
    builder.EndVector(root_start, /*typed=*/false, /*fixed=*/false);
    builder.Finish();
    const std::vector<uint8_t> & buffer = builder.GetBuffer();
    out.write(reinterpret_cast<const char *>(buffer.data()), buffer.size());
}

void FlatbuffersRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    /// Each row is a vector of its column values in the order of the header.
    size_t row_start = builder.StartVector();
    for (size_t i = 0; i < num_columns; ++i)
        serializeField(*columns[i], types[i], row_num);
    builder.EndVector(row_start, /*typed=*/false, /*fixed=*/false);
}

void FlatbuffersRowOutputFormat::serializeField(const IColumn & column, const DataTypePtr & data_type, size_t row_num)
{
    switch (data_type->getTypeId())
    {
        case TypeIndex::Nullable:
        {
            const ColumnNullable & column_nullable = assert_cast<const ColumnNullable &>(column);
            if (column_nullable.isNullAt(row_num))
                builder.Null();
            else
                serializeField(column_nullable.getNestedColumn(), removeNullable(data_type), row_num);
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
        case TypeIndex::Date: [[fallthrough]];
        case TypeIndex::UInt16:
        {
            builder.UInt(static_cast<uint64_t>(assert_cast<const ColumnUInt16 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::DateTime: [[fallthrough]];
        case TypeIndex::UInt32:
        {
            builder.UInt(static_cast<uint64_t>(assert_cast<const ColumnUInt32 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::UInt64:
        {
            builder.UInt(static_cast<uint64_t>(assert_cast<const ColumnUInt64 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::IPv4:
        {
            builder.UInt(static_cast<uint64_t>(assert_cast<const ColumnIPv4 &>(column).getElement(row_num)));
            return;
        }
        case TypeIndex::UInt128: [[fallthrough]];
        case TypeIndex::UInt256:
        {
            std::string_view data = column.getDataAt(row_num);
            builder.Blob(data.data(), data.size());
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
        case TypeIndex::Date32: [[fallthrough]];
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
        case TypeIndex::Int128: [[fallthrough]];
        case TypeIndex::Int256:
        {
            std::string_view data = column.getDataAt(row_num);
            builder.Blob(data.data(), data.size());
            return;
        }
        case TypeIndex::Float32:
        {
            builder.Float(assert_cast<const ColumnFloat32 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::Float64:
        {
            builder.Double(assert_cast<const ColumnFloat64 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::DateTime64:
        {
            builder.Int(static_cast<int64_t>(assert_cast<const DataTypeDateTime64::ColumnType &>(column).getElement(row_num)));
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
        case TypeIndex::Decimal128: [[fallthrough]];
        case TypeIndex::Decimal256:
        {
            std::string_view data = column.getDataAt(row_num);
            builder.Blob(data.data(), data.size());
            return;
        }
        case TypeIndex::IPv6:
        {
            std::string_view data = column.getDataAt(row_num);
            builder.Blob(data.data(), data.size());
            return;
        }
        case TypeIndex::String:
        {
            std::string_view str = assert_cast<const ColumnString &>(column).getDataAt(row_num);
            builder.String(str.data(), str.size());
            return;
        }
        case TypeIndex::FixedString:
        {
            std::string_view str = assert_cast<const ColumnFixedString &>(column).getDataAt(row_num);
            builder.String(str.data(), str.size());
            return;
        }
        case TypeIndex::UUID:
        {
            WriteBufferFromOwnString buf;
            writeText(assert_cast<const ColumnUUID &>(column).getElement(row_num), buf);
            std::string_view uuid_text = buf.stringView();
            builder.String(uuid_text.data(), uuid_text.size());
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
                serializeField(nested_column, nested_type, offset + i);
            builder.EndVector(start, /*typed=*/false, /*fixed=*/false);
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
            builder.EndVector(start, /*typed=*/false, /*fixed=*/false);
            return;
        }
        case TypeIndex::LowCardinality:
        {
            const ColumnLowCardinality & column_lc = assert_cast<const ColumnLowCardinality &>(column);
            auto dict_type = assert_cast<const DataTypeLowCardinality &>(*data_type).getDictionaryType();
            auto dict_column = column_lc.getDictionary().getNestedColumn();
            size_t index = column_lc.getIndexAt(row_num);
            serializeField(*dict_column, dict_type, index);
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
            const FormatSettings & settings,
            FormatFilterInfoPtr /*format_filter_info*/)
    {
        return std::make_shared<FlatbuffersRowOutputFormat>(buf, std::make_shared<const Block>(sample), settings);
    });

    factory.markOutputFormatNotTTYFriendly("Flatbuffers");
    factory.setContentType("Flatbuffers", "application/octet-stream");
}

}

#else

namespace DB
{
class FormatFactory;
void registerOutputFormatFlatbuffers(FormatFactory &) {}
}

#endif
