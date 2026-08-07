#include <Processors/Formats/Impl/FlatbuffersRowOutputFormat.h>

#if USE_FLATBUFFERS

#include <Formats/FormatFactory.h>

#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/transformEndianness.h>

#include <Core/UUID.h>

#include <bit>

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

FlatbuffersRowOutputFormat::FlatbuffersRowOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_)
    , string_as_string(format_settings_.flatbuffers.output_string_as_string)
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

void FlatbuffersRowOutputFormat::serializeString(std::string_view value)
{
    /// flexbuffers::Builder::String reads one byte past the given length (it copies a trailing '\0'
    /// so that C-string readers work), so the source must have an initialized byte at [size]. Copy
    /// into a NUL-terminated buffer first: the std::string overload passes c_str(), which always has
    /// a '\0' at [size]. This is required for FixedString and the UUID text buffer, whose backing
    /// storage does not guarantee a readable byte after the value. The buffer is fully consumed by
    /// builder.String before serializeField recurses, so a single reused member is safe.
    string_scratch.assign(value.data(), value.size());
    builder.String(string_scratch);
}

void FlatbuffersRowOutputFormat::serializeStringOrBlob(std::string_view value)
{
    /// ClickHouse String / FixedString values are arbitrary byte sequences: they may contain invalid
    /// UTF-8 and embedded zero bytes, which the UTF-8-only FlexBuffers String carrier cannot represent
    /// faithfully. Serialize them as Blob by default; `output_format_flatbuffers_string_as_string`
    /// opts into FlexBuffers String, writing the bytes verbatim.
    if (string_as_string)
        serializeString(value);
    else
        builder.Blob(value.data(), value.size());
}

template <typename ColumnType>
void FlatbuffersRowOutputFormat::serializeWideNumberAsBlob(const IColumn & column, size_t row_num)
{
    auto value = assert_cast<const ColumnType &>(column).getElement(row_num);
    /// Serialize as little-endian so the blob is byte-identical on every architecture (a no-op on
    /// little-endian hosts, a byte swap on big-endian ones such as s390x).
    transformEndianness<std::endian::little>(value);
    builder.Blob(&value, sizeof(value));
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
        case TypeIndex::UInt128:
        {
            serializeWideNumberAsBlob<ColumnUInt128>(column, row_num);
            return;
        }
        case TypeIndex::UInt256:
        {
            serializeWideNumberAsBlob<ColumnUInt256>(column, row_num);
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
        case TypeIndex::Int128:
        {
            serializeWideNumberAsBlob<ColumnInt128>(column, row_num);
            return;
        }
        case TypeIndex::Int256:
        {
            serializeWideNumberAsBlob<ColumnInt256>(column, row_num);
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
        case TypeIndex::Decimal128:
        {
            serializeWideNumberAsBlob<ColumnDecimal<Decimal128>>(column, row_num);
            return;
        }
        case TypeIndex::Decimal256:
        {
            serializeWideNumberAsBlob<ColumnDecimal<Decimal256>>(column, row_num);
            return;
        }
        case TypeIndex::IPv6:
        {
            /// IPv6 is stored as a fixed 16-byte value in network byte order, which is already a
            /// well-defined, architecture-independent byte sequence, so it is written verbatim.
            std::string_view data = column.getDataAt(row_num);
            builder.Blob(data.data(), data.size());
            return;
        }
        case TypeIndex::String:
        {
            serializeStringOrBlob(assert_cast<const ColumnString &>(column).getDataAt(row_num));
            return;
        }
        case TypeIndex::FixedString:
        {
            serializeStringOrBlob(assert_cast<const ColumnFixedString &>(column).getDataAt(row_num));
            return;
        }
        case TypeIndex::UUID:
        {
            WriteBufferFromOwnString buf;
            writeText(assert_cast<const ColumnUUID &>(column).getElement(row_num), buf);
            serializeString(buf.stringView());
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

void registerOutputFormatFlatbuffers(FormatFactory & factory);
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
    factory.markFormatHasNoAppendSupport("Flatbuffers");
    factory.setContentType("Flatbuffers", "application/octet-stream");

    factory.setDocumentation("Flatbuffers", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

The `Flatbuffers` format serializes the result set as a single schema-less
[FlexBuffers](https://flatbuffers.dev/flexbuffers.html) value (part of the FlatBuffers project).
Note that this is a schema-less FlexBuffers payload, not a schema-based FlatBuffers buffer.

The root value is a vector of rows, and each row is a vector of the column values in the order of
the `SELECT`. The whole result is built in memory and written out at the end, so the format is not
streaming: keep this in mind when exporting very large result sets.

This format is only available when ClickHouse is built with the `flatbuffers` contrib library
(which is enabled together with Arrow); it is not available in the fast test build.

## Data types matching {#data-types-matching}

| ClickHouse data type                                    | FlexBuffers value      |
|---------------------------------------------------------|------------------------|
| `UInt8`/`UInt16`/`UInt32`/`UInt64`, `Date`, `DateTime`  | `UInt`                 |
| `Int8`/`Int16`/`Int32`/`Int64`, `Date32`, `DateTime64`  | `Int`                  |
| `Enum8`/`Enum16`                                        | `Int`                  |
| `(U)Int128`/`(U)Int256`                                 | `Blob`                 |
| `Float32`                                               | `Float`                |
| `Float64`                                               | `Double`               |
| `Decimal32`/`Decimal64`                                 | `Int`                  |
| `Decimal128`/`Decimal256`                               | `Blob`                 |
| `String`, `FixedString`                                 | `Blob` (or `String`)   |
| `UUID`                                                  | `String` (text form)   |
| `IPv4`                                                  | `UInt`                 |
| `IPv6`                                                  | `Blob`                 |
| `Array`, `Tuple`                                        | `Vector`               |
| `Nullable` (`NULL`), `Nothing`                          | `Null`                 |
| `LowCardinality`                                        | (the underlying value) |

A `Nullable` value that is not `NULL` is serialized as its underlying value. Other types (for
example `Map`) are not supported and raise an exception.

The wide numeric types serialized as `Blob` (`(U)Int128`, `(U)Int256`, `Decimal128`, `Decimal256`)
are written as little-endian byte sequences, so the output is identical on every architecture.
`IPv6` is written as its 16-byte network-order representation.

ClickHouse `String` and `FixedString` values are arbitrary byte sequences that may contain invalid
UTF-8 and embedded zero bytes, while FlexBuffers `String` values are expected to be valid UTF-8
text, so these columns are serialized as `Blob` by default. Set
[`output_format_flatbuffers_string_as_string`](/reference/settings/formats/output-format#output_format_flatbuffers_string_as_string)
to serialize them as FlexBuffers `String` instead; the bytes are written verbatim, so it is the
user's responsibility to ensure they are valid UTF-8. `UUID` is always serialized as its
canonical text form, which is plain ASCII.

## Example usage {#example-usage}

```bash
$ clickhouse-client --query="SELECT number, toString(number) FROM numbers(10) FORMAT Flatbuffers" > tmp.fb;
```

## Format settings {#format-settings}

| Setting                                                                                                                                       | Description                                                                              | Default |
|-----------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------|---------|
| [`output_format_flatbuffers_string_as_string`](/reference/settings/formats/output-format#output_format_flatbuffers_string_as_string) | serialize `String`/`FixedString` columns as FlexBuffers String instead of the default Blob. | `false` |
)DOCS_MD",
        .examples = {{"Export to a file", "SELECT number, toString(number) FROM numbers(10) FORMAT Flatbuffers", ""}},
        .introduced_in = {26, 8},
        .related = {"MsgPack", "RowBinary", "Native"},
    });
}

}

#else

namespace DB
{
class FormatFactory;
void registerOutputFormatFlatbuffers(FormatFactory &);
void registerOutputFormatFlatbuffers(FormatFactory &)
{
}
}

#endif
