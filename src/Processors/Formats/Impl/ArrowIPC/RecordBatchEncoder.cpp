#include <Processors/Formats/Impl/ArrowIPC/RecordBatchEncoder.h>

#if USE_ARROW

#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/IDataType.h>
#include <IO/NetUtils.h>
#include <Core/UUID.h>
#include <Common/assert_cast.h>

#include <algorithm>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_LARGE_ARRAY_SIZE;
}
}

namespace DB::ArrowIPC
{

void RecordBatchEncoder::appendBuffer(const void * data, size_t length)
{
    while (body.size() % 8 != 0)
        body.push_back('\0');
    const size_t offset = body.size();
    buffers.emplace_back(static_cast<int64_t>(offset), static_cast<int64_t>(length));
    if (length > 0)
    {
        body.resize(offset + length);
        memcpy(body.data() + offset, data, length);
    }
}

void RecordBatchEncoder::appendEmptyBuffer()
{
    while (body.size() % 8 != 0)
        body.push_back('\0');
    buffers.emplace_back(static_cast<int64_t>(body.size()), 0);
}

int64_t RecordBatchEncoder::appendValidity(const IColumn * null_map_column, size_t num_rows)
{
    if (!null_map_column)
    {
        /// Non-nullable: emit a zero-length validity buffer (the slot still exists in the layout).
        appendEmptyBuffer();
        return 0;
    }

    const auto & null_map = assert_cast<const ColumnUInt8 &>(*null_map_column).getData();
    PODArray<char> bitmap((num_rows + 7) / 8, 0);
    int64_t null_count = 0;
    for (size_t i = 0; i < num_rows; ++i)
    {
        if (null_map[i])
            ++null_count;
        else
            bitmap[i >> 3] = static_cast<char>(bitmap[i >> 3] | (1 << (i & 7))); /// Arrow validity bit: 1 = valid.
    }
    appendBuffer(bitmap.data(), bitmap.size());
    return null_count;
}

void RecordBatchEncoder::appendOffsets(const IColumn::Offsets & ch_offsets, size_t num_rows)
{
    PODArray<int32_t> arrow_offsets(num_rows + 1);
    arrow_offsets[0] = 0;
    for (size_t i = 0; i < num_rows; ++i)
    {
        if (ch_offsets[i] > static_cast<UInt64>(std::numeric_limits<int32_t>::max()))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Arrow IPC offset {} exceeds 32 bits", ch_offsets[i]);
        arrow_offsets[i + 1] = static_cast<int32_t>(ch_offsets[i]);
    }
    appendBuffer(arrow_offsets.data(), (num_rows + 1) * sizeof(int32_t));
}

namespace
{

template <typename Col>
void appendFixedWidth(RecordBatchEncoder & /*self*/, const IColumn & column, size_t num_rows, PODArray<char> & out)
{
    using V = typename Col::ValueType;
    const auto & data = assert_cast<const Col &>(column).getData();
    out.resize(num_rows * sizeof(V));
    if (num_rows)
        memcpy(out.data(), data.data(), num_rows * sizeof(V));
}

int arrowTimeUnitForScale(UInt32 scale)
{
    if (scale == 0)
        return flatbuf::TimeUnit_SECOND;
    if (scale <= 3)
        return flatbuf::TimeUnit_MILLISECOND;
    if (scale <= 6)
        return flatbuf::TimeUnit_MICROSECOND;
    return flatbuf::TimeUnit_NANOSECOND;
}

}

void RecordBatchEncoder::encodeValues(const IColumn & column, const DataTypePtr & type, size_t num_rows)
{
    const WhichDataType which(type);

    if (isBool(type))
    {
        /// Pack the 0/1 UInt8 column into an Arrow bit-packed boolean buffer.
        const auto & data = assert_cast<const ColumnUInt8 &>(column).getData();
        PODArray<char> bitmap((num_rows + 7) / 8, 0);
        for (size_t i = 0; i < num_rows; ++i)
            if (data[i])
                bitmap[i >> 3] = static_cast<char>(bitmap[i >> 3] | (1 << (i & 7)));
        appendBuffer(bitmap.data(), bitmap.size());
        return;
    }

    PODArray<char> buf;
    switch (which.idx)
    {
        case TypeIndex::UInt8: appendFixedWidth<ColumnUInt8>(*this, column, num_rows, buf); break;
        case TypeIndex::UInt16: appendFixedWidth<ColumnUInt16>(*this, column, num_rows, buf); break;
        case TypeIndex::UInt32: appendFixedWidth<ColumnUInt32>(*this, column, num_rows, buf); break;
        case TypeIndex::UInt64: appendFixedWidth<ColumnUInt64>(*this, column, num_rows, buf); break;
        case TypeIndex::Int8: appendFixedWidth<ColumnInt8>(*this, column, num_rows, buf); break;
        case TypeIndex::Enum8: appendFixedWidth<ColumnInt8>(*this, column, num_rows, buf); break;
        case TypeIndex::Int16: appendFixedWidth<ColumnInt16>(*this, column, num_rows, buf); break;
        case TypeIndex::Enum16: appendFixedWidth<ColumnInt16>(*this, column, num_rows, buf); break;
        case TypeIndex::Int32: appendFixedWidth<ColumnInt32>(*this, column, num_rows, buf); break;
        case TypeIndex::Int64: appendFixedWidth<ColumnInt64>(*this, column, num_rows, buf); break;
        case TypeIndex::Float32: appendFixedWidth<ColumnFloat32>(*this, column, num_rows, buf); break;
        case TypeIndex::Float64: appendFixedWidth<ColumnFloat64>(*this, column, num_rows, buf); break;
        case TypeIndex::Date32: appendFixedWidth<ColumnInt32>(*this, column, num_rows, buf); break;
        case TypeIndex::DateTime: appendFixedWidth<ColumnUInt32>(*this, column, num_rows, buf); break;
        case TypeIndex::Date:
        {
            /// Date is UInt16 days; widen to Int32 to match Arrow date32.
            const auto & data = assert_cast<const ColumnUInt16 &>(column).getData();
            PODArray<int32_t> widened(num_rows);
            for (size_t i = 0; i < num_rows; ++i)
                widened[i] = static_cast<int32_t>(data[i]);
            appendBuffer(widened.data(), num_rows * sizeof(int32_t));
            return;
        }
        case TypeIndex::DateTime64:
        {
            const UInt32 scale = assert_cast<const DataTypeDateTime64 &>(*type).getScale();
            const UInt32 target_scale = static_cast<UInt32>(arrowTimeUnitForScale(scale)) * 3;
            Int64 factor = 1;
            for (UInt32 i = scale; i < target_scale; ++i)
                factor *= 10;
            const auto & data = assert_cast<const ColumnDecimal<DateTime64> &>(column).getData();
            PODArray<Int64> values(num_rows);
            for (size_t i = 0; i < num_rows; ++i)
                values[i] = data[i].value * factor;
            appendBuffer(values.data(), num_rows * sizeof(Int64));
            return;
        }
        case TypeIndex::Decimal32: case TypeIndex::Decimal64: case TypeIndex::Decimal128: case TypeIndex::Decimal256:
        {
            const size_t arrow_width = which.idx == TypeIndex::Decimal256 ? 32 : 16;
            PODArray<char> out(num_rows * arrow_width, 0);
            auto emit = [&](const auto & col)
            {
                using ValueType = typename std::decay_t<decltype(col)>::ValueType;
                const size_t ch_width = sizeof(ValueType);
                const auto & data = col.getData();
                for (size_t i = 0; i < num_rows; ++i)
                {
                    const char * src = reinterpret_cast<const char *>(&data[i]);
                    char * dst = out.data() + i * arrow_width;
                    memcpy(dst, src, ch_width);
                    /// Sign-extend to the wider Arrow representation (little-endian two's complement).
                    const char sign = (src[ch_width - 1] & static_cast<char>(0x80)) ? static_cast<char>(0xFF) : 0;
                    memset(dst + ch_width, sign, arrow_width - ch_width);
                }
            };
            switch (which.idx)
            {
                case TypeIndex::Decimal32: emit(assert_cast<const ColumnDecimal<Decimal32> &>(column)); break;
                case TypeIndex::Decimal64: emit(assert_cast<const ColumnDecimal<Decimal64> &>(column)); break;
                case TypeIndex::Decimal128: emit(assert_cast<const ColumnDecimal<Decimal128> &>(column)); break;
                default: emit(assert_cast<const ColumnDecimal<Decimal256> &>(column)); break;
            }
            appendBuffer(out.data(), out.size());
            return;
        }
        case TypeIndex::String:
        {
            const auto & cs = assert_cast<const ColumnString &>(column);
            const auto & chars = cs.getChars();
            const auto & offs = cs.getOffsets();
            PODArray<int32_t> arrow_offsets(num_rows + 1);
            PODArray<char> data;
            arrow_offsets[0] = 0;
            int64_t cur = 0;
            for (size_t i = 0; i < num_rows; ++i)
            {
                const size_t start = i == 0 ? 0 : offs[i - 1];
                const size_t len = offs[i] - start; /// ClickHouse ColumnString stores no trailing '\0'
                cur += static_cast<int64_t>(len);
                if (cur > std::numeric_limits<int32_t>::max())
                    throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Arrow IPC string offset exceeds 32 bits");
                const size_t old = data.size();
                data.resize(old + len);
                if (len)
                    memcpy(data.data() + old, &chars[start], len);
                arrow_offsets[i + 1] = static_cast<int32_t>(cur);
            }
            appendBuffer(arrow_offsets.data(), (num_rows + 1) * sizeof(int32_t));
            appendBuffer(data.data(), data.size());
            return;
        }
        case TypeIndex::FixedString:
        {
            const auto & cfs = assert_cast<const ColumnFixedString &>(column);
            appendBuffer(cfs.getChars().data(), cfs.getChars().size());
            return;
        }
        case TypeIndex::IPv4: appendFixedWidth<ColumnVector<IPv4>>(*this, column, num_rows, buf); break;
        case TypeIndex::Int128: appendFixedWidth<ColumnVector<Int128>>(*this, column, num_rows, buf); break;
        case TypeIndex::UInt128: appendFixedWidth<ColumnVector<UInt128>>(*this, column, num_rows, buf); break;
        case TypeIndex::Int256: appendFixedWidth<ColumnVector<Int256>>(*this, column, num_rows, buf); break;
        case TypeIndex::UInt256: appendFixedWidth<ColumnVector<UInt256>>(*this, column, num_rows, buf); break;
        case TypeIndex::IPv6: appendFixedWidth<ColumnVector<IPv6>>(*this, column, num_rows, buf); break;
        case TypeIndex::Interval: appendFixedWidth<ColumnVector<Int64>>(*this, column, num_rows, buf); break;
        case TypeIndex::UUID:
        {
            /// fixed_size_binary(16) with the two 64-bit halves byte-reversed, matching the library writer.
            const auto & data = assert_cast<const ColumnVector<UUID> &>(column).getData();
            PODArray<char> out(num_rows * 16);
            for (size_t i = 0; i < num_rows; ++i)
            {
                UUID value = data[i];
                auto * bytes = reinterpret_cast<uint8_t *>(&value);
                std::reverse(bytes, bytes + 8);
                std::reverse(bytes + 8, bytes + 16);
                memcpy(out.data() + i * 16, bytes, 16);
            }
            appendBuffer(out.data(), out.size());
            return;
        }
        case TypeIndex::Array:
        {
            const auto & ca = assert_cast<const ColumnArray &>(column);
            appendOffsets(ca.getOffsets(), num_rows);
            const DataTypePtr & element_type = assert_cast<const DataTypeArray &>(*type).getNestedType();
            encodeField(ca.getData(), element_type, ca.getData().size());
            return;
        }
        case TypeIndex::Tuple:
        {
            const auto & ct = assert_cast<const ColumnTuple &>(column);
            const auto & elem_types = assert_cast<const DataTypeTuple &>(*type).getElements();
            for (size_t i = 0; i < elem_types.size(); ++i)
                encodeField(ct.getColumn(i), elem_types[i], num_rows);
            return;
        }
        case TypeIndex::Map:
        {
            const auto & cm = assert_cast<const ColumnMap &>(column);
            const auto & arr = cm.getNestedColumn(); /// ColumnArray(ColumnTuple(key, value))
            appendOffsets(arr.getOffsets(), num_rows);

            const auto & entries = assert_cast<const ColumnTuple &>(arr.getData());
            const size_t entries_rows = entries.size();
            /// The entries struct node (non-nullable), then its key and value children.
            nodes.emplace_back(static_cast<int64_t>(entries_rows), 0);
            appendEmptyBuffer();
            const auto & map_type = assert_cast<const DataTypeMap &>(*type);
            encodeField(entries.getColumn(0), map_type.getKeyType(), entries_rows);
            encodeField(entries.getColumn(1), map_type.getValueType(), entries_rows);
            return;
        }
        default:
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Native Arrow IPC writer does not support encoding type {} yet", type->getName());
    }

    appendBuffer(buf.data(), buf.size());
}

void RecordBatchEncoder::encodeField(const IColumn & column, const DataTypePtr & type, size_t num_rows)
{
    if (isColumnConst(column))
    {
        auto full = column.convertToFullColumnIfConst();
        encodeField(*full, type, num_rows);
        return;
    }

    if (type->lowCardinality())
    {
        /// Materialize LowCardinality to its full column (matches output_format_arrow_low_cardinality_as_dictionary=false).
        auto full = column.convertToFullColumnIfLowCardinality();
        encodeField(*full, removeLowCardinality(type), num_rows);
        return;
    }

    const IColumn * nested = &column;
    const IColumn * null_map_column = nullptr;
    DataTypePtr nested_type = type;
    if (type->isNullable())
    {
        const auto & nullable = assert_cast<const ColumnNullable &>(column);
        null_map_column = &nullable.getNullMapColumn();
        nested = &nullable.getNestedColumn();
        nested_type = removeNullable(type);
    }

    int64_t null_count = 0;
    if (null_map_column)
    {
        const auto & null_map = assert_cast<const ColumnUInt8 &>(*null_map_column).getData();
        for (size_t i = 0; i < num_rows; ++i)
            null_count += null_map[i] ? 1 : 0;
    }

    nodes.emplace_back(static_cast<int64_t>(num_rows), null_count);
    appendValidity(null_map_column, num_rows);
    encodeValues(*nested, nested_type, num_rows);
}

RecordBatchEncoder::EncodedBatch RecordBatchEncoder::encode(const Columns & columns, const DataTypes & types, size_t num_rows)
{
    nodes.clear();
    buffers.clear();
    body.clear();

    for (size_t i = 0; i < columns.size(); ++i)
        encodeField(*columns[i], types[i], num_rows);

    /// Pad the body so its total length is a multiple of 8.
    while (body.size() % 8 != 0)
        body.push_back('\0');

    EncodedBatch result;
    result.num_rows = static_cast<int64_t>(num_rows);

    if (settings.arrow.output_compression_method == FormatSettings::ArrowCompression::NONE)
    {
        result.nodes = std::move(nodes);
        result.buffers = std::move(buffers);
        result.body = std::move(body);
    }
    else
    {
        const auto codec = settings.arrow.output_compression_method == FormatSettings::ArrowCompression::ZSTD
            ? CompressionCodec::Zstd : CompressionCodec::Lz4Frame;
        const int level = codec == CompressionCodec::Zstd ? 1 : 0;
        result.codec = codec;
        result.nodes = std::move(nodes);

        /// Each buffer becomes an 8-byte little-endian uncompressed length followed by the compressed
        /// bytes; if compression does not shrink it, store it uncompressed with a -1 length prefix.
        for (const auto & buffer : buffers)
        {
            const char * src = body.data() + buffer.offset();
            const size_t len = static_cast<size_t>(buffer.length());

            while (result.body.size() % 8 != 0)
                result.body.push_back('\0');
            const size_t dst_offset = result.body.size();

            if (len == 0)
            {
                result.buffers.emplace_back(static_cast<int64_t>(dst_offset), 0);
                continue;
            }

            auto compressed = compressBuffer(codec, src, len, level);
            int64_t prefix;
            const char * payload;
            size_t payload_size;
            if (compressed.size() < len)
            {
                prefix = DB::toLittleEndian(static_cast<int64_t>(len));
                payload = compressed.data();
                payload_size = compressed.size();
            }
            else
            {
                prefix = DB::toLittleEndian(static_cast<int64_t>(-1));
                payload = src;
                payload_size = len;
            }

            const size_t total = sizeof(int64_t) + payload_size;
            result.body.resize(dst_offset + total);
            memcpy(result.body.data() + dst_offset, &prefix, sizeof(prefix));
            memcpy(result.body.data() + dst_offset + sizeof(int64_t), payload, payload_size);
            result.buffers.emplace_back(static_cast<int64_t>(dst_offset), static_cast<int64_t>(total));
        }
        while (result.body.size() % 8 != 0)
            result.body.push_back('\0');
    }

    nodes = {};
    buffers = {};
    body = {};
    return result;
}

}

#endif
