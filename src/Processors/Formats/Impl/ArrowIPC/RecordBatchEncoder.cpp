#include <Processors/Formats/Impl/ArrowIPC/RecordBatchEncoder.h>

#if USE_ARROW

#include <Columns/ColumnVector.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/IDataType.h>
#include <IO/NetUtils.h>
#include <Core/UUID.h>
#include <Common/assert_cast.h>
#include <base/arithmeticOverflow.h>

#include <algorithm>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int DECIMAL_OVERFLOW;
}
}

namespace DB::ArrowIPC
{

namespace
{

/// Pack one bit per input byte into `out` (size (num_rows + 7) / 8, zero-initialised by the caller):
/// bit = 1 where the byte is non-zero, optionally inverted. Arrow validity uses invert = true (1 = not-null),
/// Bool uses invert = false. Processes 64 bytes per step via `bytes64MaskToBits64Mask`, scalar for the tail.
void packBytesToBits(const UInt8 * bytes, size_t num_rows, char * out, bool invert)
{
    auto * bits = reinterpret_cast<UInt8 *>(out);
    const UInt64 flip = invert ? ~UInt64(0) : 0;
    size_t i = 0;
    for (; i + 64 <= num_rows; i += 64)
    {
        /// `word` holds row i+0 in its least significant bit; store little-endian so the byte covering rows
        /// i..i+7 lands at bits[i/8] on both little- and big-endian hosts.
        const UInt64 word = bytes64MaskToBits64Mask(bytes + i) ^ flip;
        const UInt64 le = DB::toLittleEndian(word);
        memcpy(bits + i / 8, &le, sizeof(le));
    }
    for (; i < num_rows; ++i)
    {
        if ((bytes[i] != 0) != invert)
            bits[i >> 3] |= static_cast<UInt8>(1) << (i & 7);
    }
}

}

void RecordBatchEncoder::appendBuffer(const void * data, size_t length)
{
    while (body.size() % 8 != 0)
        body.push_back('\0');
    const size_t offset = body.size();
    buffers.emplace_back(static_cast<Int64>(offset), static_cast<Int64>(length));
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
    buffers.emplace_back(static_cast<Int64>(body.size()), 0);
}

Int64 RecordBatchEncoder::appendValidity(const IColumn * null_map_column, size_t num_rows)
{
    if (!null_map_column)
    {
        /// Non-nullable: emit a zero-length validity buffer (the slot still exists in the layout).
        appendEmptyBuffer();
        return 0;
    }

    const auto & null_map = assert_cast<const ColumnUInt8 &>(*null_map_column).getData();
    PODArray<char> bitmap((num_rows + 7) / 8, 0);
    packBytesToBits(null_map.data(), num_rows, bitmap.data(), /*invert=*/true); /// Arrow validity bit: 1 = valid.
    appendBuffer(bitmap.data(), bitmap.size());
    return countBytesInFilter(null_map.data(), 0, num_rows);
}

void RecordBatchEncoder::appendOffsets(const IColumn::Offsets & ch_offsets, size_t num_rows)
{
    PODArray<Int32> arrow_offsets(num_rows + 1);
    arrow_offsets[0] = 0;
    for (size_t i = 0; i < num_rows; ++i)
    {
        if (ch_offsets[i] > static_cast<UInt64>(std::numeric_limits<Int32>::max()))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Arrow IPC offset {} exceeds 32 bits", ch_offsets[i]);
        arrow_offsets[i + 1] = static_cast<Int32>(ch_offsets[i]);
    }
    appendBuffer(arrow_offsets.data(), (num_rows + 1) * sizeof(Int32));
}

namespace
{

/// Appends a fixed-width column's contiguous data straight into the body (a single copy, no temporary).
template <typename Col>
void appendFixedWidth(RecordBatchEncoder & self, const IColumn & column, size_t num_rows)
{
    using V = typename Col::ValueType;
    const auto & data = assert_cast<const Col &>(column).getData();
    self.appendBuffer(data.data(), num_rows * sizeof(V));
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

void RecordBatchEncoder::encodeValues(
    const IColumn & column, const DataTypePtr & type, size_t num_rows, const IColumn * null_map_column)
{
    const WhichDataType which(type);

    if (isBool(type))
    {
        /// Pack the 0/1 UInt8 column into an Arrow bit-packed boolean buffer.
        const auto & data = assert_cast<const ColumnUInt8 &>(column).getData();
        PODArray<char> bitmap((num_rows + 7) / 8, 0);
        packBytesToBits(data.data(), num_rows, bitmap.data(), /*invert=*/false);
        appendBuffer(bitmap.data(), bitmap.size());
        return;
    }

    switch (which.idx)
    {
        case TypeIndex::UInt8: appendFixedWidth<ColumnUInt8>(*this, column, num_rows); return;
        case TypeIndex::UInt16: appendFixedWidth<ColumnUInt16>(*this, column, num_rows); return;
        case TypeIndex::UInt32: appendFixedWidth<ColumnUInt32>(*this, column, num_rows); return;
        case TypeIndex::UInt64: appendFixedWidth<ColumnUInt64>(*this, column, num_rows); return;
        case TypeIndex::Int8: appendFixedWidth<ColumnInt8>(*this, column, num_rows); return;
        case TypeIndex::Enum8: appendFixedWidth<ColumnInt8>(*this, column, num_rows); return;
        case TypeIndex::Int16: appendFixedWidth<ColumnInt16>(*this, column, num_rows); return;
        case TypeIndex::Enum16: appendFixedWidth<ColumnInt16>(*this, column, num_rows); return;
        case TypeIndex::Int32: appendFixedWidth<ColumnInt32>(*this, column, num_rows); return;
        case TypeIndex::Int64: appendFixedWidth<ColumnInt64>(*this, column, num_rows); return;
        case TypeIndex::Float32: appendFixedWidth<ColumnFloat32>(*this, column, num_rows); return;
        case TypeIndex::Float64: appendFixedWidth<ColumnFloat64>(*this, column, num_rows); return;
        case TypeIndex::Date32: appendFixedWidth<ColumnInt32>(*this, column, num_rows); return;
        case TypeIndex::DateTime: appendFixedWidth<ColumnUInt32>(*this, column, num_rows); return;
        case TypeIndex::Date:
        {
            if (settings.arrow.output_date_as_uint16)
            {
                /// Backwards-compatible mode: write Date as a plain uint16.
                appendFixedWidth<ColumnUInt16>(*this, column, num_rows);
                return;
            }
            /// Date is UInt16 days; widen to Int32 to match Arrow date32.
            const auto & data = assert_cast<const ColumnUInt16 &>(column).getData();
            PODArray<Int32> widened(num_rows);
            for (size_t i = 0; i < num_rows; ++i)
                widened[i] = static_cast<Int32>(data[i]);
            appendBuffer(widened.data(), num_rows * sizeof(Int32));
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
            const NullMap * null_map
                = null_map_column ? &assert_cast<const ColumnUInt8 &>(*null_map_column).getData() : nullptr;
            PODArray<Int64> values(num_rows);
            for (size_t i = 0; i < num_rows; ++i)
            {
                /// A null row carries an arbitrary nested value that the validity bitmap already masks as
                /// NULL; do not rescale it (matching the Apache Arrow writer), so junk in a NULL row cannot
                /// raise DECIMAL_OVERFLOW for a value that is emitted as NULL anyway.
                if (null_map && (*null_map)[i])
                {
                    values[i] = 0;
                    continue;
                }
                /// Widening to a coarser Arrow unit multiplies the value; guard against silent wraparound,
                /// matching the Apache Arrow writer which raises `DECIMAL_OVERFLOW` for the same case.
                if (common::mulOverflow(data[i].value, factor, values[i]))
                    throw Exception(
                        ErrorCodes::DECIMAL_OVERFLOW, "Decimal value is too large to convert to the Arrow timestamp scale");
            }
            appendBuffer(values.data(), num_rows * sizeof(Int64));
            return;
        }
        case TypeIndex::Time:
            /// ClickHouse `Time` is seconds-of-day stored as Int32 → Arrow `time32[s]` (4-byte values).
            appendFixedWidth<ColumnInt32>(*this, column, num_rows);
            return;
        case TypeIndex::Time64:
        {
            /// `Time64(scale)` → Arrow `time32` (scale <= 3) or `time64`, rescaling the value up to the
            /// Arrow unit's scale, mirroring the library writer (`fillArrowArrayWithTime64ColumnData`).
            const UInt32 scale = assert_cast<const DataTypeTime64 &>(*type).getScale();
            const int unit = arrowTimeUnitForScale(scale);
            const UInt32 target_scale = static_cast<UInt32>(unit) * 3;
            Int64 factor = 1;
            for (UInt32 i = scale; i < target_scale; ++i)
                factor *= 10;
            const bool to_time32 = (unit == flatbuf::TimeUnit_SECOND || unit == flatbuf::TimeUnit_MILLISECOND);
            const auto & data = assert_cast<const ColumnDecimal<Time64> &>(column).getData();
            const NullMap * null_map
                = null_map_column ? &assert_cast<const ColumnUInt8 &>(*null_map_column).getData() : nullptr;
            if (to_time32)
            {
                PODArray<Int32> values(num_rows);
                for (size_t i = 0; i < num_rows; ++i)
                {
                    /// A null row's arbitrary value is masked by the validity bitmap; do not rescale or
                    /// range-check it (matching the library writer's `AppendNull`).
                    if (null_map && (*null_map)[i])
                    {
                        values[i] = 0;
                        continue;
                    }
                    Int64 value = data[i].value;
                    if (common::mulOverflow(value, factor, value)
                        || value > std::numeric_limits<Int32>::max() || value < std::numeric_limits<Int32>::min())
                        throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Decimal math overflow");
                    values[i] = static_cast<Int32>(value);
                }
                appendBuffer(values.data(), num_rows * sizeof(Int32));
            }
            else
            {
                PODArray<Int64> values(num_rows);
                for (size_t i = 0; i < num_rows; ++i)
                {
                    if (null_map && (*null_map)[i])
                    {
                        values[i] = 0;
                        continue;
                    }
                    if (common::mulOverflow(data[i].value, factor, values[i]))
                        throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Decimal math overflow");
                }
                appendBuffer(values.data(), num_rows * sizeof(Int64));
            }
            return;
        }
        case TypeIndex::Decimal32: case TypeIndex::Decimal64: case TypeIndex::Decimal128: case TypeIndex::Decimal256:
        {
            const size_t arrow_width = which.idx == TypeIndex::Decimal256 ? 32 : 16;
            /// Every byte of every slot is written below (value bytes + sign extension), so no zero-init.
            PODArray<char> out(num_rows * arrow_width);
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
            const NullMap * null_map
                = null_map_column ? &assert_cast<const ColumnUInt8 &>(*null_map_column).getData() : nullptr;
            PODArray<Int32> arrow_offsets(num_rows + 1);
            PODArray<char> data;
            arrow_offsets[0] = 0;
            Int64 cur = 0;
            for (size_t i = 0; i < num_rows; ++i)
            {
                /// A null row carries an arbitrary nested value that the validity bitmap already masks as
                /// NULL; emit a zero-length slot for it (matching the Apache Arrow writer's `AppendNull`)
                /// so the bytes of a logically-NULL row never reach the IPC body.
                if (!(null_map && (*null_map)[i]))
                {
                    const size_t start = i == 0 ? 0 : offs[i - 1];
                    const size_t len = offs[i] - start; /// ClickHouse ColumnString stores no trailing '\0'
                    cur += static_cast<Int64>(len);
                    if (cur > std::numeric_limits<Int32>::max())
                        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Arrow IPC string offset exceeds 32 bits");
                    const size_t old = data.size();
                    data.resize(old + len);
                    if (len)
                        memcpy(data.data() + old, &chars[start], len);
                }
                arrow_offsets[i + 1] = static_cast<Int32>(cur);
            }
            appendBuffer(arrow_offsets.data(), (num_rows + 1) * sizeof(Int32));
            appendBuffer(data.data(), data.size());
            return;
        }
        case TypeIndex::FixedString:
        {
            const auto & cfs = assert_cast<const ColumnFixedString &>(column);
            if (settings.arrow.output_fixed_string_as_fixed_byte_array)
            {
                appendBuffer(cfs.getChars().data(), num_rows * cfs.getN());
                return;
            }
            /// `output_fixed_string_as_fixed_byte_array = 0`: the schema advertises a variable-width
            /// Utf8/Binary, so emit the int32 offsets buffer (each non-null row is exactly N bytes) and the
            /// data buffer, instead of a single fixed-width values buffer.
            const size_t n = cfs.getN();
            const auto & chars = cfs.getChars();
            const NullMap * null_map
                = null_map_column ? &assert_cast<const ColumnUInt8 &>(*null_map_column).getData() : nullptr;
            PODArray<Int32> arrow_offsets(num_rows + 1);
            arrow_offsets[0] = 0;
            if (!null_map)
            {
                /// No null map: every row contributes exactly N bytes, so the offsets are regular and the
                /// nested buffer can be copied in one shot.
                for (size_t i = 0; i < num_rows; ++i)
                {
                    const UInt64 end = static_cast<UInt64>(i + 1) * n;
                    if (end > static_cast<UInt64>(std::numeric_limits<Int32>::max()))
                        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Arrow IPC string offset exceeds 32 bits");
                    arrow_offsets[i + 1] = static_cast<Int32>(end);
                }
                appendBuffer(arrow_offsets.data(), (num_rows + 1) * sizeof(Int32));
                appendBuffer(chars.data(), num_rows * n);
                return;
            }
            /// A null row carries an arbitrary nested value that the validity bitmap already masks as NULL;
            /// emit a zero-length slot for it (matching the Apache Arrow writer's `AppendNull`, and the
            /// `String` path above) so the bytes of a logically-NULL row never reach the IPC body.
            PODArray<char> data;
            Int64 cur = 0;
            for (size_t i = 0; i < num_rows; ++i)
            {
                if (!(*null_map)[i])
                {
                    cur += static_cast<Int64>(n);
                    if (cur > std::numeric_limits<Int32>::max())
                        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Arrow IPC string offset exceeds 32 bits");
                    const size_t old = data.size();
                    data.resize(old + n);
                    if (n)
                        memcpy(data.data() + old, &chars[i * n], n);
                }
                arrow_offsets[i + 1] = static_cast<Int32>(cur);
            }
            appendBuffer(arrow_offsets.data(), (num_rows + 1) * sizeof(Int32));
            appendBuffer(data.data(), data.size());
            return;
        }
        case TypeIndex::IPv4: appendFixedWidth<ColumnVector<IPv4>>(*this, column, num_rows); return;
        case TypeIndex::Int128: appendFixedWidth<ColumnVector<Int128>>(*this, column, num_rows); return;
        case TypeIndex::UInt128: appendFixedWidth<ColumnVector<UInt128>>(*this, column, num_rows); return;
        case TypeIndex::Int256: appendFixedWidth<ColumnVector<Int256>>(*this, column, num_rows); return;
        case TypeIndex::UInt256: appendFixedWidth<ColumnVector<UInt256>>(*this, column, num_rows); return;
        case TypeIndex::IPv6: appendFixedWidth<ColumnVector<IPv6>>(*this, column, num_rows); return;
        case TypeIndex::Interval: appendFixedWidth<ColumnVector<Int64>>(*this, column, num_rows); return;
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
            nodes.emplace_back(static_cast<Int64>(entries_rows), 0);
            appendEmptyBuffer();
            const auto & map_type = assert_cast<const DataTypeMap &>(*type);
            encodeField(entries.getColumn(0), map_type.getKeyType(), entries_rows);
            encodeField(entries.getColumn(1), map_type.getValueType(), entries_rows);
            return;
        }
        default:
            /// A type with no first-class Arrow mapping. Mirror the Apache Arrow library writer: when
            /// `output_format_arrow_unsupported_types_as_binary` is set, write its raw per-row bytes as an
            /// Arrow `Binary` column (read back as `String`); otherwise reject it.
            if (settings.arrow.output_unsupported_types_as_binary)
            {
                encodeAsBinary(column, num_rows, null_map_column);
                return;
            }
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Native Arrow IPC writer does not support encoding type {}. Set "
                "output_format_arrow_unsupported_types_as_binary = 1 to write it as binary",
                type->getName());
    }
}

void RecordBatchEncoder::encodeAsBinary(const IColumn & column, size_t num_rows, const IColumn * null_map_column)
{
    const NullMap * null_map
        = null_map_column ? &assert_cast<const ColumnUInt8 &>(*null_map_column).getData() : nullptr;
    PODArray<Int32> arrow_offsets(num_rows + 1);
    arrow_offsets[0] = 0;
    PODArray<char> data;
    size_t total = 0;
    for (size_t i = 0; i < num_rows; ++i)
    {
        /// Skip the payload of a logically-NULL row (matching the Apache Arrow writer's `AppendNull`):
        /// emit a zero-length slot instead of the arbitrary bytes the null row may carry.
        if (!(null_map && (*null_map)[i]))
        {
            const std::string_view value = column.getDataAt(i);
            total += value.size();
            if (total > static_cast<size_t>(std::numeric_limits<Int32>::max()))
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Arrow IPC binary offset exceeds 32 bits");
            data.insert(data.end(), value.data(), value.data() + value.size());
        }
        arrow_offsets[i + 1] = static_cast<Int32>(total);
    }
    appendBuffer(arrow_offsets.data(), (num_rows + 1) * sizeof(Int32));
    appendBuffer(data.data(), data.size());
}

void RecordBatchEncoder::encodeField(const IColumn & column, const DataTypePtr & type, size_t num_rows)
{
    if (isColumnConst(column))
    {
        auto full = column.convertToFullColumnIfConst();
        encodeField(*full, type, num_rows);
        return;
    }

    /// Materialize a lazily-replicated column (`ColumnReplicated`, produced by `JOIN`/`ARRAY JOIN` with
    /// `enable_lazy_columns_replication`) before the type-specific casts below, matching the Apache Arrow
    /// library writer's normalization. We could not reproduce such a column actually reaching this writer
    /// (the SELECT pipeline materializes it before the output format), so this is defensive parity with the
    /// library rather than a fix for an observed failure.
    if (column.isReplicated())
    {
        auto full = column.convertToFullColumnIfReplicated();
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

    /// Variant maps to an Arrow dense union, which (unlike every other type) has no validity buffer.
    if (isVariant(type))
    {
        encodeVariant(column, type, num_rows);
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

    /// `appendValidity` packs the validity buffer and returns the null count in one pass over the null map.
    const Int64 null_count = appendValidity(null_map_column, num_rows);
    nodes.emplace_back(static_cast<Int64>(num_rows), null_count);
    encodeValues(*nested, nested_type, num_rows, null_map_column);
}

void RecordBatchEncoder::encodeVariant(const IColumn & column, const DataTypePtr & type, size_t num_rows)
{
    const auto & variant_column = assert_cast<const ColumnVariant &>(column);
    const auto & variant_type = assert_cast<const DataTypeVariant &>(*type);
    const size_t num_variants = variant_type.getVariants().size();
    /// An Arrow dense-union type id is a signed int8 and the trailing NULL child uses id `num_variants`,
    /// so a Variant with more than 127 alternatives cannot be represented.
    if (num_variants > 127)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Native Arrow IPC writer does not support a Variant with {} alternatives (maximum is 127)", num_variants);
    const auto & local_discriminators = variant_column.getLocalDiscriminators();
    const auto & ch_offsets = variant_column.getOffsets();

    /// The union node itself: no nulls are reported here (NULL rows use the dedicated null child).
    nodes.emplace_back(static_cast<Int64>(num_rows), 0);

    PODArray<Int8> type_ids(num_rows);
    PODArray<Int32> value_offsets(num_rows);
    for (size_t row = 0; row < num_rows; ++row)
    {
        const auto local = local_discriminators[row];
        if (local == ColumnVariant::NULL_DISCRIMINATOR)
        {
            type_ids[row] = static_cast<Int8>(num_variants);
            value_offsets[row] = 0;
            continue;
        }
        type_ids[row] = static_cast<Int8>(variant_column.globalDiscriminatorByLocal(local));
        const UInt64 off = ch_offsets[row];
        if (off > static_cast<UInt64>(std::numeric_limits<Int32>::max()))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Arrow IPC dense union offset exceeds 32 bits");
        value_offsets[row] = static_cast<Int32>(off);
    }
    appendBuffer(type_ids.data(), num_rows * sizeof(Int8));
    appendBuffer(value_offsets.data(), num_rows * sizeof(Int32));

    /// Children in global discriminator order, then a trailing single-element null child for NULL rows.
    for (size_t i = 0; i < num_variants; ++i)
    {
        const IColumn & child = variant_column.getVariantByGlobalDiscriminator(i);
        encodeField(child, variant_type.getVariant(i), child.size());
    }
    nodes.emplace_back(1, 1);
}

RecordBatchEncoder::EncodedBatch RecordBatchEncoder::encode(const Columns & columns, const DataTypes & types, size_t num_rows)
{
    nodes.clear();
    buffers.clear();
    body.clear();

    /// Reserve the body up front so `appendBuffer` does not repeatedly reallocate and copy as it grows.
    /// The column byte sizes approximate the Arrow body size closely enough to serve as a capacity hint.
    size_t body_hint = 0;
    for (const auto & column : columns)
        body_hint += column->byteSize();
    body.reserve(body_hint);

    for (size_t i = 0; i < columns.size(); ++i)
        encodeField(*columns[i], types[i], num_rows);

    /// Pad the body so its total length is a multiple of 8.
    while (body.size() % 8 != 0)
        body.push_back('\0');

    EncodedBatch result;
    result.num_rows = static_cast<Int64>(num_rows);

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

        /// Compress every (non-empty) buffer independently. Like the Apache Arrow writer, do this in
        /// parallel across the shared format thread pool — compression CPU dominates here. The body is
        /// then assembled sequentially (each buffer: an 8-byte little-endian uncompressed length, then
        /// the compressed bytes; or, if compression does not shrink it, the raw bytes with a -1 prefix).
        const size_t n = buffers.size();
        VectorWithMemoryTracking<std::pair<const char *, size_t>> inputs(n);
        for (size_t i = 0; i < n; ++i)
            inputs[i] = {body.data() + buffers[i].offset(), static_cast<size_t>(buffers[i].length())};
        VectorWithMemoryTracking<PODArray<char>> compressed(n);
        compressBuffersParallel(codec, level, inputs, compressed);

        for (size_t i = 0; i < n; ++i)
        {
            const auto & buffer = buffers[i];
            const size_t len = static_cast<size_t>(buffer.length());

            while (result.body.size() % 8 != 0)
                result.body.push_back('\0');
            const size_t dst_offset = result.body.size();

            if (len == 0)
            {
                result.buffers.emplace_back(static_cast<Int64>(dst_offset), 0);
                continue;
            }

            Int64 prefix = 0;
            const char * payload = nullptr;
            size_t payload_size = 0;
            if (compressed[i].size() < len)
            {
                prefix = DB::toLittleEndian(static_cast<Int64>(len));
                payload = compressed[i].data();
                payload_size = compressed[i].size();
            }
            else
            {
                prefix = DB::toLittleEndian(static_cast<Int64>(-1));
                payload = body.data() + buffer.offset();
                payload_size = len;
            }

            const size_t total = sizeof(Int64) + payload_size;
            result.body.resize(dst_offset + total);
            memcpy(result.body.data() + dst_offset, &prefix, sizeof(prefix));
            memcpy(result.body.data() + dst_offset + sizeof(Int64), payload, payload_size);
            result.buffers.emplace_back(static_cast<Int64>(dst_offset), static_cast<Int64>(total));
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
