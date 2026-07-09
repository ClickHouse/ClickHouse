#include <Processors/Formats/Impl/ArrowIPC/RecordBatchDecoder.h>

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/BufferCompression.h>
#include <IO/NetUtils.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/IDataType.h>
#include <Common/assert_cast.h>
#include <Common/FloatUtils.h>
#include <Common/DateLUTImpl.h>
#include <Core/UUID.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include <algorithm>
#include <limits>
#include <unordered_map>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}
}

namespace DB::ArrowIPC
{

namespace
{
/// Rebuild `type` so its Nullable placement matches the decoded `column`. `fieldToCHType` decides whether a
/// Struct becomes `Nullable(Tuple)` from the setting alone, while `decodeField` also honours a nullable type
/// hint, so the two can disagree only in whether a struct is wrapped in Nullable. Walk both in parallel and
/// adopt the column's nullability, keeping the element names and other details from `type`.
DataTypePtr matchColumnNullability(const DataTypePtr & type, const ColumnPtr & column)
{
    const bool col_nullable = column->isNullable();
    const ColumnPtr & nested_col = col_nullable
        ? assert_cast<const ColumnNullable &>(*column).getNestedColumnPtr() : column;
    DataTypePtr t = removeNullable(type);

    if (const auto * arr_t = typeid_cast<const DataTypeArray *>(t.get()))
    {
        if (const auto * arr_c = typeid_cast<const ColumnArray *>(nested_col.get()))
            t = std::make_shared<DataTypeArray>(matchColumnNullability(arr_t->getNestedType(), arr_c->getDataPtr()));
    }
    else if (const auto * tup_t = typeid_cast<const DataTypeTuple *>(t.get()))
    {
        const auto * tup_c = typeid_cast<const ColumnTuple *>(nested_col.get());
        if (tup_c && tup_c->tupleSize() == tup_t->getElements().size())
        {
            DataTypes elems(tup_t->getElements().size());
            for (size_t i = 0; i < elems.size(); ++i)
                elems[i] = matchColumnNullability(tup_t->getElement(i), tup_c->getColumnPtr(i));
            t = tup_t->hasExplicitNames()
                ? std::make_shared<DataTypeTuple>(elems, tup_t->getElementNames())
                : std::make_shared<DataTypeTuple>(elems);
        }
    }
    else if (const auto * map_t = typeid_cast<const DataTypeMap *>(t.get()))
    {
        const auto * map_c = typeid_cast<const ColumnMap *>(nested_col.get());
        const auto * arr_c = map_c ? typeid_cast<const ColumnArray *>(map_c->getNestedColumnPtr().get()) : nullptr;
        const auto * tup_c = arr_c ? typeid_cast<const ColumnTuple *>(arr_c->getDataPtr().get()) : nullptr;
        if (tup_c && tup_c->tupleSize() == 2)
            t = std::make_shared<DataTypeMap>(
                matchColumnNullability(map_t->getKeyType(), tup_c->getColumnPtr(0)),
                matchColumnNullability(map_t->getValueType(), tup_c->getColumnPtr(1)));
    }

    if (col_nullable && t->canBeInsideNullable())
        return std::make_shared<DataTypeNullable>(t);
    return t;
}

/// Expand an Arrow LSB-first bitmap into one byte per row (0 or 1). With `invert` each output byte is
/// flipped, turning the validity bitmap (1 = valid) into a ClickHouse null map (1 = null). Unpacks 8 bits
/// at a time via SWAR (the same trick as the Parquet decoder), scalar for the trailing < 8 bits.
void expandBitmapToBytes(const uint8_t * bits, size_t rows, UInt8 * out, bool invert)
{
    const UInt64 flip = invert ? 0x0101010101010101ULL : 0;
    size_t i = 0;
    for (; i + 8 <= rows; i += 8)
    {
        UInt64 x = UInt64(bits[i / 8]);
        x = (x | (x << 28)) & 0x0000000f0000000ful;
        x = (x | (x << 14)) & 0x0003000300030003ul;
        x = (x | (x <<  7)) & 0x0101010101010101ul;
        x ^= flip;
        /// `x` holds row i+0 in its least significant byte; store little-endian so that byte lands at out[i]
        /// on both little- and big-endian hosts.
        x = DB::toLittleEndian(x);
        memcpy(out + i, &x, 8);
    }
    for (; i < rows; ++i)
    {
        const UInt8 bit = (bits[i >> 3] >> (i & 7)) & 1;
        out[i] = invert ? (bit ^ 1) : bit;
    }
}

/// Strips the outer `Nullable`/`LowCardinality` wrappers off a requested-type hint so the underlying
/// type (number, Array, Tuple, Map) can be inspected. Handles both `LowCardinality(Nullable(...))` and
/// `Nullable(LowCardinality(...))`.
DataTypePtr stripHint(const DataTypePtr & type)
{
    if (!type)
        return nullptr;
    return removeNullable(removeLowCardinality(removeNullable(type)));
}

/// The requested type hint for the element of an Array-like field, or null when the hint is not an Array.
DataTypePtr arrayElementHint(const DataTypePtr & hint)
{
    if (const auto * array = typeid_cast<const DataTypeArray *>(stripHint(hint).get()))
        return array->getNestedType();
    return nullptr;
}

/// The requested type hint for a struct child. For a named Tuple it is matched by element name — the same
/// way the later named-tuple CAST maps the struct, including case-insensitively when requested — and there
/// is no positional fallback (that could attach the hint to the wrong element). For an unnamed Tuple (the
/// synthetic Map-entries hint) it is matched by position. Null when the hint is not a Tuple or has no match.
DataTypePtr tupleElementHint(const DataTypePtr & hint, const String & child_name, size_t pos, bool case_insensitive)
{
    const auto * tuple = typeid_cast<const DataTypeTuple *>(stripHint(hint).get());
    if (!tuple)
        return nullptr;
    if (tuple->hasExplicitNames())
    {
        const auto & names = tuple->getElementNames();
        for (size_t i = 0; i < names.size(); ++i)
        {
            const bool match = case_insensitive ? boost::iequals(names[i], child_name) : names[i] == child_name;
            if (match)
                return tuple->getElements()[i];
        }
        return nullptr;
    }
    if (pos < tuple->getElements().size())
        return tuple->getElements()[pos];
    return nullptr;
}

/// A synthetic Tuple(key, value) hint for a Map's entries struct, or null when the hint is not a Map.
DataTypePtr mapEntriesHint(const DataTypePtr & hint)
{
    if (const auto * map = typeid_cast<const DataTypeMap *>(stripHint(hint).get()))
        return std::make_shared<DataTypeTuple>(DataTypes{map->getKeyType(), map->getValueType()});
    return nullptr;
}
}

void DictionaryRegistry::set(Int64 id, ColumnPtr values, bool is_delta)
{
    auto it = dictionaries.find(id);
    if (is_delta)
    {
        /// A delta dictionary batch appends to an existing dictionary; one whose id has no base
        /// dictionary yet is malformed — decoding indices against only the delta values would return
        /// wrong LowCardinality values. Reject it instead of treating the delta as a fresh dictionary.
        if (it == dictionaries.end())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC delta dictionary batch for unknown dictionary id {}", id);
        auto merged = IColumn::mutate(std::move(it->second));
        merged->insertRangeFrom(*values, 0, values->size());
        it->second = std::move(merged);
    }
    else
    {
        dictionaries[id] = std::move(values);
    }
}

ColumnPtr DictionaryRegistry::get(Int64 id) const
{
    auto it = dictionaries.find(id);
    if (it == dictionaries.end())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC record batch references unknown dictionary id {}", id);
    return it->second;
}

const flatbuf::FieldNode & RecordBatchDecoder::nextNode()
{
    const auto * nodes = current_batch->nodes();
    if (!nodes || node_index >= nodes->size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC record batch has fewer field nodes than the schema requires");
    return *nodes->Get(static_cast<flatbuffers::uoffset_t>(node_index++));
}

RecordBatchDecoder::Slice RecordBatchDecoder::nextBuffer()
{
    if (buffer_index >= buffer_slices.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC record batch has fewer buffers than the schema requires");
    return buffer_slices[buffer_index++];
}

namespace
{

void checkBufferSize(const RecordBatchDecoder::Slice & slice, size_t required, const char * what)
{
    if (static_cast<size_t>(slice.length) < required)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Arrow IPC {} buffer is too small: have {} bytes, need {}",
            what, slice.length, required);
}

/// Overflow-safe `count * elem_size` for buffer-size validation. An untrusted Arrow file can declare a
/// row count near 2^62; multiplying it by the element size would wrap modulo 2^64 to a small value,
/// letting an undersized buffer pass `checkBufferSize` and then driving an oversized column allocation
/// (resize/reserve). Reject the overflow as corrupt data before any allocation, mirroring the checked
/// arithmetic the Apache Arrow library based reader uses.
size_t requiredBytes(size_t count, size_t elem_size)
{
    size_t bytes = 0;
    if (__builtin_mul_overflow(count, elem_size, &bytes))
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Arrow IPC buffer size overflows: {} elements of {} bytes each", count, elem_size);
    return bytes;
}

/// Fills a fixed-width ClickHouse column (ColumnVector / ColumnDecimal) by copying `value_size`
/// bytes per row from the source buffer. For decimals `value_size` may be smaller than the Arrow
/// storage width, so the low (little-endian) bytes are taken per value.
template <typename Col>
void fillFixed(IColumn & column, size_t rows, const RecordBatchDecoder::Slice & values, size_t arrow_value_size)
{
    using V = typename Col::ValueType;
    checkBufferSize(values, requiredBytes(rows, arrow_value_size), "values");
    auto & data = assert_cast<Col &>(column).getData();
    data.resize(rows);
    if (rows == 0)
        return;
    if (arrow_value_size == sizeof(V))
    {
        memcpy(data.data(), values.ptr, rows * sizeof(V));
    }
    else
    {
        /// Decimal stored wider in Arrow than in ClickHouse: take the low bytes of each value.
        chassert(arrow_value_size > sizeof(V));
        auto * dst = reinterpret_cast<char *>(data.data());
        for (size_t i = 0; i < rows; ++i)
            memcpy(dst + i * sizeof(V), values.ptr + i * arrow_value_size, sizeof(V));
    }
}

}

ColumnPtr RecordBatchDecoder::buildNullMap(const Slice & validity, size_t rows, Int64 null_count) const
{
    auto null_map = ColumnUInt8::create(rows);
    auto & data = null_map->getData();

    /// A field with no nulls may omit the validity bitmap (a zero-length buffer): everything is valid.
    if (null_count == 0)
    {
        memset(data.data(), 0, rows);
        return null_map;
    }

    /// A non-zero (or unknown, i.e. negative) null count requires the validity bitmap to identify the
    /// null rows; accepting an absent bitmap here would silently turn malformed nullable data into real values.
    if (validity.length == 0)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Arrow IPC field declares a null count of {} but omits the validity bitmap", null_count);

    checkBufferSize(validity, (rows + 7) / 8, "validity");
    const auto * bits = reinterpret_cast<const uint8_t *>(validity.ptr);
    /// Arrow validity bitmap is LSB-first and uses 1 = valid; ClickHouse null map uses 1 = null.
    expandBitmapToBytes(bits, rows, data.data(), /*invert=*/true);

    return null_map;
}

ColumnPtr RecordBatchDecoder::decodeInner(const ArrowField & field, size_t rows, const DataTypePtr & target_hint, const String & path)
{
    const ArrowType & type = field.type;
    DataTypePtr inner_type = fieldToCHType(field, settings, /*make_nullable=*/false, /*allow_null_type=*/true);
    auto column = inner_type->createColumn();

    /// This field's requested ClickHouse type (parent-derived hint, or a dotted-name lookup), used only to
    /// decide whether a `date32` maps to a numeric target and is read raw; and to derive child hints below.
    const DataTypePtr effective_hint = resolveTargetHint(target_hint, path);
    const bool date32_as_number = effective_hint && isNumber(stripHint(effective_hint));

    auto child_path = [&](const String & child_name) -> String
    {
        String seg = child_name;
        if (settings.arrow.case_insensitive_column_matching)
            boost::to_lower(seg);
        return path.empty() ? seg : path + "." + seg;
    };

    switch (type.kind)
    {
        case TypeKind::Int:
        {
            const Slice values = nextBuffer();
            if (type.is_signed)
            {
                switch (type.bit_width)
                {
                    case 8: fillFixed<ColumnInt8>(*column, rows, values, 1); break;
                    case 16: fillFixed<ColumnInt16>(*column, rows, values, 2); break;
                    case 32: fillFixed<ColumnInt32>(*column, rows, values, 4); break;
                    case 64: fillFixed<ColumnInt64>(*column, rows, values, 8); break;
                    default: throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported Arrow int bit width {}", type.bit_width);
                }
            }
            else
            {
                switch (type.bit_width)
                {
                    case 8: fillFixed<ColumnUInt8>(*column, rows, values, 1); break;
                    case 16: fillFixed<ColumnUInt16>(*column, rows, values, 2); break;
                    case 32: fillFixed<ColumnUInt32>(*column, rows, values, 4); break;
                    case 64: fillFixed<ColumnUInt64>(*column, rows, values, 8); break;
                    default: throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported Arrow int bit width {}", type.bit_width);
                }
            }
            break;
        }
        case TypeKind::FloatingPoint:
        {
            const Slice values = nextBuffer();
            if (type.float_precision == flatbuf::Precision_DOUBLE)
                fillFixed<ColumnFloat64>(*column, rows, values, 8);
            else if (type.float_precision == flatbuf::Precision_SINGLE)
                fillFixed<ColumnFloat32>(*column, rows, values, 4);
            else
            {
                /// half-float -> Float32
                checkBufferSize(values, requiredBytes(rows, sizeof(UInt16)), "half_float");
                auto & data = assert_cast<ColumnFloat32 &>(*column).getData();
                data.resize(rows);
                const auto * src = reinterpret_cast<const UInt16 *>(values.ptr);
                for (size_t i = 0; i < rows; ++i)
                    data[i] = convertFloat16ToFloat32(src[i]);
            }
            break;
        }
        case TypeKind::Bool:
        {
            const Slice values = nextBuffer();
            checkBufferSize(values, (rows + 7) / 8, "bool");
            auto & data = assert_cast<ColumnUInt8 &>(*column).getData();
            data.resize(rows);
            const auto * bits = reinterpret_cast<const uint8_t *>(values.ptr);
            expandBitmapToBytes(bits, rows, data.data(), /*invert=*/false);
            break;
        }
        case TypeKind::Decimal:
        {
            const Slice values = nextBuffer();
            const size_t arrow_value_size = static_cast<size_t>(type.decimal_bit_width) / 8;
            /// `fillFixed` copies the low `sizeof(V)` bytes of each value and trusts the buffer to hold at
            /// least `arrow_value_size` bytes per row. An untrusted Arrow file can declare a `bitWidth`
            /// narrower than the ClickHouse decimal selected from `precision` (e.g. `bitWidth = 32` with
            /// `precision = 18` -> `Decimal64`); reading `sizeof(V)` bytes from a smaller stride would read
            /// out of bounds. Reject any decimal whose Arrow storage is narrower than the target value.
            auto fill_decimal = [&]<typename Decimal>(size_t ch_value_size)
            {
                if (arrow_value_size < ch_value_size)
                    throw Exception(
                        ErrorCodes::INCORRECT_DATA,
                        "Arrow decimal bit width {} is too small for the {}-byte ClickHouse decimal",
                        type.decimal_bit_width, ch_value_size);
                fillFixed<ColumnDecimal<Decimal>>(*column, rows, values, arrow_value_size);
            };
            switch (column->getDataType())
            {
                case TypeIndex::Decimal32: fill_decimal.template operator()<Decimal32>(sizeof(Decimal32)); break;
                case TypeIndex::Decimal64: fill_decimal.template operator()<Decimal64>(sizeof(Decimal64)); break;
                case TypeIndex::Decimal128: fill_decimal.template operator()<Decimal128>(sizeof(Decimal128)); break;
                case TypeIndex::Decimal256: fill_decimal.template operator()<Decimal256>(sizeof(Decimal256)); break;
                default: throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected decimal column type");
            }
            break;
        }
        case TypeKind::Date:
        {
            const Slice values = nextBuffer();
            if (type.unit == flatbuf::DateUnit_DAY)
            {
                /// date32: days since the epoch, maps to Date32 (Int32). When the requested header type is
                /// numeric, read the raw day number without the range check (matching the Apache Arrow
                /// library reader's numeric type-hint behavior); `buildChunk` then casts it to the number.
                if (date32_as_number)
                {
                    fillFixed<ColumnInt32>(*column, rows, values, sizeof(Int32));
                    break;
                }
                /// Otherwise enforce the same range/overflow contract as the library reader
                /// (`readColumnWithDate32Data`): a day number outside ClickHouse's allowed Date32 range is
                /// saturated or rejected according to `date_time_overflow_behavior` (its default `Ignore`,
                /// like `Throw`, rejects — preserving the pre-`date_time_overflow_behavior` behavior)
                /// instead of leaving an invalid Date32 in the result.
                checkBufferSize(values, requiredBytes(rows, sizeof(Int32)), "date32");
                auto & data = assert_cast<ColumnInt32 &>(*column).getData();
                data.resize(rows);
                const auto * src = reinterpret_cast<const Int32 *>(values.ptr);
                const bool saturate = settings.date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Saturate;
                for (size_t i = 0; i < rows; ++i)
                {
                    Int32 days = src[i];
                    if (days > DATE_LUT_MAX_EXTEND_DAY_NUM || days < -DAYNUM_OFFSET_EPOCH)
                    {
                        if (saturate)
                            days = days < -DAYNUM_OFFSET_EPOCH ? -DAYNUM_OFFSET_EPOCH : DATE_LUT_MAX_EXTEND_DAY_NUM;
                        else
                            throw Exception(
                                ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                                "Arrow IPC date32 value {} is out of the allowed Date32 range [{}, {}]",
                                days, -DAYNUM_OFFSET_EPOCH, DATE_LUT_MAX_EXTEND_DAY_NUM);
                    }
                    data[i] = days;
                }
            }
            else
            {
                /// date64: milliseconds since the epoch, maps to DateTime (UInt32 seconds).
                checkBufferSize(values, requiredBytes(rows, sizeof(Int64)), "date64");
                auto & data = assert_cast<ColumnUInt32 &>(*column).getData();
                data.resize(rows);
                const auto * src = reinterpret_cast<const Int64 *>(values.ptr);
                for (size_t i = 0; i < rows; ++i)
                    data[i] = static_cast<UInt32>(src[i] / 1000);
            }
            break;
        }
        case TypeKind::Timestamp:
        {
            /// Maps to DateTime64(unit*3); the raw int64 value is exactly the underlying value at that scale.
            const Slice values = nextBuffer();
            fillFixed<ColumnDecimal<DateTime64>>(*column, rows, values, 8);
            break;
        }
        case TypeKind::Time:
        {
            /// Maps to Time64(unit*3); the raw value is exactly the underlying value at that scale, matching
            /// the library reader (`readColumnWithTimeData`). `time32[s|ms]` stores 4-byte values;
            /// `time64[us|ns]` stores 8-byte values.
            const Slice values = nextBuffer();
            if (type.time_bit_width == 32)
            {
                checkBufferSize(values, requiredBytes(rows, sizeof(Int32)), "time32");
                auto & data = assert_cast<ColumnDecimal<Time64> &>(*column).getData();
                data.resize(rows);
                const auto * src = reinterpret_cast<const Int32 *>(values.ptr);
                for (size_t i = 0; i < rows; ++i)
                    data[i] = Time64(src[i]);
                break;
            }
            fillFixed<ColumnDecimal<Time64>>(*column, rows, values, 8);
            break;
        }
        case TypeKind::Duration:
        {
            /// Maps to Interval (stored as Int64); the raw int64 count in the duration's unit.
            const Slice values = nextBuffer();
            fillFixed<ColumnInt64>(*column, rows, values, 8);
            break;
        }
        case TypeKind::Utf8:
        case TypeKind::Binary:
        case TypeKind::LargeUtf8:
        case TypeKind::LargeBinary:
        {
            const bool large = type.kind == TypeKind::LargeUtf8 || type.kind == TypeKind::LargeBinary;
            const Slice offsets_slice = nextBuffer();
            const Slice data_slice = nextBuffer();
            auto & string_column = assert_cast<ColumnString &>(*column);
            /// A zero-row column may omit its offsets buffer entirely; nothing to decode.
            if (rows == 0)
                break;

            /// Validate the offsets buffer before reserving: an inflated (or forged-huge) row count would
            /// otherwise reserve gigabytes (and hit the memory limit) before this check could reject the file.
            const size_t offset_size = large ? sizeof(Int64) : sizeof(Int32);
            checkBufferSize(offsets_slice, requiredBytes(rows + 1, offset_size), "offsets");

            string_column.reserve(rows);
            string_column.getChars().reserve(static_cast<size_t>(data_slice.length) + rows);

            auto read_offset = [&](size_t i) -> Int64
            {
                if (large)
                    return reinterpret_cast<const Int64 *>(offsets_slice.ptr)[i];
                return reinterpret_cast<const Int32 *>(offsets_slice.ptr)[i];
            };

            /// A sliced Arrow string array can begin at a non-negative first offset; the value bytes are
            /// read directly from `data[offset[i], offset[i + 1])`, so any base offset works as long as the
            /// offsets stay monotonic and within the data buffer (matching the Apache Arrow library reader).
            Int64 prev = read_offset(0);
            if (prev < 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC string column has a negative first offset {}", prev);
            for (size_t i = 0; i < rows; ++i)
            {
                const Int64 end = read_offset(i + 1);
                if (end < prev || end > data_slice.length)
                    throw Exception(
                        ErrorCodes::INCORRECT_DATA,
                        "Arrow IPC string column has a corrupted offset (prev {}, end {}, data size {})",
                        prev, end, data_slice.length);
                /// A valid all-empty string array has a zero-length (hence `nullptr`) data buffer; forming
                /// `data_slice.ptr + prev` would be undefined pointer arithmetic on null even though no bytes
                /// are read. Insert the empty value without touching the data pointer.
                if (end == prev)
                    string_column.insertData("", 0);
                else
                    string_column.insertData(data_slice.ptr + prev, static_cast<size_t>(end - prev));
                prev = end;
            }
            break;
        }
        case TypeKind::BinaryView:
        case TypeKind::Utf8View:
        {
            /// Layout: validity (consumed), a 16-byte-per-row views buffer, then `variadic_counts` data
            /// buffers. Each view is {int32 length; if length<=12 inline 12 bytes; else int32 prefix,
            /// int32 buffer_index, int32 offset into that data buffer}.
            const Slice views = nextBuffer();
            checkBufferSize(views, requiredBytes(rows, 16), "binary view");
            const Int64 num_data = variadic_index < variadic_counts.size() ? variadic_counts[variadic_index] : 0;
            ++variadic_index;
            /// `num_data` is untrusted IPC metadata (already checked non-negative in `decodeColumns`). A forged
            /// huge positive count would drive an oversized `reserve` before `nextBuffer` notices the batch has
            /// fewer buffers; cap it at the number of remaining buffers first.
            if (static_cast<size_t>(num_data) > buffer_slices.size() - buffer_index)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Arrow IPC binary view column declares {} data buffers but only {} remain",
                    num_data, buffer_slices.size() - buffer_index);
            VectorWithMemoryTracking<Slice> data_buffers;
            data_buffers.reserve(static_cast<size_t>(num_data));
            for (Int64 i = 0; i < num_data; ++i)
                data_buffers.push_back(nextBuffer());

            auto & string_column = assert_cast<ColumnString &>(*column);
            string_column.reserve(rows);
            for (size_t i = 0; i < rows; ++i)
            {
                const char * v = views.ptr + i * 16;
                Int32 length = 0;
                memcpy(&length, v, sizeof(Int32));
                if (length < 0)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Negative Arrow view length {}", length);
                if (length <= 12)
                {
                    string_column.insertData(v + 4, static_cast<size_t>(length));
                }
                else
                {
                    Int32 data_buffer_index = 0;
                    Int32 offset = 0;
                    memcpy(&data_buffer_index, v + 8, sizeof(Int32));
                    memcpy(&offset, v + 12, sizeof(Int32));
                    if (data_buffer_index < 0 || static_cast<size_t>(data_buffer_index) >= data_buffers.size())
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow view references invalid data buffer {}", data_buffer_index);
                    const Slice & data = data_buffers[data_buffer_index];
                    if (offset < 0 || static_cast<Int64>(offset) + length > data.length)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow view references out-of-range data");
                    string_column.insertData(data.ptr + offset, static_cast<size_t>(length));
                }
            }
            break;
        }
        case TypeKind::FixedSizeBinary:
        {
            const Slice values = nextBuffer();
            const size_t n = static_cast<size_t>(type.byte_width);
            checkBufferSize(values, requiredBytes(rows, n), "fixed_size_binary");
            if (isUUIDField(field))
            {
                /// 16 bytes per value, with the two 64-bit halves byte-reversed (matches the writer).
                auto & data = assert_cast<ColumnVector<UUID> &>(*column).getData();
                data.resize(rows);
                for (size_t i = 0; i < rows; ++i)
                {
                    auto * dst = reinterpret_cast<uint8_t *>(&data[i]);
                    memcpy(dst, values.ptr + i * 16, 16);
                    std::reverse(dst, dst + 8);
                    std::reverse(dst + 8, dst + 16);
                }
                break;
            }
            auto & fixed_column = assert_cast<ColumnFixedString &>(*column);
            auto & chars = fixed_column.getChars();
            chars.resize(rows * n);
            if (rows)
                memcpy(chars.data(), values.ptr, rows * n);
            break;
        }
        case TypeKind::List:
        case TypeKind::LargeList:
            return readOffsetsAndChild(
                field, rows, /*large=*/type.kind == TypeKind::LargeList, arrayElementHint(effective_hint), path);
        case TypeKind::FixedSizeList:
        {
            /// No offsets buffer: each row has exactly `list_size` elements. `list_size` is untrusted IPC
            /// metadata: a negative value would wrap to a huge `size_t`, and a zero would make the expected
            /// child length independent of `rows`, leaving the forged parent row count unbounded. Reject a
            /// non-positive size, and compute the expected child length with checked multiplication so an
            /// overflowing `rows * list_size` cannot wrap to disguise a forged `rows` before allocating.
            if (type.list_size <= 0)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA, "Arrow IPC fixed-size-list has a non-positive list size {}", type.list_size);
            const size_t list_size = static_cast<size_t>(type.list_size);
            const size_t expected_child = requiredBytes(rows, list_size);
            ColumnPtr child = decodeField(type.children.at(0), /*allow_low_cardinality=*/false, arrayElementHint(effective_hint), path);
            if (child->size() != expected_child)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Arrow IPC fixed-size-list child has {} rows, expected {}", child->size(), expected_child);
            auto offsets_col = ColumnUInt64::create(rows);
            auto & offs = offsets_col->getData();
            for (size_t i = 0; i < rows; ++i)
                offs[i] = (i + 1) * list_size;
            return ColumnArray::create(child, std::move(offsets_col));
        }
        case TypeKind::Struct:
        {
            if (type.children.empty())
                return ColumnTuple::create(rows); /// empty Tuple() has no element columns
            Columns elements;
            elements.reserve(type.children.size());
            for (size_t i = 0; i < type.children.size(); ++i)
            {
                const ArrowField & child = type.children[i];
                ColumnPtr element = decodeField(
                    child, /*allow_low_cardinality=*/false,
                    tupleElementHint(effective_hint, child.name, i, settings.arrow.case_insensitive_column_matching),
                    child_path(child.name));
                /// Every struct field carries its own `FieldNode.length`, but they must all equal the
                /// parent struct's row count. A malformed file can shorten one field (or slice a child
                /// out of range), leaving a `ColumnTuple` with elements of unequal size. Reject it here
                /// (the Apache Arrow library reader's `StructArray::field()` silently clamps such fields).
                if (element->size() != rows)
                    throw Exception(
                        ErrorCodes::INCORRECT_DATA,
                        "Arrow IPC struct field '{}' has {} rows, expected {}", child.name, element->size(), rows);
                elements.push_back(std::move(element));
            }
            return ColumnTuple::create(elements);
        }
        case TypeKind::Map:
        {
            /// Map is List<Struct<key, value>>: read the list offsets, then the entries struct.
            const Slice offsets_slice = nextBuffer();
            checkBufferSize(offsets_slice, requiredBytes(rows + 1, sizeof(Int32)), "map offsets");
            const auto * arrow_offsets = reinterpret_cast<const Int32 *>(offsets_slice.ptr);
            const Int64 base = arrow_offsets[0];
            if (base < 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC map has a negative first offset {}", base);
            auto offsets_col = ColumnUInt64::create(rows);
            auto & offs = offsets_col->getData();
            /// Offsets must be monotonic non-decreasing: compare each with the previous one, not only `base`.
            Int64 prev = base;
            for (size_t i = 0; i < rows; ++i)
            {
                const Int64 end = arrow_offsets[i + 1];
                if (end < prev)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC map has non-monotonic offsets");
                offs[i] = static_cast<UInt64>(end - base);
                prev = end;
            }

            /// The entries struct's (key, value) get their hints from a synthetic Tuple(keyType, valueType)
            /// built from the Map hint; the struct recursion then matches them by position.
            ColumnPtr entries = decodeField(
                type.children.at(0), /*allow_low_cardinality=*/false, mapEntriesHint(effective_hint), path);
            const auto & entries_tuple = assert_cast<const ColumnTuple &>(*entries);
            if (entries_tuple.tupleSize() != 2)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC map entries must be a struct of (key, value)");
            /// A sliced Arrow map can begin at a non-zero first offset; only entries[base, prev) are
            /// referenced. Reject offsets past the entries, then slice the key/value columns to that range
            /// so their size matches the base-relative offsets (matching the Apache Arrow library reader).
            if (prev > static_cast<Int64>(entries_tuple.size()))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC map offset {} points past the {} entries", prev, entries_tuple.size());
            const size_t referenced = static_cast<size_t>(prev - base);
            ColumnPtr keys = entries_tuple.getColumnPtr(0);
            ColumnPtr values = entries_tuple.getColumnPtr(1);
            if (!(base == 0 && referenced == entries_tuple.size()))
            {
                keys = keys->cut(static_cast<size_t>(base), referenced);
                values = values->cut(static_cast<size_t>(base), referenced);
            }
            return ColumnMap::create(keys, values, std::move(offsets_col));
        }
        default:
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Native Arrow IPC reader does not support decoding this type yet (field '{}')",
                field.name);
    }

    return column;
}

ColumnPtr RecordBatchDecoder::readOffsetsAndChild(
    const ArrowField & field, size_t rows, bool large, const DataTypePtr & target_hint, const String & path)
{
    const Slice offsets_slice = nextBuffer();
    const size_t offset_size = large ? sizeof(Int64) : sizeof(Int32);
    checkBufferSize(offsets_slice, requiredBytes(rows + 1, offset_size), "list offsets");

    auto read_offset = [&](size_t i) -> Int64
    {
        if (large)
            return reinterpret_cast<const Int64 *>(offsets_slice.ptr)[i];
        return reinterpret_cast<const Int32 *>(offsets_slice.ptr)[i];
    };

    const Int64 base = read_offset(0);
    /// The first offset must be non-negative; the per-row offsets below are stored relative to it.
    if (base < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC list has a negative first offset {}", base);
    auto offsets_col = ColumnUInt64::create(rows);
    auto & offs = offsets_col->getData();
    /// Offsets must be monotonic non-decreasing: compare each with the previous one, not only `base`.
    Int64 prev = base;
    for (size_t i = 0; i < rows; ++i)
    {
        const Int64 end = read_offset(i + 1);
        if (end < prev)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC list has non-monotonic offsets");
        offs[i] = static_cast<UInt64>(end - base);
        prev = end;
    }

    /// `target_hint`/`path` are this list's element hint and dotted name, threaded for the recursive
    /// `date32` numeric type hint (a list does not extend the dotted path).
    ColumnPtr child = decodeField(field.type.children.at(0), /*allow_low_cardinality=*/false, target_hint, path);
    /// A sliced Arrow list can begin at a non-zero first offset; only child[base, prev) is referenced.
    /// Reject offsets that point past the child, then slice it to the referenced range so its size matches
    /// the base-relative offsets — mirroring the Apache Arrow library reader's Flatten/slice of a slice.
    if (prev > static_cast<Int64>(child->size()))
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Arrow IPC list offset {} points past the {}-element child", prev, child->size());
    const size_t referenced = static_cast<size_t>(prev - base);
    ColumnPtr child_slice
        = (base == 0 && referenced == child->size()) ? child : child->cut(static_cast<size_t>(base), referenced);
    return ColumnArray::create(child_slice, std::move(offsets_col));
}

ColumnPtr RecordBatchDecoder::decodeDictionary(
    const ArrowField & field, size_t rows, const Slice & validity, Int64 null_count, bool allow_low_cardinality)
{
    const Slice indices_slice = nextBuffer();
    ColumnPtr values = registry.get(field.dictionary->id);
    const size_t dict_size = values->size();

    const int bits = field.dictionary->index_bit_width;
    const bool index_is_signed = field.dictionary->index_is_signed;
    /// Validate the index width before it is used to size buffers or allocate the output indexes. An
    /// invalid width such as 7 would make `index_size` zero (so `checkBufferSize` requires no bytes) and
    /// let a forged huge `FieldNode::length` drive an oversized `ColumnUInt64::create(rows)` before the
    /// `switch` over `bits` below could report the unsupported width.
    if (bits != 8 && bits != 16 && bits != 32 && bits != 64)
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "Arrow IPC dictionary index bit width {} is not supported (must be 8, 16, 32, or 64)", bits);
    const size_t index_size = static_cast<size_t>(bits) / 8;
    checkBufferSize(indices_slice, requiredBytes(rows, index_size), "dictionary indices");

    /// A row can be null either because the indices array marks it null (pyarrow style) or because it
    /// points at a null entry inside the dictionary values (the ClickHouse writer style); handle both.
    const UInt8 * index_nulls = nullptr;
    ColumnPtr index_null_map;
    if (field.nullable)
    {
        index_null_map = buildNullMap(validity, rows, null_count);
        index_nulls = assert_cast<const ColumnUInt8 &>(*index_null_map).getData().data();
    }

    /// Keys for the LowCardinality dictionary: the decoded Arrow dictionary values plus, for nullable
    /// fields, a trailing NULL that null rows point at (Arrow keeps nulls only in the index validity).
    DataTypePtr value_type = fieldToCHType(field, settings, field.nullable);
    MutableColumnPtr keys = IColumn::mutate(values->cloneResized(dict_size));
    UInt64 null_key_index = dict_size;
    if (field.nullable)
        keys->insertDefault(); /// a NULL for a Nullable value column

    /// Map each row to a key index (UInt64), pointing null rows at the trailing NULL key.
    auto indexes = ColumnUInt64::create(rows);
    auto & idx = indexes->getData();
    for (size_t i = 0; i < rows; ++i)
    {
        if (index_nulls && index_nulls[i])
        {
            idx[i] = null_key_index;
            continue;
        }
        /// The index buffer may be signed (`DictionaryEncoding::indexType`); a negative index is invalid
        /// and must be rejected before widening, otherwise it would wrap to a large unsigned key.
        Int64 v = 0;
        switch (bits)
        {
            case 8: v = index_is_signed ? Int64(reinterpret_cast<const int8_t *>(indices_slice.ptr)[i])
                                        : Int64(reinterpret_cast<const uint8_t *>(indices_slice.ptr)[i]); break;
            case 16: v = index_is_signed ? Int64(reinterpret_cast<const Int16 *>(indices_slice.ptr)[i])
                                         : Int64(reinterpret_cast<const UInt16 *>(indices_slice.ptr)[i]); break;
            case 32: v = index_is_signed ? Int64(reinterpret_cast<const Int32 *>(indices_slice.ptr)[i])
                                         : Int64(reinterpret_cast<const UInt32 *>(indices_slice.ptr)[i]); break;
            case 64: v = reinterpret_cast<const Int64 *>(indices_slice.ptr)[i]; break;
            default: throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported Arrow dictionary index width {}", bits);
        }
        if (v < 0 || static_cast<UInt64>(v) >= dict_size)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC dictionary index {} out of range (size {})", v, dict_size);
        idx[i] = static_cast<UInt64>(v);
    }

    /// Build the LowCardinality column directly from (keys, indexes): the dictionary is deduplicated
    /// once (a handful of values) and the per-row indexes are remapped with a cheap gather — no
    /// materialization of the full column and no per-row hashing. `decodeColumns` reports the matching
    /// LowCardinality type. A dictionary nested inside Array/Map/Tuple/Union is not wrapped as
    /// LowCardinality by `fieldToCHType` (only top-level fields are), so materialize those to the plain
    /// value column to keep the decoded column structure consistent with the declared type. For the rare
    /// value type that cannot live inside LowCardinality, gather the full column too (the trailing NULL
    /// key makes null rows resolve to NULL).
    if (allow_low_cardinality && value_type->canBeInsideLowCardinality())
    {
        auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(value_type);
        auto column = low_cardinality_type->createColumn();
        assert_cast<ColumnLowCardinality &>(*column).insertRangeFromDictionaryEncodedColumn(*keys, *indexes);
        return column;
    }
    return keys->index(*indexes, 0);
}

ColumnPtr RecordBatchDecoder::decodeUnion(const ArrowField & field, size_t rows)
{
    const ArrowType & type = field.type;
    const bool dense = type.union_mode == flatbuf::UnionMode_Dense;

    /// A union has no validity buffer: a types buffer (int8), and for dense unions an offsets buffer (int32).
    const Slice type_ids_slice = nextBuffer();
    checkBufferSize(type_ids_slice, rows, "union type ids");
    const auto * type_ids = reinterpret_cast<const int8_t *>(type_ids_slice.ptr);

    const Int32 * value_offsets = nullptr;
    if (dense)
    {
        const Slice offsets_slice = nextBuffer();
        checkBufferSize(offsets_slice, requiredBytes(rows, sizeof(Int32)), "union offsets");
        value_offsets = reinterpret_cast<const Int32 *>(offsets_slice.ptr);
    }

    /// Decode children. Arrow `null`-typed children are the ClickHouse NULL placeholder: they carry a
    /// FieldNode but no buffers and contribute no Variant element. The rest become Variant elements.
    Columns variant_columns;
    DataTypes variant_types;
    /// `ColumnVariant` elements cannot be Nullable, so the element-level null map is dropped from the
    /// stored column. Keep a pointer to it (per local element, nullptr if the child is not nullable) so
    /// rows that reference a null child value can be translated to the Variant NULL discriminator below,
    /// instead of silently becoming the nested column's default value.
    VectorWithMemoryTracking<const NullMap *> child_null_maps;
    /// Each `child_null_maps` entry points into the null-map column of a `ColumnNullable`. The owning
    /// `ColumnNullable` is dropped below (only its nested column is kept as the Variant element), so keep
    /// the null-map columns alive here for the lifetime of the decode; otherwise the pointers dangle.
    VectorWithMemoryTracking<ColumnPtr> child_null_map_holders;
    /// Maps an Arrow union type id to a local Variant element index, or -1 for the NULL placeholder.
    UnorderedMapWithMemoryTracking<int, int> type_id_to_local;
    for (size_t child_idx = 0; child_idx < type.children.size(); ++child_idx)
    {
        const ArrowField & child = type.children[child_idx];
        const int tid = child_idx < type.union_type_ids.size()
            ? type.union_type_ids[child_idx] : static_cast<int>(child_idx);

        if (child.type.kind == TypeKind::Null)
        {
            nextNode(); /// consume the placeholder node; the null type has no buffers
            type_id_to_local[tid] = -1;
            continue;
        }

        ColumnPtr child_column = decodeField(child);
        DataTypePtr child_type = fieldToCHType(child, settings, /*make_nullable=*/false);
        /// Variant elements cannot be Nullable; remember the null map, then drop the nullability.
        const NullMap * child_null_map = nullptr;
        if (child_column->isNullable())
        {
            const auto & nullable = assert_cast<const ColumnNullable &>(*child_column);
            /// Keep the null-map column alive: the line below reassigns `child_column` to the nested
            /// column, which would otherwise free the only owner of this `ColumnNullable` (and its null
            /// map), leaving `child_null_map` dangling for the row checks further down.
            ColumnPtr null_map_holder = nullable.getNullMapColumnPtr();
            child_null_map = &assert_cast<const ColumnUInt8 &>(*null_map_holder).getData();
            child_null_map_holders.push_back(std::move(null_map_holder));
            child_column = nullable.getNestedColumnPtr();
        }
        type_id_to_local[tid] = static_cast<int>(variant_columns.size());
        variant_columns.push_back(std::move(child_column));
        variant_types.push_back(removeNullable(child_type));
        child_null_maps.push_back(child_null_map);
    }

    /// The Variant's global discriminator order is defined by sorting element type names; build the
    /// local (child) -> global mapping accordingly.
    auto variant_data_type = std::make_shared<DataTypeVariant>(variant_types);
    /// `DataTypeVariant` deduplicates its element types by name, but the decoder keeps one local column
    /// per Arrow union child. If two children map to the same ClickHouse type, the locals would both point
    /// at a single global discriminator, producing a `ColumnVariant` whose physical layout does not match
    /// its declared type. Reject such a union rather than build an inconsistent column.
    if (variant_data_type->getVariants().size() != variant_types.size())
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Arrow IPC union has multiple children mapping to the same ClickHouse type, which Variant cannot represent");
    UnorderedMapWithMemoryTracking<String, ColumnVariant::Discriminator> name_to_global;
    for (size_t g = 0; g < variant_data_type->getVariants().size(); ++g)
        name_to_global[variant_data_type->getVariants()[g]->getName()] = static_cast<ColumnVariant::Discriminator>(g);
    VectorWithMemoryTracking<ColumnVariant::Discriminator> local_to_global(variant_columns.size());
    for (size_t l = 0; l < variant_types.size(); ++l)
        local_to_global[l] = name_to_global[variant_types[l]->getName()];

    auto local_discriminators = ColumnVariant::ColumnDiscriminators::create(rows);
    auto offsets = ColumnVariant::ColumnOffsets::create(rows);
    auto & discr_data = local_discriminators->getData();
    auto & off_data = offsets->getData();

    if (dense)
    {
        for (size_t row = 0; row < rows; ++row)
        {
            auto local_it = type_id_to_local.find(type_ids[row]);
            if (local_it == type_id_to_local.end())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow union row references unknown type id {}", type_ids[row]);
            const int local = local_it->second;
            if (local < 0)
            {
                discr_data[row] = ColumnVariant::NULL_DISCRIMINATOR;
                off_data[row] = 0;
                continue;
            }
            const Int32 off = value_offsets[row];
            if (off < 0 || static_cast<size_t>(off) >= variant_columns[local]->size())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow dense union offset {} out of range", off);
            /// A referenced null value in a nullable child becomes a Variant NULL, not a default value.
            if (child_null_maps[local] && (*child_null_maps[local])[off])
            {
                discr_data[row] = ColumnVariant::NULL_DISCRIMINATOR;
                off_data[row] = 0;
                continue;
            }
            discr_data[row] = static_cast<ColumnVariant::Discriminator>(local);
            off_data[row] = static_cast<ColumnVariant::Offset>(off);
        }
        return ColumnVariant::create(
            std::move(local_discriminators), std::move(offsets), Columns(variant_columns), local_to_global);
    }

    /// Sparse union: every child holds `rows` values; compact each Variant element to only its own rows.
    /// Validate that before the per-row access below — a malformed file can give a child a shorter
    /// `FieldNode::length` (the decoder accepts each child length independently), and `insertFrom` / the
    /// child null-map lookup would then read past the child column. Dense unions are already bounded by the
    /// per-row offset check above, so this is needed only for the sparse layout.
    for (size_t l = 0; l < variant_columns.size(); ++l)
    {
        if (variant_columns[l]->size() != rows)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Arrow IPC sparse union child {} has {} rows, expected {}", l, variant_columns[l]->size(), rows);
    }

    MutableColumns compact;
    compact.reserve(variant_columns.size());
    for (const auto & col : variant_columns)
        compact.push_back(col->cloneEmpty());
    for (size_t row = 0; row < rows; ++row)
    {
        auto local_it = type_id_to_local.find(type_ids[row]);
        if (local_it == type_id_to_local.end())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow union row references unknown type id {}", type_ids[row]);
        const int local = local_it->second;
        if (local < 0)
        {
            discr_data[row] = ColumnVariant::NULL_DISCRIMINATOR;
            off_data[row] = 0;
            continue;
        }
        /// A referenced null value in a nullable child becomes a Variant NULL, not a default value.
        if (child_null_maps[local] && (*child_null_maps[local])[row])
        {
            discr_data[row] = ColumnVariant::NULL_DISCRIMINATOR;
            off_data[row] = 0;
            continue;
        }
        off_data[row] = static_cast<ColumnVariant::Offset>(compact[local]->size());
        compact[local]->insertFrom(*variant_columns[local], row);
        discr_data[row] = static_cast<ColumnVariant::Discriminator>(local);
    }
    return ColumnVariant::create(
        std::move(local_discriminators), std::move(offsets), std::move(compact), local_to_global);
}

ColumnPtr RecordBatchDecoder::decodeField(
    const ArrowField & field, bool allow_low_cardinality, const DataTypePtr & target_hint, const String & path)
{
    const flatbuf::FieldNode & node = nextNode();
    /// `FieldNode::length` is signed IPC metadata; reject a negative (corrupted) length before casting,
    /// otherwise it would become a huge row count and drive oversized allocations.
    const Int64 node_length = node.length();
    if (node_length < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC field node has a negative length {}", node_length);
    const size_t rows = static_cast<size_t>(node_length);

    /// A dictionary-encoded field is physically an index array here (validity slot + indices); its value-type
    /// layout (union, null, ...) lives only in the DictionaryBatch. Skip the value-type special cases below
    /// for such a field so its index buffer is not misread as, e.g., a union type-id buffer; the
    /// `field.dictionary` branch further down consumes the validity slot and the indices.
    const bool is_dictionary = field.dictionary.has_value();

    /// Unions have no validity buffer; handle them before consuming one.
    if (!is_dictionary && field.type.kind == TypeKind::Union)
    {
        /// An Arrow union carries no top-level validity bitmap, so a node that nonetheless reports nulls is
        /// malformed: there is no bitmap to say which rows are null, and `decodeUnion` would decode the
        /// type-id/value buffers as if every row were valid. Reject a non-zero (or unknown, i.e. negative)
        /// null count. Real `Variant` nulls travel through the explicit `null` child /
        /// `ColumnVariant::NULL_DISCRIMINATOR`, not a FieldNode null count.
        if (node.null_count() != 0)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Arrow IPC Union field '{}' has no validity bitmap but its FieldNode reports {} nulls",
                field.name, node.null_count());
        return decodeUnion(field, rows);
    }

    /// An Arrow `null`-typed field carries no buffers at all (not even validity); it is an all-null
    /// column. Decode it as an all-null `Nullable(Nothing)` (matching `fieldToCHType` and how the library
    /// reader wraps its `Nothing` column); `buildChunk` then casts it to the requested target as NULLs
    /// (or column DEFAULTs with `null_as_default`).
    if (!is_dictionary && field.type.kind == TypeKind::Null)
        return ColumnNullable::create(ColumnNothing::create(rows), ColumnUInt8::create(rows, UInt8{1}));

    /// Every nullable-capable node carries a validity buffer slot first, then its value buffers.
    const Slice validity = nextBuffer();

    /// A non-nullable field must not declare nulls. We build no null map for non-nullable fields (and
    /// Array/Tuple/Map drop their outer validity), so a non-zero (or unknown, i.e. negative) FieldNode
    /// null count would otherwise let those null rows be decoded silently as whatever is in the value
    /// buffer. Reject it before reading values; this also guards the dictionary branch below.
    if (!field.nullable && node.null_count() != 0)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Arrow IPC field '{}' is declared non-nullable but its FieldNode reports {} nulls",
            field.name, node.null_count());

    /// Validate the validity buffer against the declared row count whenever the field declares nulls,
    /// before decoding the value buffers — even when the resulting column type drops the null map
    /// (Array/Tuple/Map cannot be `Nullable` in ClickHouse, so their outer validity is not built). A
    /// non-zero (or unknown, i.e. negative) null count needs a bitmap to say which rows are null: an
    /// absent (zero-length) validity buffer would otherwise be treated as all-valid and silently turn the
    /// declared nulls into real values. A present bitmap must also be large enough for the row count (a
    /// too-small or forged-huge length would read past the buffer or drive an oversized allocation).
    if (node.null_count() != 0)
    {
        if (validity.length == 0)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Arrow IPC field '{}' declares {} nulls but carries no validity bitmap",
                field.name, node.null_count());
        checkBufferSize(validity, (rows + 7) / 8, "validity");
    }

    /// Dictionary-encoded fields carry indices here; the values come from a separate DictionaryBatch.
    if (field.dictionary)
        return decodeDictionary(field, rows, validity, node.null_count(), allow_low_cardinality);

    ColumnPtr inner = decodeInner(field, rows, target_hint, path);

    /// Only wrap in Nullable when the type allows it; Array/Map cannot be inside Nullable in ClickHouse, so
    /// (matching the Apache Arrow library reader) their outer validity is dropped. A Struct (Tuple) is only
    /// wrapped when it is allowed: either `allow_experimental_nullable_tuple_type` is on, or the requested
    /// target type at this field is already nullable (e.g. reading into an existing `Nullable(Tuple)` column).
    /// This mirrors the library reader's `allow_nullable_struct`. Otherwise the struct is read as a plain
    /// Tuple, dropping the struct-level null map. `decodeColumns` reconciles the reported type to this column.
    const DataTypePtr effective_hint = resolveTargetHint(target_hint, path);
    const bool nullable_tuple_allowed = settings.schema_inference_allow_nullable_tuple_type
        || (effective_hint && (effective_hint->isNullable() || effective_hint->isLowCardinalityNullable()));
    const bool struct_not_allowed_nullable = field.type.kind == TypeKind::Struct && !nullable_tuple_allowed;
    if (field.nullable && inner->canBeInsideNullable() && !struct_not_allowed_nullable)
    {
        ColumnPtr null_map = buildNullMap(validity, rows, node.null_count());
        return ColumnNullable::create(inner, null_map);
    }
    return inner;
}

void RecordBatchDecoder::skipField(const ArrowField & field)
{
    /// Consume this field's FieldNode, mirroring decodeField.
    nextNode();

    /// A dictionary-encoded field is physically an index array here (validity slot + indices); its
    /// value-type layout lives only in the DictionaryBatch. Handle it before the value-type special cases
    /// below, which would otherwise consume the wrong buffers and desync the cursor.
    if (field.dictionary)
    {
        nextBuffer(); /// validity
        nextBuffer(); /// indices
        return;
    }

    /// Unions carry no validity buffer (decodeField dispatches to decodeUnion before consuming one):
    /// a type-ids buffer, an offsets buffer for dense unions, then one subtree per child — a null-typed
    /// child being just a placeholder node, exactly as decodeUnion consumes them.
    if (field.type.kind == TypeKind::Union)
    {
        nextBuffer();
        if (field.type.union_mode == flatbuf::UnionMode_Dense)
            nextBuffer();
        for (const ArrowField & child : field.type.children)
        {
            if (child.type.kind == TypeKind::Null)
                nextNode();
            else
                skipField(child);
        }
        return;
    }

    /// A null-typed field has no buffers at all (not even validity).
    if (field.type.kind == TypeKind::Null)
        return;

    /// RunEndEncoded has no buffers of its own — not even validity (its nulls live in the `values` child).
    /// The reader cannot decode it, but its layout is known well enough to skip an unrequested column: just
    /// its two children (run_ends, values). Handle it before consuming the validity buffer below.
    if (field.type.kind == TypeKind::Unsupported && field.type.skip_layout == ArrowType::SkipLayout::RunEndEncoded)
    {
        for (const ArrowField & child : field.type.children)
            skipField(child);
        return;
    }

    /// Validity buffer, present for every other field.
    nextBuffer();

    switch (field.type.kind)
    {
        /// Fixed-width / primitive layouts: a single data buffer after the validity buffer. `Interval` is
        /// included for skipping although decodeInner does not decode it, so an unrequested Arrow interval
        /// column does not block reading the other columns.
        case TypeKind::Int:
        case TypeKind::FloatingPoint:
        case TypeKind::Bool:
        case TypeKind::Decimal:
        case TypeKind::Date:
        case TypeKind::Time:
        case TypeKind::Timestamp:
        case TypeKind::Duration:
        case TypeKind::Interval:
        case TypeKind::FixedSizeBinary:
            nextBuffer();
            break;
        /// Variable-length binary/utf8: an offsets buffer and a data buffer.
        case TypeKind::Utf8:
        case TypeKind::LargeUtf8:
        case TypeKind::Binary:
        case TypeKind::LargeBinary:
            nextBuffer();
            nextBuffer();
            break;
        /// View layouts: a views buffer plus a metadata-declared number of variadic data buffers.
        case TypeKind::BinaryView:
        case TypeKind::Utf8View:
        {
            nextBuffer();
            const Int64 num_data = variadic_index < variadic_counts.size() ? variadic_counts[variadic_index] : 0;
            ++variadic_index;
            for (Int64 i = 0; i < num_data; ++i)
                nextBuffer();
            break;
        }
        /// List/Map: an offsets buffer, then the single child subtree.
        case TypeKind::List:
        case TypeKind::LargeList:
        case TypeKind::Map:
            nextBuffer();
            skipField(field.type.children.at(0));
            break;
        /// FixedSizeList: no buffer of its own beyond validity, only the child subtree.
        case TypeKind::FixedSizeList:
            skipField(field.type.children.at(0));
            break;
        /// Struct: no buffer of its own beyond validity, only the child subtrees.
        case TypeKind::Struct:
            for (const ArrowField & child : field.type.children)
                skipField(child);
            break;
        case TypeKind::Null:
        case TypeKind::Union:
            break; /// handled above
        case TypeKind::Unsupported:
            /// ListView/LargeListView: an offsets buffer and a sizes buffer (after the validity buffer
            /// consumed above), then the single child. The reader cannot decode these, but skipping an
            /// unrequested one keeps the other columns readable. (RunEndEncoded is handled before the
            /// validity buffer above, so it never reaches here.)
            if (field.type.skip_layout == ArrowType::SkipLayout::ListView)
            {
                nextBuffer(); /// offsets
                nextBuffer(); /// sizes
                skipField(field.type.children.at(0));
                break;
            }
            /// The buffer layout of any other unsupported Arrow type is unknown, so its buffers cannot be
            /// skipped to reach later columns. A `SELECT` of other columns from a file with such an
            /// (unrequested) column therefore still fails — but with a clear error rather than a cursor
            /// desync.
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Native Arrow IPC reader cannot skip the unsupported Arrow type {} of field '{}'",
                field.type.unsupported_type_name, field.name);
    }
}

RecordBatchDecoder::DecodedColumns RecordBatchDecoder::decodeColumns(
    const flatbuf::RecordBatch & batch, const PODArray<char> & body, const ArrowFields & fields,
    const UnorderedSetWithMemoryTracking<String> * keep_top_level_fields,
    const UnorderedMapWithMemoryTracking<String, DataTypePtr> * target_types_,
    const VectorWithMemoryTracking<char> * reachable_buffers)
{
    target_types = target_types_;
    current_batch = &batch;
    node_index = 0;
    buffer_index = 0;
    variadic_index = 0;
    variadic_counts.clear();
    if (const auto * counts = batch.variadicBufferCounts())
    {
        for (Int64 c : *counts)
        {
            /// Untrusted IPC metadata: a negative count would become a huge `size_t` when reserving the
            /// data-buffer vector for a `BinaryView`/`Utf8View` column. Reject it.
            if (c < 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC variadic buffer count is negative ({})", c);
            variadic_counts.push_back(c);
        }
    }

    /// Every top-level column must decode to the batch's row count; otherwise the returned `Chunk` would
    /// mix columns of different sizes (an internal inconsistency) instead of being rejected as bad data.
    /// Validate this before `prepareBuffers`, so a negative length on the dictionary-batch path is rejected
    /// before any (possibly large or compressed) buffer is allocated or decompressed from the batch metadata.
    if (batch.length() < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC record batch has a negative length {}", batch.length());
    const size_t batch_rows = static_cast<size_t>(batch.length());

    prepareBuffers(batch, body, reachable_buffers);

    const bool case_insensitive = settings.arrow.case_insensitive_column_matching;

    DecodedColumns result;
    result.reserve(fields.size());
    for (const ArrowField & field : fields)
    {
        String normalized_name = field.name;
        if (case_insensitive)
            boost::to_lower(normalized_name);

        if (keep_top_level_fields && !keep_top_level_fields->contains(normalized_name))
        {
            /// Unrequested column: advance the node/buffer cursors past it without decoding, so a
            /// SELECT of a subset of columns neither pays for nor fails on columns it did not request.
            skipField(field);
            continue;
        }

        DecodedColumn decoded;
        decoded.name = field.name;
        decoded.type = fieldToCHType(field, settings, field.nullable, /*allow_null_type=*/true);
        /// A top-level dictionary-encoded field decodes into a LowCardinality column of its value type.
        if (field.dictionary && decoded.type->canBeInsideLowCardinality())
            decoded.type = std::make_shared<DataTypeLowCardinality>(decoded.type);
        /// `normalized_name` seeds the recursive `date32` numeric type hint (looked up in `target_types`);
        /// nested fields derive their hints from it as the decoder recurses. See decodeField/decodeInner.
        decoded.column = decodeField(field, /*allow_low_cardinality=*/true, /*target_hint=*/nullptr, normalized_name);
        /// `decodeField` may wrap a struct in Nullable based on the requested type hint even when
        /// `fieldToCHType` did not (setting off); reconcile the reported type with the decoded column so the
        /// column/type pair stays consistent for the subsequent cast.
        decoded.type = matchColumnNullability(decoded.type, decoded.column);
        if (decoded.column->size() != batch_rows)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Arrow IPC top-level column '{}' has {} rows, expected the batch length {}",
                field.name, decoded.column->size(), batch_rows);
        result.push_back(std::move(decoded));
    }

    /// Verify the layout math is exact: every FieldNode, buffer and variadic-buffer count the batch declares
    /// must have been consumed by the decoded-or-skipped fields. This must run whether or not columns were
    /// pruned: when pruning, a mismatch means `skipField` mis-counted a layout; when every column is decoded,
    /// a surplus (e.g. an extra in-bounds buffer interposed before a requested column) would otherwise make
    /// the decoder read a column from the injected buffer and silently ignore the real trailing buffer. Fail
    /// loudly here rather than risk reading a column from the wrong buffer. (Dictionary value batches get the
    /// same exactness from `validateBatchLayout`.)
    const size_t total_nodes = current_batch->nodes() ? current_batch->nodes()->size() : 0;
    if (node_index != total_nodes || buffer_index != buffer_slices.size() || variadic_index != variadic_counts.size())
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Arrow IPC record batch consumed {}/{} field nodes, {}/{} buffers and {}/{} variadic counts; "
            "the record batch layout does not match the schema",
            node_index, total_nodes, buffer_index, buffer_slices.size(), variadic_index, variadic_counts.size());

    current_batch = nullptr;
    target_types = nullptr;
    buffer_slices.clear();
    return result;
}

void RecordBatchDecoder::prepareBuffers(const flatbuf::RecordBatch & batch, const PODArray<char> & body, const VectorWithMemoryTracking<char> * reachable)
{
    buffer_slices.clear();
    decompressed_body.clear();

    const auto * buffers = batch.buffers();
    const size_t num_buffers = buffers ? buffers->size() : 0;
    const Int64 body_size = static_cast<Int64>(body.size());

    auto validate = [&](Int64 offset, Int64 length)
    {
        /// Empty buffers may use a placeholder offset (e.g. -1); only non-empty buffers must be in range.
        if (length == 0)
            return;
        if (offset < 0 || length < 0 || offset > body_size || length > body_size - offset)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Arrow IPC buffer (offset {}, length {}) is out of the message body of size {}", offset, length, body_size);
    };

    if (batch.compression() == nullptr)
    {
        buffer_slices.reserve(num_buffers);
        for (size_t i = 0; i < num_buffers; ++i)
        {
            /// Unreachable buffer (subset read): it was not read into `body`. Emit a placeholder slice
            /// without validating it or pointing at its absolute offset; `skipField` consumes it unread.
            if (reachable && !(*reachable)[i])
            {
                buffer_slices.push_back(Slice{nullptr, 0});
                continue;
            }
            const auto * buffer = buffers->Get(static_cast<flatbuffers::uoffset_t>(i));
            validate(buffer->offset(), buffer->length());
            /// Typed decoders read int32/int64 values straight from `body.data() + offset` (offsets, list and
            /// dictionary indices, union offsets). Arrow IPC pads every buffer to an 8-byte boundary and the
            /// body is allocated aligned, so a non-empty buffer at an unaligned offset is malformed and an
            /// unaligned typed load would be undefined behavior; reject it as corrupt data. Compressed buffers
            /// are decompressed into an aligned scratch buffer below, so this only applies to the direct path.
            if (buffer->length() > 0 && (buffer->offset() % 8) != 0)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA, "Arrow IPC buffer offset {} is not 8-byte aligned", buffer->offset());
            const char * ptr = buffer->length() > 0 ? body.data() + buffer->offset() : nullptr;
            buffer_slices.push_back(Slice{ptr, buffer->length()});
        }
        return;
    }

    /// Compressed body: each non-empty buffer is an 8-byte little-endian uncompressed length followed
    /// by the compressed bytes (or, when the length is -1, the bytes stored uncompressed).
    /// Validate the compression type explicitly: an unknown value must be rejected rather than silently
    /// treated as LZ4 (which a malformed batch could otherwise pass off if its payload happens to decode
    /// as valid LZ4). An if-chain (not a `switch`) so an out-of-range value — which the FlatBuffers enum
    /// can still carry — is handled without tripping `-Wcovered-switch-default`.
    /// The Arrow body compression layout decoded below (a per-buffer 8-byte uncompressed-length prefix) is
    /// only defined for `BodyCompressionMethod_BUFFER`. Reject any other (malformed or future) method
    /// instead of decoding the buffer prefixes/offsets under the wrong layout.
    if (batch.compression()->method() != flatbuf::BodyCompressionMethod_BUFFER)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Unsupported Arrow IPC body compression method {}", static_cast<int>(batch.compression()->method()));

    const auto compression_type = batch.compression()->codec();
    CompressionCodec codec = CompressionCodec::Lz4Frame;
    if (compression_type == flatbuf::CompressionType_LZ4_FRAME)
        codec = CompressionCodec::Lz4Frame;
    else if (compression_type == flatbuf::CompressionType_ZSTD)
        codec = CompressionCodec::Zstd;
    else
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "Unsupported Arrow IPC compression type {}", static_cast<int>(compression_type));

    /// First pass: lay out each buffer's decompressed slot (8-byte aligned) without touching the data,
    /// so the destination buffer can be allocated once and the buffers decompressed in parallel.
    struct Placement { size_t offset; size_t length; const char * src; size_t src_size; bool raw; };
    VectorWithMemoryTracking<Placement> placements(num_buffers);
    size_t pos = 0;
    for (size_t i = 0; i < num_buffers; ++i)
    {
        /// Unreachable buffer (subset read): not present in `body`. Reserve an empty placement and skip
        /// reading its length prefix, validating, or decompressing it.
        if (reachable && !(*reachable)[i])
        {
            placements[i] = {pos, 0, nullptr, 0, true};
            continue;
        }
        const auto * buffer = buffers->Get(static_cast<flatbuffers::uoffset_t>(i));
        validate(buffer->offset(), buffer->length());
        const Int64 length = buffer->length();

        /// `pos` accumulates from untrusted `uncompressed_length` metadata; both the 8-byte alignment and
        /// the running total must not wrap, otherwise `decompressed_body` would be under-allocated while a
        /// later placement's offset+length writes past it. Use checked arithmetic and fail closed.
        if (pos > std::numeric_limits<size_t>::max() - 7)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC decompressed body size overflows");
        pos = (pos + 7) & ~size_t(7);
        if (length == 0)
        {
            placements[i] = {pos, 0, nullptr, 0, true};
            continue;
        }
        /// Form the pointer only for a non-empty buffer; an empty buffer may carry a placeholder offset (e.g. -1).
        const char * src = body.data() + buffer->offset();
        if (length < 8)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Compressed Arrow IPC buffer is too small for its length prefix");

        Int64 uncompressed_length = 0;
        memcpy(&uncompressed_length, src, sizeof(uncompressed_length));
        uncompressed_length = DB::fromLittleEndian(uncompressed_length);

        /// Arrow uses exactly -1 as the "stored uncompressed" sentinel; any other negative value is
        /// malformed and must not be accepted as a raw (uncompressed) buffer.
        if (uncompressed_length < -1)
            throw Exception(
                ErrorCodes::INCORRECT_DATA, "Arrow IPC buffer has an invalid uncompressed length prefix {}", uncompressed_length);

        const size_t out_len = uncompressed_length < 0 ? static_cast<size_t>(length - 8) : static_cast<size_t>(uncompressed_length);
        /// A compressed (non-raw) buffer with no payload (length == 8, i.e. only the length prefix) cannot
        /// produce output. Reject a positive declared uncompressed length here, otherwise its `out_len`
        /// bytes would be allocated in `decompressed_body` but never written by any decompression job, and
        /// `buffer_slices` would later expose those uninitialized bytes as decoded Arrow data.
        if (uncompressed_length >= 0 && length == 8 && out_len > 0)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Arrow IPC compressed buffer declares {} uncompressed bytes but carries no payload", out_len);
        if (out_len > std::numeric_limits<size_t>::max() - pos)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC decompressed body size overflows");
        placements[i] = {pos, out_len, src + 8, static_cast<size_t>(length - 8), uncompressed_length < 0};
        pos += out_len;
    }

    decompressed_body.resize(pos);

    VectorWithMemoryTracking<DecompressJob> jobs;
    jobs.reserve(num_buffers);
    for (const auto & p : placements)
    {
        char * dst = decompressed_body.data() + p.offset;
        if (p.raw)
        {
            /// Raw (uncompressed, `-1` prefix) buffer: copy the payload verbatim; nothing to validate.
            if (p.length > 0)
                memcpy(dst, p.src, p.length);
            continue;
        }
        /// Compressed buffer: run the codec whenever there is a payload, even when it decodes to zero
        /// bytes, so a non-empty frame is still validated. A genuinely empty buffer has nothing to decode.
        if (p.src_size > 0)
            jobs.push_back(DecompressJob{p.src, p.src_size, dst, p.length});
    }
    decompressBuffersParallel(codec, jobs);

    buffer_slices.reserve(num_buffers);
    for (const auto & p : placements)
        buffer_slices.push_back(Slice{decompressed_body.data() + p.offset, static_cast<Int64>(p.length)});
}

RecordBatchDecoder::DecodedColumns
RecordBatchDecoder::decodeBatch(
    const flatbuf::RecordBatch & batch, const PODArray<char> & body,
    const UnorderedSetWithMemoryTracking<String> * keep_top_level_fields,
    const UnorderedMapWithMemoryTracking<String, DataTypePtr> * target_types_,
    const VectorWithMemoryTracking<char> * reachable_buffers)
{
    return decodeColumns(batch, body, schema.fields, keep_top_level_fields, target_types_, reachable_buffers);
}

VectorWithMemoryTracking<char> RecordBatchDecoder::reachableTopLevelBuffers(
    const flatbuf::RecordBatch & batch, const UnorderedSetWithMemoryTracking<String> * keep_top_level_fields)
{
    const size_t num_buffers = batch.buffers() ? batch.buffers()->size() : 0;
    /// No pruning requested: every buffer is reachable (the caller reads the whole body).
    if (!keep_top_level_fields)
        return VectorWithMemoryTracking<char>(num_buffers, 1);

    VectorWithMemoryTracking<char> reachable(num_buffers, 0);

    /// Walk the schema with the decoder's own cursor logic (`skipField`) so the buffer spans match decoding
    /// exactly. No body is needed: buffer counts come from the field types and the batch's variadic counts.
    /// `buffer_slices` must be addressable for `nextBuffer()` during the walk, but the slices are never read.
    current_batch = &batch;
    node_index = 0;
    buffer_index = 0;
    variadic_index = 0;
    variadic_counts.clear();
    buffer_slices.assign(num_buffers, Slice{});

    try
    {
        if (const auto * counts = batch.variadicBufferCounts())
        {
            for (Int64 c : *counts)
            {
                if (c < 0)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC variadic buffer count is negative ({})", c);
                variadic_counts.push_back(c);
            }
        }

        const bool case_insensitive = settings.arrow.case_insensitive_column_matching;
        for (const ArrowField & field : schema.fields)
        {
            String normalized_name = field.name;
            if (case_insensitive)
                boost::to_lower(normalized_name);
            const size_t start = buffer_index;
            skipField(field);
            const size_t end = buffer_index;
            if (keep_top_level_fields->contains(normalized_name))
            {
                for (size_t i = start; i < end && i < num_buffers; ++i)
                    reachable[i] = 1;
            }
        }
    }
    catch (const Exception &)
    {
        /// The layout could not be pre-walked (e.g. an unrequested unsupported column with an unknown
        /// layout). Fall back to reading the whole body; the decode path then reports the precise error.
        reachable.assign(num_buffers, 1);
    }

    current_batch = nullptr;
    node_index = 0;
    buffer_index = 0;
    variadic_index = 0;
    variadic_counts.clear();
    buffer_slices.clear();
    return reachable;
}

void RecordBatchDecoder::validateBatchLayout(const flatbuf::RecordBatch & batch, const ArrowFields & fields)
{
    const size_t total_nodes = batch.nodes() ? batch.nodes()->size() : 0;
    const size_t total_buffers = batch.buffers() ? batch.buffers()->size() : 0;

    current_batch = &batch;
    node_index = 0;
    buffer_index = 0;
    variadic_index = 0;
    variadic_counts.clear();
    if (const auto * counts = batch.variadicBufferCounts())
    {
        for (Int64 c : *counts)
        {
            if (c < 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC variadic buffer count is negative ({})", c);
            variadic_counts.push_back(c);
        }
    }
    /// `skipField` pops slices via `nextBuffer`; give it placeholders so the cursor can advance. A batch
    /// declaring fewer buffers than the field needs makes `nextBuffer` throw here, before any materialization.
    buffer_slices.assign(total_buffers, Slice{});

    for (const ArrowField & field : fields)
        skipField(field);

    const bool exact = node_index == total_nodes && buffer_index == total_buffers
        && variadic_index == variadic_counts.size();

    current_batch = nullptr;
    node_index = 0;
    buffer_index = 0;
    variadic_index = 0;
    const size_t declared_variadic = variadic_counts.size();
    variadic_counts.clear();
    buffer_slices.clear();

    if (!exact)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Arrow IPC dictionary batch declares {} field nodes, {} buffers and {} variadic counts, which do "
            "not match the dictionary value field's layout",
            total_nodes, total_buffers, declared_variadic);
}

DataTypePtr RecordBatchDecoder::resolveTargetHint(const DataTypePtr & parent_hint, const String & path) const
{
    /// A hint derived from the parent (Array element, Tuple element, Map key/value) wins; it already
    /// reflects this exact node. Otherwise look the dotted column name up in the caller's requested types,
    /// which resolves a `date32` addressed as a subcolumn (e.g. `t.d`).
    if (parent_hint)
        return parent_hint;
    if (target_types)
    {
        auto it = target_types->find(path);
        if (it != target_types->end())
            return it->second;
    }
    return nullptr;
}

}

#endif
