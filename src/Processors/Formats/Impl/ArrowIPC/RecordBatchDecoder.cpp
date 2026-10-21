#include <Processors/Formats/Impl/ArrowIPC/RecordBatchDecoder.h>

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/BufferCompression.h>
#include <IO/NetUtils.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnConst.h>
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
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/IDataType.h>
#include <Common/assert_cast.h>
#include <Common/FloatUtils.h>
#include <Common/DateLUTImpl.h>
#include <Functions/DateTimeTransforms.h>
#include <Core/UUID.h>
#include <boost/algorithm/string/case_conv.hpp>

#include <algorithm>
#include <limits>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
    extern const int TYPE_MISMATCH;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}
}

namespace DB::ArrowIPC
{

namespace
{
/// Match the declared type to the decoded column, preserving tuple names. Nullable structs retain their
/// null maps until conversion, including when schema inference produces a plain `Tuple`.
DataTypePtr matchColumnNullability(const DataTypePtr & type, const ColumnPtr & column)
{
    const bool is_nullable = column->isNullable();
    const IColumn & nested_column = is_nullable
        ? assert_cast<const ColumnNullable &>(*column).getNestedColumn() : *column;
    DataTypePtr nested_type = removeNullable(type);

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(nested_type.get()))
    {
        const auto & array_column = assert_cast<const ColumnArray &>(nested_column);
        nested_type = std::make_shared<DataTypeArray>(
            matchColumnNullability(array_type->getNestedType(), array_column.getDataPtr()));
    }
    else if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(nested_type.get()))
    {
        const auto & tuple_column = assert_cast<const ColumnTuple &>(nested_column);
        DataTypes elements(tuple_type->getElements().size());
        for (size_t i = 0; i < elements.size(); ++i)
            elements[i] = matchColumnNullability(tuple_type->getElement(i), tuple_column.getColumnPtr(i));
        nested_type = tuple_type->hasExplicitNames()
            ? std::make_shared<DataTypeTuple>(elements, tuple_type->getElementNames())
            : std::make_shared<DataTypeTuple>(elements);
    }
    else if (const auto * map_type = typeid_cast<const DataTypeMap *>(nested_type.get()))
    {
        const auto & entries = assert_cast<const ColumnMap &>(nested_column).getNestedData();
        nested_type = std::make_shared<DataTypeMap>(
            matchColumnNullability(map_type->getKeyType(), entries.getColumnPtr(0)),
            matchColumnNullability(map_type->getValueType(), entries.getColumnPtr(1)));
    }

    return is_nullable ? std::make_shared<DataTypeNullable>(nested_type) : nested_type;
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

}

DataTypePtr stripHint(const DataTypePtr & type)
{
    if (!type)
        return nullptr;
    return removeLowCardinalityAndNullable(type);
}

DataTypePtr arrayElementHint(const DataTypePtr & hint)
{
    if (const auto * array = typeid_cast<const DataTypeArray *>(stripHint(hint).get()))
        return array->getNestedType();
    return nullptr;
}

DataTypePtr tupleElementHint(const DataTypePtr & hint, const String & child_name, size_t pos, bool case_insensitive)
{
    const auto * tuple = typeid_cast<const DataTypeTuple *>(stripHint(hint).get());
    if (!tuple)
        return nullptr;
    if (tuple->hasExplicitNames())
    {
        if (const auto position = tuple->tryGetPositionByName(child_name, case_insensitive))
            return tuple->getElements()[*position];
        return nullptr;
    }
    if (pos < tuple->getElements().size())
        return tuple->getElements()[pos];
    return nullptr;
}

DataTypePtr mapEntriesHint(const DataTypePtr & hint)
{
    if (const auto * map = typeid_cast<const DataTypeMap *>(stripHint(hint).get()))
        return std::make_shared<DataTypeTuple>(DataTypes{map->getKeyType(), map->getValueType()});
    return nullptr;
}

size_t rawByteWidth(const WhichDataType & which)
{
    if (which.isIPv6() || which.isInt128() || which.isUInt128())
        return 16;
    if (which.isInt256() || which.isUInt256())
        return 32;
    return 0;
}

namespace
{

/// The IPv6 / big-integer type a hint requests for a variable binary leaf, or null when it requests
/// none of those. The conversion runs right after the leaf decodes (see the Utf8/Binary and view
/// branches of `decodeInner`), where the invisible-rows mask still exists — hidden bytes under a
/// dropped struct null map or in a masked list range must not force the column into the text-parsed
/// String fallback. The post-decode raw-byte rewrite in `ArrowIPCBlockInputFormat` then only
/// reconciles the declared type.
DataTypePtr rawByteTargetType(const DataTypePtr & hint)
{
    if (!hint)
        return nullptr;
    DataTypePtr stripped = stripHint(hint);
    if (rawByteWidth(WhichDataType(stripped)) != 0)
        return stripped;
    return nullptr;
}

}

String DictionaryRegistry::positionKey(const FieldPosition & position)
{
    return fmt::format("{}/{}", position.list_depth, position.path);
}

void DictionaryRegistry::set(Int64 id, const FieldPosition & position, Values values, bool is_delta)
{
    if (!is_delta)
    {
        Dictionary dictionary;
        dictionary.offsets.push_back(values.column->size());
        dictionary.segments.push_back(std::move(values));
        dictionaries[id][positionKey(position)] = std::move(dictionary);
        return;
    }

    auto it = dictionaries.find(id);
    if (it == dictionaries.end())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC delta dictionary batch for unknown dictionary id {}", id);
    Dictionary & dictionary = it->second.at(positionKey(position));
    const IColumn * base_column = dictionary.segments.front().column.get();
    const IColumn * delta_column = values.column.get();
    if (const auto * constant = typeid_cast<const ColumnConst *>(base_column))
        base_column = &constant->getDataColumn();
    if (const auto * constant = typeid_cast<const ColumnConst *>(delta_column))
        delta_column = &constant->getDataColumn();

    /// Requested raw-byte conversions can produce different layouts in separate dictionary batches.
    /// Every batch must share a layout so its referenced values can be gathered into one column.
    if (!base_column->structureEquals(*delta_column))
        throw Exception(
            ErrorCodes::TYPE_MISMATCH,
            "Arrow IPC delta dictionary batch for dictionary {} decodes to {} under the requested type, but the "
            "dictionary's earlier values decoded to {}",
            id, delta_column->getName(), base_column->getName());

    size_t total_rows = 0;
    if (__builtin_add_overflow(dictionary.size(), values.column->size(), &total_rows))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC dictionary {} length overflows", id);
    Values & last = dictionary.segments.back();
    if (!isColumnConst(*last.column) && !isColumnConst(*values.column))
    {
        const size_t previous_rows = last.column->size();
        const size_t added_rows = values.column->size();
        auto merged = IColumn::mutate(std::move(last.column));
        merged->insertRangeFrom(*values.column, 0, added_rows);
        last.column = std::move(merged);
        if (last.null_map || values.null_map)
        {
            MutableColumnPtr null_map;
            if (last.null_map)
                null_map = IColumn::mutate(std::move(last.null_map));
            else
                null_map = ColumnUInt8::create(previous_rows, UInt8{0});
            if (values.null_map)
                null_map->insertRangeFrom(*values.null_map, 0, added_rows);
            else
                null_map->insertManyDefaults(added_rows);
            last.null_map = std::move(null_map);
        }
        dictionary.offsets.back() = total_rows;
    }
    else
    {
        dictionary.offsets.push_back(total_rows);
        dictionary.segments.push_back(std::move(values));
    }
}

const DictionaryRegistry::Dictionary & DictionaryRegistry::get(Int64 id, const FieldPosition & position) const
{
    auto it = dictionaries.find(id);
    if (it == dictionaries.end())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC record batch references unknown dictionary id {}", id);
    return it->second.at(positionKey(position));
}

MutableColumnPtr reinterpretStringLeaf(const ColumnString & str, const NullMap * null_map, const DataTypePtr & to_no_null)
{
    const size_t width = rawByteWidth(WhichDataType(to_no_null));
    if (width == 0)
        return nullptr;

    const size_t rows = str.size();
    for (size_t i = 0; i < rows; ++i)
    {
        if (null_map && (*null_map)[i])
            continue;
        if (str.getDataAt(i).size() != width)
            return nullptr;
    }

    auto out = to_no_null->createColumn();
    out->reserve(rows);
    for (size_t i = 0; i < rows; ++i)
    {
        if (null_map && (*null_map)[i])
        {
            out->insertDefault();
            continue;
        }
        const auto ref = str.getDataAt(i);
        out->insertData(ref.data(), ref.size());
    }
    return out;
}

const flatbuf::FieldNode & RecordBatchDecoder::nextNode()
{
    const auto * nodes = current_batch->nodes();
    if (!nodes || node_index >= nodes->size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC record batch has fewer field nodes than the schema requires");
    return *nodes->Get(static_cast<flatbuffers::uoffset_t>(node_index++));
}

const flatbuf::FieldNode & RecordBatchDecoder::peekNode(size_t offset) const
{
    const auto * nodes = current_batch->nodes();
    if (!nodes || node_index + offset >= nodes->size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC record batch has fewer field nodes than the schema requires");
    return *nodes->Get(static_cast<flatbuffers::uoffset_t>(node_index + offset));
}

Int64 RecordBatchDecoder::peekNextNodeLength() const
{
    return peekNode(0).length();
}

void RecordBatchDecoder::expectNextNodeLength(size_t expected, const String & what) const
{
    const Int64 declared = peekNextNodeLength();
    if (declared < 0 || static_cast<size_t>(declared) != expected)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC {} declares {} rows, expected {}", what, declared, expected);
}

void RecordBatchDecoder::expectNextNodeLengthAtLeast(size_t minimum, const String & what) const
{
    const Int64 declared = peekNextNodeLength();
    if (declared < 0 || static_cast<size_t>(declared) < minimum)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Arrow IPC {} declares {} rows, fewer than the {} its parent references", what, declared, minimum);
}

void RecordBatchDecoder::checkRowCountWithinBody(size_t rows, const String & what) const
{
    if (rows / 8 + (rows % 8 != 0) > total_buffer_bytes)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Arrow IPC {} declares {} rows, more than the {}-byte message body can hold", what, rows, total_buffer_bytes);
}

size_t RecordBatchDecoder::peekNodeRows() const
{
    return static_cast<size_t>(std::max<Int64>(peekNextNodeLength(), 0));
}

namespace
{

/// Whether row `i` is invisible under an optional mask; a null mask means nothing is invisible.
bool isInvisible(const InvisibleRowsMask * mask, size_t i)
{
    return mask && (*mask)[i];
}

const InvisibleRowsMask * maskPtr(const std::optional<InvisibleRowsMask> & mask)
{
    return mask ? &*mask : nullptr;
}

/// Dictionary values reflect the referencing field's nullability. A non-nullable field removes any
/// `Nullable` wrapper and cannot reference null entries. `null_entries` preserves that restriction for
/// value types such as `Array`, `Map`, and plain `Tuple` whose decoded columns have no outer null map.
struct FieldDictionaryValues
{
    ColumnPtr column;
    DataTypePtr type;
    const IColumn * null_entries = nullptr;
};

FieldDictionaryValues dictionaryValuesFor(const ArrowField & field, const DictionaryRegistry::Values & registered)
{
    if (field.nullable)
        return {registered.column, registered.type, nullptr};

    ColumnPtr column;
    if (const auto * constant = typeid_cast<const ColumnConst *>(registered.column.get()))
        column = ColumnConst::create(removeNullable(constant->getDataColumnPtr()), constant->size());
    else
        column = removeNullable(registered.column);
    return {std::move(column), removeNullable(registered.type), registered.null_map.get()};
}

/// The requested type hint of a field at dotted name `path`, `list_depth` lists below the top level. A
/// hint derived from the parent (Array element, Tuple element, Map key/value) wins: it already reflects
/// this exact node. Otherwise the dotted name is looked up in `types` (null: nothing to look up), which
/// resolves a `date32` addressed as a subcolumn (e.g. `t.d`). The looked-up type describes the flattened
/// column, one Array layer per List/Map level crossed on the way here (`Nested(d Int32)` requests
/// `n.d Array(Int32)` while this field's own type is the element); those layers are peeled so the hint
/// matches this field, exactly as the parent-derived chain unwraps one Array per list. A type with fewer
/// layers does not describe this field: no hint.
DataTypePtr resolveHint(
    const DataTypePtr & parent_hint, const String & path, size_t list_depth,
    const UnorderedMapWithMemoryTracking<String, DataTypePtr> * types)
{
    if (parent_hint)
        return parent_hint;
    if (!types)
        return nullptr;
    auto it = types->find(path);
    if (it == types->end())
        return nullptr;
    DataTypePtr hint = it->second;
    for (size_t i = 0; i < list_depth && hint; ++i)
        hint = arrayElementHint(hint);
    return hint;
}

/// A `Null` field has no buffers. A struct or fixed-size-list tree of such fields needs only optional
/// validity buffers, so its logical row count can be independent of the message body size.
bool isBufferlessSubtree(const ArrowField & field)
{
    if (field.dictionary)
        return false;
    switch (field.type.kind)
    {
        case TypeKind::Null:
            return true;
        case TypeKind::Struct:
            return std::ranges::all_of(field.type.children, isBufferlessSubtree);
        case TypeKind::FixedSizeList:
            return isBufferlessSubtree(field.type.children.at(0));
        default:
            return false;
    }
}

/// An invisible Array/Map row decodes as the type default — the empty range — the same way the native
/// Parquet reader materializes a null list slot (its definition levels reference no child values at
/// all). Keeping the spec-undefined range would resurface it as a value whenever the slot's null map
/// is dropped, which is always: Array/Map cannot be inside Nullable in ClickHouse. Rewrites `offs`
/// (ClickHouse cumulative lengths) in place to empty every invisible slot's range and returns the number
/// of child rows the visible slots still reference, i.e. the size the child has once the emptied ranges
/// are dropped. The offsets buffer itself stays structurally validated; only the decoded ranges change.
size_t emptyInvisibleSlotRanges(ColumnUInt64::Container & offs, const InvisibleRowsMask * invisible_rows)
{
    if (!invisible_rows)
        return offs.empty() ? 0 : offs.back();

    UInt64 kept = 0;
    UInt64 begin = 0;
    for (size_t i = 0; i < offs.size(); ++i)
    {
        const UInt64 end = offs[i];
        if (!(*invisible_rows)[i])
            kept += end - begin;
        begin = end;
        offs[i] = kept;
    }
    return kept;
}

/// `emptyInvisibleSlotRanges` for a decoded child of `child_rows` rows (the last entry of `offs`): also
/// returns the filter selecting the child rows the visible slots still reference, or std::nullopt when no
/// invisible slot references any row — the child then needs no filtering and `offs` stays untouched.
std::optional<IColumn::Filter> emptyInvisibleSlots(
    ColumnUInt64::Container & offs, size_t child_rows, const InvisibleRowsMask * invisible_rows)
{
    if (!invisible_rows)
        return std::nullopt;

    bool any_referencing_invisible = false;
    UInt64 begin = 0;
    for (size_t i = 0; i < offs.size(); ++i)
    {
        if ((*invisible_rows)[i] && offs[i] > begin)
        {
            any_referencing_invisible = true;
            break;
        }
        begin = offs[i];
    }
    if (!any_referencing_invisible)
        return std::nullopt;

    IColumn::Filter filt(child_rows, 0);
    begin = 0;
    for (size_t i = 0; i < offs.size(); ++i)
    {
        const UInt64 end = offs[i];
        if (!(*invisible_rows)[i] && end > begin)
            memset(filt.data() + begin, 1, end - begin);
        begin = end;
    }
    emptyInvisibleSlotRanges(offs, invisible_rows);
    return filt;
}

}

std::optional<InvisibleRowsMask> RecordBatchDecoder::buildOffsetsChildInvisibleMask(
    size_t rows, Int64 base, Int64 prev, const PaddedPODArray<UInt64> & offsets, const InvisibleRowsMask * invisible_rows) const
{
    const size_t child_rows = peekNodeRows();
    const bool has_unreferenced_rows = static_cast<size_t>(base) > 0 || static_cast<size_t>(prev) < child_rows;
    if (!invisible_rows && !has_unreferenced_rows)
        return std::nullopt;

    /// Start all-invisible and clear each visible slot's range: rows of invisible slots and rows no
    /// slot references then stay marked without being enumerated.
    InvisibleRowsMask mask;
    mask.resize_fill(child_rows, 1);
    for (size_t i = 0; i < rows; ++i)
    {
        if (isInvisible(invisible_rows, i))
            continue;

        const size_t range_begin = std::min<size_t>(static_cast<size_t>(base) + (i == 0 ? 0 : offsets[i - 1]), child_rows);
        const size_t range_end = std::min<size_t>(static_cast<size_t>(base) + offsets[i], child_rows);
        if (range_begin < range_end)
            memset(mask.data() + range_begin, 0, range_end - range_begin);
    }
    return mask;
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

size_t dictionaryIndexByteWidth(int bits)
{
    if (bits != 8 && bits != 16 && bits != 32 && bits != 64)
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "Arrow IPC dictionary index bit width {} is not supported (must be 8, 16, 32, or 64)", bits);
    return static_cast<size_t>(bits) / 8;
}

/// The number of child rows a fixed-size list of `rows` rows holds. `list_size` is untrusted IPC metadata:
/// a negative value would wrap to a huge `size_t`, and a zero would make the child length independent of
/// `rows`, leaving a forged parent row count unbounded. Reject a non-positive size, and multiply with the
/// overflow check of `requiredBytes` so `rows * list_size` cannot wrap to disguise a forged `rows`.
size_t fixedSizeListChildRows(const ArrowType & type, size_t rows)
{
    if (type.list_size <= 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC fixed-size-list has a non-positive list size {}", type.list_size);
    return requiredBytes(rows, static_cast<size_t>(type.list_size));
}

/// Zeroes the value slots of the invisible rows of a fixed-width column, whose values sit at
/// `value_size` byte strides in `data`. Their bytes are undefined per the Arrow spec, so they must not
/// surface as values: dropping an ancestor null map (a struct read as a plain `Tuple`) turns such a row
/// into a visible one. All-zero bytes are the default of every fixed-width type the decoder builds.
void defaultInvisibleFixed(char * data, size_t value_size, size_t rows, const InvisibleRowsMask * invisible_rows)
{
    if (!invisible_rows)
        return;
    for (size_t i = 0; i < rows; ++i)
        if ((*invisible_rows)[i])
            memset(data + i * value_size, 0, value_size);
}

/// Copies fixed-width values into a `ColumnVector` or `ColumnDecimal`. The source buffer has
/// `arrow_value_size` bytes per row; narrower ClickHouse decimals keep each value's low bytes.
template <typename Col>
void fillFixed(
    IColumn & column, size_t rows, const RecordBatchDecoder::Slice & values, size_t arrow_value_size,
    const InvisibleRowsMask * invisible_rows)
{
    using V = typename Col::ValueType;
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
    defaultInvisibleFixed(reinterpret_cast<char *>(data.data()), sizeof(V), rows, invisible_rows);
}

}

ColumnUInt8::Ptr RecordBatchDecoder::buildNullMap(const Slice & validity, size_t rows, Int64 null_count) const
{
    auto null_map = ColumnUInt8::create(rows);
    auto & data = null_map->getData();
    if (null_count == 0)
        memset(data.data(), 0, rows);
    else
    {
        /// Arrow validity uses 1 for valid rows; a ClickHouse null map uses 1 for null rows.
        const auto * bits = reinterpret_cast<const uint8_t *>(validity.ptr);
        expandBitmapToBytes(bits, rows, data.data(), /*invert=*/true);
    }
    return null_map;
}

ColumnPtr RecordBatchDecoder::decodeInner(
    const ArrowField & field, size_t rows, const DataTypePtr & target_hint, const String & path,
    size_t list_depth, const InvisibleRowsMask * invisible_rows)
{
    const ArrowType & type = field.type;
    DataTypePtr inner_type = fieldToCHType(field, settings, /*make_nullable=*/false, /*allow_null_type=*/true);
    auto column = inner_type->createColumn();

    /// This field's requested ClickHouse type (parent-derived hint, or a dotted-name lookup), used only to
    /// decide whether a `date32` maps to a numeric target and is read raw; and to derive child hints below.
    /// Decimal targets take the raw read too: no `Date32` -> Decimal cast exists, so the raw day number is
    /// the only value the request can mean (the post-decode rewrite re-declares the column as `Int32` for
    /// the Int -> Decimal cast).
    const DataTypePtr effective_hint = resolveTargetHint(target_hint, path, list_depth);
    const DataTypePtr stripped_effective_hint = stripHint(effective_hint);
    const bool date32_as_number = stripped_effective_hint && (isNumber(stripped_effective_hint) || isDecimal(stripped_effective_hint));

    switch (type.kind)
    {
        case TypeKind::Int:
        {
            const Slice values = nextBuffer();
            if (type.is_signed)
            {
                switch (type.bit_width)
                {
                    case 8: fillFixed<ColumnInt8>(*column, rows, values, 1, invisible_rows); break;
                    case 16: fillFixed<ColumnInt16>(*column, rows, values, 2, invisible_rows); break;
                    case 32: fillFixed<ColumnInt32>(*column, rows, values, 4, invisible_rows); break;
                    case 64: fillFixed<ColumnInt64>(*column, rows, values, 8, invisible_rows); break;
                    default: throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported Arrow int bit width {}", type.bit_width);
                }
            }
            else
            {
                switch (type.bit_width)
                {
                    case 8: fillFixed<ColumnUInt8>(*column, rows, values, 1, invisible_rows); break;
                    case 16: fillFixed<ColumnUInt16>(*column, rows, values, 2, invisible_rows); break;
                    case 32: fillFixed<ColumnUInt32>(*column, rows, values, 4, invisible_rows); break;
                    case 64: fillFixed<ColumnUInt64>(*column, rows, values, 8, invisible_rows); break;
                    default: throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported Arrow int bit width {}", type.bit_width);
                }
            }
            break;
        }
        case TypeKind::FloatingPoint:
        {
            const Slice values = nextBuffer();
            if (type.float_precision == flatbuf::Precision_DOUBLE)
                fillFixed<ColumnFloat64>(*column, rows, values, 8, invisible_rows);
            else if (type.float_precision == flatbuf::Precision_SINGLE)
                fillFixed<ColumnFloat32>(*column, rows, values, 4, invisible_rows);
            else
            {
                /// half-float -> Float32
                auto & data = assert_cast<ColumnFloat32 &>(*column).getData();
                data.resize(rows);
                const auto * src = reinterpret_cast<const UInt16 *>(values.ptr);
                for (size_t i = 0; i < rows; ++i)
                    data[i] = isInvisible(invisible_rows, i) ? 0 : convertFloat16ToFloat32(src[i]);
            }
            break;
        }
        case TypeKind::Bool:
        {
            const Slice values = nextBuffer();
            auto & data = assert_cast<ColumnUInt8 &>(*column).getData();
            data.resize(rows);
            const auto * bits = reinterpret_cast<const uint8_t *>(values.ptr);
            expandBitmapToBytes(bits, rows, data.data(), /*invert=*/false);
            defaultInvisibleFixed(reinterpret_cast<char *>(data.data()), sizeof(UInt8), rows, invisible_rows);
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
                fillFixed<ColumnDecimal<Decimal>>(*column, rows, values, arrow_value_size, invisible_rows);
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
                    /// The raw read skips the range check; `fillFixed` still defaults the invisible
                    /// slots, whose bytes must not surface as values (a dropped struct null map turns
                    /// such rows into visible ones), exactly as the checked branch below does.
                    fillFixed<ColumnInt32>(*column, rows, values, sizeof(Int32), invisible_rows);
                    break;
                }
                /// Otherwise enforce the same range/overflow contract as the library reader
                /// (`readColumnWithDate32Data`): a day number outside ClickHouse's allowed Date32 range is
                /// saturated or rejected according to `date_time_overflow_behavior` (its default `Ignore`,
                /// like `Throw`, rejects — preserving the pre-`date_time_overflow_behavior` behavior)
                /// instead of leaving an invalid Date32 in the result. When the requested header type is
                /// `Date`, enforce the narrower Date range [0, 65535] instead: `buildChunk` later casts the
                /// intermediate Date32 column to `Date` without checks, narrowing the day number to UInt16,
                /// so an unchecked extended Date32 value would wrap into an unrelated in-range `Date`.
                /// Similarly, when the requested header type is `DateTime`, enforce the
                /// [0, MAX_DATETIME_DAY_NUM] window that `ToDateTimeImpl` uses: the later context-less cast
                /// ignores `date_time_overflow_behavior` and wraps day numbers whose midnight does not fit.
                /// A `DateTime64` header type needs the same treatment, but its window is scale-dependent: the
                /// context-less cast clamps whole seconds the target scale cannot represent, and a scale-9
                /// `DateTime64` stops at `2262-04-11`, far below the Date32 upper bound.
                const bool date32_as_date = stripped_effective_hint && isDate(*stripped_effective_hint);
                const bool date32_as_datetime = stripped_effective_hint && isDateTime(*stripped_effective_hint);
                const auto * dt64_hint
                    = stripped_effective_hint ? typeid_cast<const DataTypeDateTime64 *>(stripped_effective_hint.get()) : nullptr;
                const auto [dt64_min_day, dt64_max_day] = dt64_hint
                    ? getDateTime64DayNumRange(
                          DecimalUtils::scaleMultiplier<DateTime64::NativeType>(dt64_hint->getScale()), dt64_hint->getTimeZone())
                    : std::pair<Int32, Int32>{DATE_LUT_MIN_EXTEND_DAY_NUM, DATE_LUT_MAX_EXTEND_DAY_NUM};
                const Int32 min_day = (date32_as_date || date32_as_datetime) ? 0
                    : dt64_hint ? dt64_min_day
                    : DATE_LUT_MIN_EXTEND_DAY_NUM;
                const Int32 max_day = date32_as_date ? DATE_LUT_MAX_DAY_NUM
                    : date32_as_datetime ? static_cast<Int32>(MAX_DATETIME_DAY_NUM)
                    : dt64_hint ? dt64_max_day
                    : DATE_LUT_MAX_EXTEND_DAY_NUM;
                const String target_type_name = date32_as_date ? "Date"
                    : date32_as_datetime ? "DateTime"
                    : dt64_hint ? stripped_effective_hint->getName()
                    : "Date32";
                auto & data = assert_cast<ColumnInt32 &>(*column).getData();
                data.resize(rows);
                const auto * src = reinterpret_cast<const Int32 *>(values.ptr);
                const bool saturate = settings.date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Saturate;
                for (size_t i = 0; i < rows; ++i)
                {
                    /// The bytes of an invisible slot (see `InvisibleRowsMask`) are undefined per the
                    /// Arrow spec, so they must not be range-checked; decode them as the type default.
                    if (isInvisible(invisible_rows, i))
                    {
                        data[i] = 0;
                        continue;
                    }
                    Int32 days = src[i];
                    if (days > max_day || days < min_day)
                    {
                        if (saturate)
                            days = days < min_day ? min_day : max_day;
                        else
                            throw Exception(
                                ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                                "Arrow IPC date32 value {} is out of the allowed {} range [{}, {}]",
                                days, target_type_name, min_day, max_day);
                    }
                    data[i] = days;
                }
            }
            else
            {
                /// date64: milliseconds since the epoch, maps to DateTime (UInt32 seconds).
                auto & data = assert_cast<ColumnUInt32 &>(*column).getData();
                data.resize(rows);
                const auto * src = reinterpret_cast<const Int64 *>(values.ptr);
                for (size_t i = 0; i < rows; ++i)
                    data[i] = isInvisible(invisible_rows, i) ? 0 : static_cast<UInt32>(src[i] / 1000);
            }
            break;
        }
        case TypeKind::Timestamp:
        {
            /// Maps to DateTime64(unit*3); the raw int64 value is exactly the underlying value at that scale.
            const Slice values = nextBuffer();
            fillFixed<ColumnDecimal<DateTime64>>(*column, rows, values, 8, invisible_rows);
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
                auto & data = assert_cast<ColumnDecimal<Time64> &>(*column).getData();
                data.resize(rows);
                const auto * src = reinterpret_cast<const Int32 *>(values.ptr);
                for (size_t i = 0; i < rows; ++i)
                    data[i] = isInvisible(invisible_rows, i) ? Time64(0) : Time64(src[i]);
                break;
            }
            fillFixed<ColumnDecimal<Time64>>(*column, rows, values, 8, invisible_rows);
            break;
        }
        case TypeKind::Duration:
        {
            /// Maps to Interval (stored as Int64); the raw int64 count in the duration's unit.
            const Slice values = nextBuffer();
            fillFixed<ColumnInt64>(*column, rows, values, 8, invisible_rows);
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
                /// are read. Insert the empty value without touching the data pointer. An invisible slot's
                /// bytes are undefined, so they are not copied either (the offsets themselves stay validated
                /// above: monotonicity and the data bound are structural, not value-level, properties).
                if (end == prev || isInvisible(invisible_rows, i))
                    string_column.insertData("", 0);
                else
                    string_column.insertData(data_slice.ptr + prev, static_cast<size_t>(end - prev));
                prev = end;
            }
            /// A type hint can request the raw bytes as an IPv6 or big integer (how the ClickHouse
            /// writer stores those types); convert here, where the invisible-rows mask still exempts
            /// hidden bytes from the width sniff (see `rawByteTargetType`).
            if (const DataTypePtr raw_target = rawByteTargetType(effective_hint))
                if (MutableColumnPtr typed = reinterpretStringLeaf(string_column, invisible_rows, raw_target))
                    return typed;
            break;
        }
        case TypeKind::BinaryView:
        case TypeKind::Utf8View:
        {
            /// Layout: validity (consumed), a 16-byte-per-row views buffer, then `variadic_counts` data
            /// buffers. Each view is {int32 length; if length<=12 inline 12 bytes; else int32 prefix,
            /// int32 buffer_index, int32 offset into that data buffer}.
            const Slice views = nextBuffer();
            const Int64 num_data = variadic_index < variadic_counts.size() ? variadic_counts[variadic_index] : 0;
            ++variadic_index;
            VectorWithMemoryTracking<Slice> data_buffers;
            data_buffers.reserve(static_cast<size_t>(num_data));
            for (Int64 i = 0; i < num_data; ++i)
                data_buffers.push_back(nextBuffer());

            auto & string_column = assert_cast<ColumnString &>(*column);
            string_column.reserve(rows);
            for (size_t i = 0; i < rows; ++i)
            {
                if (isInvisible(invisible_rows, i))
                {
                    string_column.insertDefault();
                    continue;
                }
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
            /// Same raw-byte type-hint conversion as the Utf8/Binary branch above.
            if (const DataTypePtr raw_target = rawByteTargetType(effective_hint))
                if (MutableColumnPtr typed = reinterpretStringLeaf(string_column, invisible_rows, raw_target))
                    return typed;
            break;
        }
        case TypeKind::FixedSizeBinary:
        {
            const Slice values = nextBuffer();
            const size_t n = static_cast<size_t>(type.byte_width);
            if (isUUIDField(field))
            {
                /// 16 bytes per value, with the two 64-bit halves byte-reversed (matches the writer).
                auto & data = assert_cast<ColumnVector<UUID> &>(*column).getData();
                data.resize(rows);
                for (size_t i = 0; i < rows; ++i)
                {
                    auto * dst = reinterpret_cast<uint8_t *>(&data[i]);
                    /// The bytes of an invisible slot are undefined; a self-describing `uuid` extension
                    /// field decodes straight into `UUID` here, with no later rewrite that could default
                    /// them, so they must not be copied at all.
                    if (isInvisible(invisible_rows, i))
                    {
                        memset(dst, 0, 16);
                        continue;
                    }
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
            /// A `UUID` / `IPv6` / big-integer type hint reinterprets these raw bytes verbatim
            /// (`reinterpretFixedStringLeaf`), so defaulting the invisible slots here is what keeps their
            /// undefined bytes out of those targets as well.
            defaultInvisibleFixed(reinterpret_cast<char *>(chars.data()), n, rows, invisible_rows);
            break;
        }
        case TypeKind::List:
        case TypeKind::LargeList:
            return readOffsetsAndChild(
                field, rows, /*large=*/type.kind == TypeKind::LargeList, arrayElementHint(effective_hint), path,
                list_depth, invisible_rows);
        case TypeKind::FixedSizeList:
        {
            /// Each row owns `list_size` child elements; the layout pass validates their total count.
            const size_t list_size = static_cast<size_t>(type.list_size);
            const size_t expected_child = rows * list_size;
            const ArrowField & child_field = type.children.at(0);

            /// Children with physical data remain subject to the message-body row bound. Children
            /// determined by their size alone are materialized only for visible slots below.
            const bool size_determined_child = isSizeDeterminedSubtree(child_field);
            if (!size_determined_child)
                checkRowCountWithinBody(expected_child, "fixed-size-list child");

            auto offsets_col = ColumnUInt64::create(rows);
            auto & offs = offsets_col->getData();
            for (size_t i = 0; i < rows; ++i)
                offs[i] = (i + 1) * list_size;

            /// A child determined by its size alone is built for the visible slots only: the elements of
            /// invisible slots are never materialized, however large `list_size` makes them (see
            /// `buildSizeDeterminedColumn`).
            if (size_determined_child)
            {
                const size_t kept = emptyInvisibleSlotRanges(offs, invisible_rows);
                ColumnPtr child = buildSizeDeterminedColumn(child_field, kept, arrayElementHint(effective_hint), path, list_depth + 1);
                return ColumnArray::create(child, std::move(offsets_col));
            }

            /// Every child row belongs to the slot at `j / list_size`; a child row of an invisible slot is
            /// itself invisible.
            const std::optional<InvisibleRowsMask> child_invisible = [&]() -> std::optional<InvisibleRowsMask>
            {
                if (!invisible_rows)
                    return std::nullopt;
                InvisibleRowsMask mask;
                mask.resize(expected_child);
                for (size_t j = 0; j < expected_child; ++j)
                    mask[j] = (*invisible_rows)[j / list_size];
                return mask;
            }();
            ColumnPtr child = decodeField(
                child_field, /*allow_low_cardinality=*/false, arrayElementHint(effective_hint), path, list_depth + 1,
                maskPtr(child_invisible));
            if (const auto filt = emptyInvisibleSlots(offs, child->size(), invisible_rows))
                child = child->filter(*filt, -1);
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
                /// Struct children are row-aligned with the parent, so the invisible-rows mask (which
                /// already includes this struct's own nulls, composed in `decodeField`) passes through
                /// unchanged: the bytes of a child slot under a null struct row are undefined per the
                /// Arrow spec even when the child's own validity marks the slot valid.
                ColumnPtr element = decodeField(
                    child, /*allow_low_cardinality=*/false,
                    tupleElementHint(effective_hint, child.name, i, settings.arrow.case_insensitive_column_matching),
                    childPath(path, child.name), list_depth, invisible_rows);
                elements.push_back(std::move(element));
            }
            return ColumnTuple::create(elements);
        }
        case TypeKind::Map:
        {
            /// Map is List<Struct<key, value>>: read the list offsets, then the entries struct.
            Int64 base = 0;
            Int64 prev = 0;
            auto offsets_col = decodeListOffsets(rows, /*large=*/false, "map", base, prev);
            auto & offs = offsets_col->getData();
            const ArrowField & entries_field = type.children.at(0);

            /// A sliced map can retain unreferenced entries. Its declared entry count must cover every
            /// referenced entry and stay within the message-body row bound.
            expectNextNodeLengthAtLeast(static_cast<size_t>(prev), "map entries");
            checkRowCountWithinBody(peekNodeRows(), "map entries");

            /// Entry rows that only invisible map slots reference — or that no slot references — hold
            /// undefined bytes and must not be value-validated; see `buildOffsetsChildInvisibleMask`.
            const std::optional<InvisibleRowsMask> entries_invisible
                = buildOffsetsChildInvisibleMask(rows, base, prev, offs, invisible_rows);

            /// The entries struct's (key, value) get their hints from a synthetic Tuple(keyType, valueType)
            /// built from the Map hint; the struct recursion then matches them by position.
            ColumnPtr entries = decodeField(
                entries_field, /*allow_low_cardinality=*/false, mapEntriesHint(effective_hint), path, list_depth + 1,
                maskPtr(entries_invisible));
            const auto & entries_tuple = assert_cast<const ColumnTuple &>(*entries);
            if (entries_tuple.tupleSize() != 2)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC map entries must be a struct of (key, value)");
            /// A sliced Arrow map can begin at a non-zero first offset; only entries[base, prev) are
            /// referenced. Slice the key/value columns to that range so their size matches the base-relative
            /// offsets (matching the Apache Arrow library reader).
            const size_t referenced = static_cast<size_t>(prev - base);
            ColumnPtr keys = entries_tuple.getColumnPtr(0);
            ColumnPtr values = entries_tuple.getColumnPtr(1);
            if (!(base == 0 && referenced == entries_tuple.size()))
            {
                keys = keys->cut(static_cast<size_t>(base), referenced);
                values = values->cut(static_cast<size_t>(base), referenced);
            }
            if (const auto filt = emptyInvisibleSlots(offs, keys->size(), invisible_rows))
            {
                keys = keys->filter(*filt, -1);
                values = values->filter(*filt, -1);
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

ColumnUInt64::MutablePtr RecordBatchDecoder::decodeListOffsets(
    size_t rows, bool large, const char * what, Int64 & base, Int64 & prev)
{
    const Slice offsets_slice = nextBuffer();
    auto read_offset = [&](size_t i) -> Int64
    {
        if (large)
            return reinterpret_cast<const Int64 *>(offsets_slice.ptr)[i];
        return reinterpret_cast<const Int32 *>(offsets_slice.ptr)[i];
    };

    auto offsets_col = ColumnUInt64::create(rows);
    auto & offs = offsets_col->getData();
    base = 0;
    prev = 0;
    /// A zero-row list may omit its offsets buffer, so no offset is read for an empty column.
    if (rows > 0)
    {
        base = read_offset(0);
        /// The first offset must be non-negative; the per-row offsets below are stored relative to it.
        if (base < 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC {} has a negative first offset {}", what, base);
        /// Offsets must be monotonic non-decreasing: compare each with the previous one, not only `base`.
        prev = base;
        for (size_t i = 0; i < rows; ++i)
        {
            const Int64 end = read_offset(i + 1);
            if (end < prev)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC {} has non-monotonic offsets", what);
            offs[i] = static_cast<UInt64>(end - base);
            prev = end;
        }
    }
    return offsets_col;
}

ColumnPtr RecordBatchDecoder::readOffsetsAndChild(
    const ArrowField & field, size_t rows, bool large, const DataTypePtr & target_hint, const String & path,
    size_t list_depth, const InvisibleRowsMask * invisible_rows)
{
    Int64 base = 0;
    Int64 prev = 0;
    auto offsets_col = decodeListOffsets(rows, large, "list", base, prev);
    auto & offs = offsets_col->getData();
    const ArrowField & child_field = field.type.children.at(0);

    /// The child must cover the rows the offsets reference; it may declare more, as a sliced Arrow list
    /// keeps the full child.
    expectNextNodeLengthAtLeast(static_cast<size_t>(prev), "list child");

    /// A child determined by its size alone is built for the rows the visible slots reference only: the
    /// ranges under invisible slots and the unreferenced head and tail of a sliced list are never
    /// materialized, however large the offsets make them (see `buildSizeDeterminedColumn`). Either way the
    /// child gets this list's element hint and dotted name as its requested-type position: a list does not
    /// extend the dotted path, but does add an Array layer to the flattened dotted-name types, hence
    /// `list_depth + 1`.
    if (isSizeDeterminedSubtree(child_field))
    {
        const size_t kept = emptyInvisibleSlotRanges(offs, invisible_rows);
        ColumnPtr child = buildSizeDeterminedColumn(child_field, kept, target_hint, path, list_depth + 1);
        return ColumnArray::create(child, std::move(offsets_col));
    }

    /// Children with physical data are decoded at their full declared length, which must stay within
    /// the message-body row bound even when some rows are unreferenced.
    checkRowCountWithinBody(peekNodeRows(), "list child");

    /// Rows of the child that only invisible slots reference — or that no slot references — hold
    /// undefined bytes and must not be value-validated; see `buildOffsetsChildInvisibleMask`.
    const std::optional<InvisibleRowsMask> child_invisible = buildOffsetsChildInvisibleMask(rows, base, prev, offs, invisible_rows);

    ColumnPtr child = decodeField(
        child_field, /*allow_low_cardinality=*/false, target_hint, path, list_depth + 1, maskPtr(child_invisible));
    /// A sliced Arrow list can begin at a non-zero first offset; only child[base, prev) is referenced.
    /// Slice the child to that range so its size matches the base-relative offsets — mirroring the Apache
    /// Arrow library reader's Flatten/slice of a slice.
    const size_t referenced = static_cast<size_t>(prev - base);
    ColumnPtr child_slice
        = (base == 0 && referenced == child->size()) ? child : child->cut(static_cast<size_t>(base), referenced);
    if (const auto filt = emptyInvisibleSlots(offs, child_slice->size(), invisible_rows))
        child_slice = child_slice->filter(*filt, -1);
    return ColumnArray::create(child_slice, std::move(offsets_col));
}

bool RecordBatchDecoder::isSizeDeterminedSubtree(const ArrowField & field) const
{
    size_t node_offset = 0;
    return isBufferlessSubtree(field) && !bufferlessSubtreeDeclaresNulls(field, node_offset);
}

bool RecordBatchDecoder::bufferlessSubtreeDeclaresNulls(const ArrowField & field, size_t & node_offset) const
{
    const flatbuf::FieldNode & node = peekNode(node_offset++);
    /// A `null` array reports every row as null: that is its content, not validity data.
    if (field.type.kind == TypeKind::Null)
        return false;
    if (node.null_count() != 0)
        return true;
    for (const ArrowField & child : field.type.children)
        if (bufferlessSubtreeDeclaresNulls(child, node_offset))
            return true;
    return false;
}

ColumnPtr RecordBatchDecoder::buildSizeDeterminedColumn(
    const ArrowField & field, size_t rows, const DataTypePtr & target_hint, const String & path, size_t list_depth)
{
    /// The caller has validated the node's length and that it declares no nulls; the length only fixes what
    /// the children must declare.
    const size_t declared_rows = static_cast<size_t>(nextNode().length());
    const ArrowType & type = field.type;
    if (type.kind == TypeKind::Null)
        return ColumnNullable::create(ColumnNothing::create(rows), ColumnUInt8::create(rows, UInt8{1}));

    /// Constant roots bypass the layout pass, so this traversal validates child lengths while
    /// consuming each struct or fixed-size-list node's validity slot and children.
    nextBuffer();
    const DataTypePtr effective_hint = resolveTargetHint(target_hint, path, list_depth);
    ColumnPtr inner;
    if (type.kind == TypeKind::FixedSizeList)
    {
        const size_t declared_child = fixedSizeListChildRows(type, declared_rows);
        const size_t list_size = static_cast<size_t>(type.list_size);
        expectNextNodeLength(declared_child, "fixed-size-list child");
        ColumnPtr child = buildSizeDeterminedColumn(
            type.children.at(0), fixedSizeListChildRows(type, rows), arrayElementHint(effective_hint), path, list_depth + 1);
        auto offsets_col = ColumnUInt64::create(rows);
        auto & offs = offsets_col->getData();
        for (size_t i = 0; i < rows; ++i)
            offs[i] = (i + 1) * list_size;
        inner = ColumnArray::create(child, std::move(offsets_col));
    }
    else
    {
        Columns elements;
        elements.reserve(type.children.size());
        for (size_t i = 0; i < type.children.size(); ++i)
        {
            const ArrowField & child = type.children[i];
            expectNextNodeLength(declared_rows, fmt::format("struct field '{}'", child.name));
            elements.push_back(buildSizeDeterminedColumn(
                child, rows, tupleElementHint(effective_hint, child.name, i, settings.arrow.case_insensitive_column_matching),
                childPath(path, child.name), list_depth));
        }
        inner = type.children.empty() ? ColumnTuple::create(rows) : ColumnTuple::create(elements);
    }
    /// Every row is valid: the same wrapper decision as `decodeField`, with an all-zero null map.
    if (wrapsInNullable(field, *inner, effective_hint))
        return ColumnNullable::create(inner, ColumnUInt8::create(rows, UInt8{0}));
    return inner;
}

ColumnPtr RecordBatchDecoder::decodeDictionary(
    const ArrowField & field, size_t rows, bool allow_low_cardinality, const InvisibleRowsMask * invisible_rows,
    const String & path, size_t list_depth)
{
    const Slice indices_slice = nextBuffer();
    const DictionaryRegistry::Dictionary & dictionary
        = registry.get(field.dictionary->id, FieldPosition{path, list_depth});
    const size_t dictionary_size = dictionary.size();

    const int bits = field.dictionary->index_bit_width;
    const bool index_is_signed = field.dictionary->index_is_signed;

    VectorWithMemoryTracking<FieldDictionaryValues> segments;
    segments.reserve(dictionary.segments.size());
    for (const auto & segment : dictionary.segments)
        segments.push_back(dictionaryValuesFor(field, segment));

    const DataTypePtr & value_type = segments.front().type;
    const IColumn * first_column = segments.front().column.get();
    if (const auto * constant = typeid_cast<const ColumnConst *>(first_column))
        first_column = &constant->getDataColumn();
    MutableColumnPtr keys = first_column->cloneEmpty();
    VectorWithMemoryTracking<size_t> key_starts;
    key_starts.reserve(segments.size());
    bool has_constant_segments = false;
    for (const auto & segment : segments)
    {
        key_starts.push_back(keys->size());
        const IColumn * values = segment.column.get();
        if (const auto * constant = typeid_cast<const ColumnConst *>(values))
        {
            values = &constant->getDataColumn();
            has_constant_segments = true;
        }
        keys->insertRangeFrom(*values, 0, values->size());
    }
    const size_t default_key_index = keys->size();
    if (invisible_rows)
        keys->insertDefault();

    /// Ordinary dictionaries have one segment whose indexes address the keys directly. Constant segments
    /// contribute one physical key each and require translating their logical index ranges.
    auto indexes = ColumnUInt64::create(rows);
    auto & index_data = indexes->getData();
    for (size_t row = 0; row < rows; ++row)
    {
        if (isInvisible(invisible_rows, row))
        {
            index_data[row] = default_key_index;
            continue;
        }
        /// Invisible rows have undefined index bytes. Visible signed indexes must be non-negative before
        /// they are widened and checked against the dictionary's logical size.
        Int64 index = 0;
        switch (bits)
        {
            case 8: index = index_is_signed ? Int64(reinterpret_cast<const int8_t *>(indices_slice.ptr)[row])
                                           : Int64(reinterpret_cast<const uint8_t *>(indices_slice.ptr)[row]); break;
            case 16: index = index_is_signed ? Int64(reinterpret_cast<const Int16 *>(indices_slice.ptr)[row])
                                            : Int64(reinterpret_cast<const UInt16 *>(indices_slice.ptr)[row]); break;
            case 32: index = index_is_signed ? Int64(reinterpret_cast<const Int32 *>(indices_slice.ptr)[row])
                                            : Int64(reinterpret_cast<const UInt32 *>(indices_slice.ptr)[row]); break;
            case 64: index = reinterpret_cast<const Int64 *>(indices_slice.ptr)[row]; break;
            default: UNREACHABLE();
        }
        if (index < 0 || static_cast<UInt64>(index) >= dictionary_size)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC dictionary index {} out of range (size {})", index, dictionary_size);

        const size_t segment_index = has_constant_segments
            ? static_cast<size_t>(std::upper_bound(dictionary.offsets.begin(), dictionary.offsets.end(), static_cast<size_t>(index))
                - dictionary.offsets.begin())
            : 0;
        const size_t segment_start = segment_index == 0 ? 0 : dictionary.offsets[segment_index - 1];
        const size_t index_in_segment = static_cast<size_t>(index) - segment_start;
        const FieldDictionaryValues & segment = segments[segment_index];
        if (segment.null_entries && segment.null_entries->getUInt(index_in_segment))
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Arrow IPC field '{}' is declared non-nullable but its row {} references null dictionary entry {}",
                field.name, row, index);

        index_data[row] = has_constant_segments
            ? key_starts[segment_index] + (isColumnConst(*segment.column) ? 0 : index_in_segment)
            : static_cast<size_t>(index);
    }

    /// Top-level dictionary fields use `LowCardinality` when the value type supports it. Nested fields
    /// and other value types gather the same compact keys into a plain column.
    if (allow_low_cardinality && value_type->canBeInsideLowCardinality())
    {
        auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(value_type);
        auto column = low_cardinality_type->createColumn();
        assert_cast<ColumnLowCardinality &>(*column).insertRangeFromDictionaryEncodedColumn(*keys, *indexes);
        return column;
    }
    return keys->index(*indexes, 0);
}

ColumnPtr RecordBatchDecoder::decodeUnion(const ArrowField & field, size_t rows, const InvisibleRowsMask * invisible_rows)
{
    const ArrowType & type = field.type;
    const bool dense = type.union_mode == flatbuf::UnionMode_Dense;

    /// A union has no validity buffer: a types buffer (int8), and for dense unions an offsets buffer (int32).
    const Slice type_ids_slice = nextBuffer();
    const auto * type_ids = reinterpret_cast<const int8_t *>(type_ids_slice.ptr);

    const Int32 * value_offsets = nullptr;
    if (dense)
    {
        const Slice offsets_slice = nextBuffer();
        value_offsets = reinterpret_cast<const Int32 *>(offsets_slice.ptr);
    }

    /// Decode children. Arrow `null`-typed children are the ClickHouse NULL placeholder: they carry a
    /// FieldNode but no buffers and contribute no Variant element. The rest become Variant elements.
    Columns variant_columns;
    DataTypes variant_types;
    /// Per Variant element: the child's own null map (null when it declares no nulls), kept apart from the
    /// element column because `ColumnVariant` elements cannot be Nullable and Array/Map/Tuple columns carry
    /// no null map of their own, so a selected null value can still become a `Variant` NULL below. A
    /// size-determined dense child holds only the selected values, in row order, and `next_row` walks them.
    struct ChildState
    {
        ColumnUInt8::Ptr null_map;
        bool size_determined;
        size_t next_row = 0;
    };
    VectorWithMemoryTracking<ChildState> child_states;
    /// Maps an Arrow union type id to a local Variant element index, or -1 for the NULL placeholder.
    UnorderedMapWithMemoryTracking<int, int> type_id_to_local;
    size_t total_child_rows = 0;
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

        /// A dense child may declare more rows than the union selects. A buffer-less child has no buffer
        /// whose size could bound that count, so it is built from the selected rows alone, in row order.
        /// Any other child's declared length is backed by a buffer the subtree validation of
        /// `decodeBatchColumn` has already checked, so it keeps its declared layout under a visibility
        /// mask: a slot holds a meaningful value only when a visible row's type id selects it — other
        /// slots hold undefined bytes per the Arrow spec, even in a file without nulls — so value decoding
        /// must skip them.
        const size_t child_rows = peekNodeRows();
        const bool size_determined = dense && isSizeDeterminedSubtree(child);
        InvisibleRowsMask child_invisible;
        if (!size_determined)
            child_invisible.resize_fill(child_rows, 1);
        size_t selected_rows = 0;
        for (size_t row = 0; row < rows; ++row)
        {
            if (type_ids[row] != tid || isInvisible(invisible_rows, row))
                continue;

            const Int64 target = dense ? Int64(value_offsets[row]) : Int64(row);
            if (dense && (target < 0 || static_cast<size_t>(target) >= child_rows))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow dense union offset {} out of range", target);
            ++selected_rows;
            if (!size_determined)
                child_invisible[static_cast<size_t>(target)] = 0;
        }

        ColumnUInt8::Ptr child_null_map;
        ColumnPtr child_column = size_determined
            ? buildSizeDeterminedColumn(child, selected_rows, /*target_hint=*/nullptr, /*path=*/{}, /*list_depth=*/0)
            : decodeField(
                child, /*allow_low_cardinality=*/false, /*target_hint=*/nullptr, /*path=*/{}, /*list_depth=*/0,
                &child_invisible, &child_null_map);
        DataTypePtr child_type = fieldToCHType(child, settings, /*make_nullable=*/false, /*allow_null_type=*/true);
        /// The decoded null map survives even when a complex child cannot retain its outer wrapper.
        /// A nullable dictionary result also includes nulls contributed by dictionary entries.
        if (child_column->isNullable())
        {
            const auto & nullable = assert_cast<const ColumnNullable &>(*child_column);
            child_null_map = nullable.getNullMapColumn().getPtr();
            child_column = nullable.getNestedColumnPtr();
        }
        type_id_to_local[tid] = static_cast<int>(variant_columns.size());
        total_child_rows += child_column->size();
        variant_columns.push_back(std::move(child_column));
        variant_types.push_back(removeNullable(child_type));
        child_states.push_back({std::move(child_null_map), size_determined});
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

    /// `ColumnVariant` requires each element column to hold exactly the values of the rows selecting it.
    /// Sparse children hold a value for every union row, dense children may retain unselected values after
    /// slicing, and a selected null value becomes a `Variant` NULL that leaves the value unreferenced. In
    /// all these cases the selected values are gathered into compact element columns.
    const bool compact_children = !dense || invisible_rows || total_child_rows > rows
        || std::ranges::any_of(child_states, [](const ChildState & state) { return state.null_map != nullptr; });
    MutableColumns compact;
    if (compact_children)
    {
        compact.reserve(variant_columns.size());
        for (const auto & col : variant_columns)
            compact.push_back(col->cloneEmpty());
    }

    /// The row shape shared by every NULL outcome: invisible row, NULL-placeholder child, null child value.
    auto emit_null_row = [&](size_t row)
    {
        discr_data[row] = ColumnVariant::NULL_DISCRIMINATOR;
        off_data[row] = 0;
    };

    for (size_t row = 0; row < rows; ++row)
    {
        /// The type-id (and, for dense unions, offset) bytes of a row that is invisible at an ancestor
        /// level are undefined per the Arrow spec; represent the row as a Variant NULL without
        /// interpreting them.
        if (isInvisible(invisible_rows, row))
        {
            emit_null_row(row);
            continue;
        }
        auto local_it = type_id_to_local.find(type_ids[row]);
        if (local_it == type_id_to_local.end())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow union row references unknown type id {}", type_ids[row]);
        const int local = local_it->second;
        if (local < 0)
        {
            emit_null_row(row);
            continue;
        }
        /// The child row this row's value lives in: the next selected value of a size-determined child,
        /// the offsets-referenced row of another dense child, the row itself in the sparse layout.
        ChildState & child_state = child_states[local];
        const size_t src = child_state.size_determined
            ? child_state.next_row++
            : (dense ? static_cast<size_t>(value_offsets[row]) : row);
        /// A referenced null value in a nullable child becomes a `Variant` NULL.
        if (child_state.null_map && child_state.null_map->getData()[src])
        {
            emit_null_row(row);
            continue;
        }
        discr_data[row] = static_cast<ColumnVariant::Discriminator>(local);
        if (compact_children)
        {
            off_data[row] = static_cast<ColumnVariant::Offset>(compact[local]->size());
            compact[local]->insertFrom(*variant_columns[local], src);
        }
        else
            off_data[row] = static_cast<ColumnVariant::Offset>(src);
    }

    if (compact_children)
        return ColumnVariant::create(
            std::move(local_discriminators), std::move(offsets), std::move(compact), local_to_global);
    return ColumnVariant::create(
        std::move(local_discriminators), std::move(offsets), Columns(variant_columns), local_to_global);
}

ColumnPtr RecordBatchDecoder::decodeField(
    const ArrowField & field, bool allow_low_cardinality, const DataTypePtr & target_hint, const String & path,
    size_t list_depth, const InvisibleRowsMask * invisible_rows, ColumnUInt8::Ptr * decoded_null_map)
{
    if (decoded_null_map)
        *decoded_null_map = nullptr;

    const flatbuf::FieldNode & node = nextNode();
    const size_t rows = static_cast<size_t>(node.length());

    /// Dictionary fields store index validity and indices here; their value layout is in `DictionaryBatch`.
    /// The value-type branches apply only to fields whose values are inline.
    const bool is_dictionary = field.dictionary.has_value();

    /// Unions have no validity buffer; handle them before consuming one.
    if (!is_dictionary && field.type.kind == TypeKind::Union)
    {
        /// Union nulls use an explicit `null` child, so the `FieldNode` must declare zero nulls.
        /// Inherited invisible rows still propagate through the selected children in `decodeUnion`.
        if (node.null_count() != 0)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Arrow IPC Union field '{}' has no validity bitmap but its FieldNode reports {} nulls",
                field.name, node.null_count());
        return decodeUnion(field, rows, invisible_rows);
    }

    /// An Arrow `null` field has no buffers and maps to an all-null `Nullable(Nothing)` column.
    if (!is_dictionary && field.type.kind == TypeKind::Null)
    {
        ColumnUInt8::Ptr null_map = ColumnUInt8::create(rows, UInt8{1});
        if (decoded_null_map)
            *decoded_null_map = null_map;
        return ColumnNullable::create(ColumnNothing::create(rows), null_map);
    }

    /// Every nullable-capable node carries a validity buffer slot first, then its value buffers.
    const Slice validity = nextBuffer();

    /// A non-nullable field must declare a zero null count.
    if (!field.nullable && node.null_count() != 0)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Arrow IPC field '{}' is declared non-nullable but its FieldNode reports {} nulls",
            field.name, node.null_count());

    /// Null rows have undefined value bytes and join the inherited invisible rows before value decoding.
    ColumnUInt8::Ptr own_null_map;
    InvisibleRowsMask composed_invisible;
    const InvisibleRowsMask * effective_invisible = invisible_rows;
    if (node.null_count() != 0)
    {
        own_null_map = buildNullMap(validity, rows, node.null_count());
        effective_invisible = unionNullMaps(own_null_map->getData(), invisible_rows, composed_invisible);
        if (decoded_null_map)
            *decoded_null_map = own_null_map;
    }

    /// Dictionary values come from separate batches; `effective_invisible` controls index visibility.
    if (field.dictionary)
        return decodeDictionary(field, rows, allow_low_cardinality, effective_invisible, path, list_depth);

    ColumnPtr inner = decodeInner(field, rows, target_hint, path, list_depth, effective_invisible);
    if (wrapsInNullable(field, *inner, resolveTargetHint(target_hint, path, list_depth)))
    {
        ColumnUInt8::Ptr null_map = own_null_map ? own_null_map : buildNullMap(validity, rows, node.null_count());
        return ColumnNullable::create(inner, null_map);
    }
    return inner;
}

bool RecordBatchDecoder::wrapsInNullable(const ArrowField & field, const IColumn & inner, const DataTypePtr & effective_hint) const
{
    const bool preserve_struct_nulls = settings.schema_inference_allow_nullable_tuple_type
        || (effective_hint && (effective_hint->isNullable() || effective_hint->isLowCardinalityNullable()
            || isTuple(stripHint(effective_hint))));
    return field.nullable && inner.canBeInsideNullable()
        && (field.type.kind != TypeKind::Struct || preserve_struct_nulls);
}

void RecordBatchDecoder::advanceField(const ArrowField & field, bool validate_lengths)
{
    const auto & node = nextNode();
    if (validate_lengths && node.length() < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC field node has a negative length {}", node.length());
    const size_t rows = validate_lengths ? static_cast<size_t>(node.length()) : 0;

    auto consume_buffer = [&](size_t count, size_t width, const char * what)
    {
        const Slice buffer = nextBuffer();
        if (validate_lengths)
            checkBufferSize(buffer, requiredBytes(count, width), what);
    };
    auto consume_validity = [&]
    {
        const Slice validity = nextBuffer();
        if (validate_lengths && node.null_count() != 0)
        {
            if (validity.length == 0)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Arrow IPC field declares a null count of {} but omits the validity bitmap", node.null_count());
            checkBufferSize(validity, (rows + 7) / 8, "validity");
        }
    };

    /// Dictionary fields contain indices here; their value layout belongs to a separate batch.
    if (field.dictionary)
    {
        consume_validity();
        const size_t width = validate_lengths ? dictionaryIndexByteWidth(field.dictionary->index_bit_width) : 0;
        consume_buffer(rows, width, "dictionary indices");
        return;
    }

    const auto & type = field.type;
    if (type.kind == TypeKind::Union)
    {
        const bool dense = type.union_mode == flatbuf::UnionMode_Dense;
        consume_buffer(rows, 1, "union type ids");
        if (dense)
            consume_buffer(rows, sizeof(Int32), "union offsets");
        for (const ArrowField & child : type.children)
        {
            if (child.type.kind == TypeKind::Null)
                nextNode();
            else
            {
                if (validate_lengths && !dense)
                    expectNextNodeLength(rows, fmt::format("sparse union child '{}'", child.name));
                advanceField(child, validate_lengths);
            }
        }
        return;
    }

    /// `Null` and `RunEndEncoded` fields have no validity buffer of their own.
    if (type.kind == TypeKind::Null)
        return;
    if (type.kind == TypeKind::Unsupported && type.skip_layout == ArrowType::SkipLayout::RunEndEncoded)
    {
        for (const ArrowField & child : type.children)
            advanceField(child, validate_lengths);
        return;
    }

    consume_validity();

    switch (type.kind)
    {
        case TypeKind::Int:
            consume_buffer(rows, type.bit_width / 8, "values");
            break;
        case TypeKind::FloatingPoint:
        {
            const size_t width = type.float_precision == flatbuf::Precision_DOUBLE ? sizeof(Float64)
                : type.float_precision == flatbuf::Precision_SINGLE ? sizeof(Float32) : sizeof(UInt16);
            consume_buffer(rows, width, "values");
            break;
        }
        case TypeKind::Bool:
            consume_buffer((rows + 7) / 8, 1, "bool");
            break;
        case TypeKind::Decimal:
            consume_buffer(rows, type.decimal_bit_width / 8, "values");
            break;
        case TypeKind::Date:
            consume_buffer(rows, type.unit == flatbuf::DateUnit_DAY ? sizeof(Int32) : sizeof(Int64), "values");
            break;
        case TypeKind::Time:
            consume_buffer(rows, type.time_bit_width / 8, "values");
            break;
        case TypeKind::Timestamp:
        case TypeKind::Duration:
            consume_buffer(rows, sizeof(Int64), "values");
            break;
        case TypeKind::Interval:
        {
            const size_t width = type.unit == flatbuf::IntervalUnit_YEAR_MONTH ? 4
                : type.unit == flatbuf::IntervalUnit_DAY_TIME ? 8 : 16;
            consume_buffer(rows, width, "interval");
            break;
        }
        case TypeKind::FixedSizeBinary:
            consume_buffer(rows, type.byte_width, "fixed_size_binary");
            break;
        case TypeKind::Utf8:
        case TypeKind::LargeUtf8:
        case TypeKind::Binary:
        case TypeKind::LargeBinary:
        {
            const bool large = type.kind == TypeKind::LargeUtf8 || type.kind == TypeKind::LargeBinary;
            consume_buffer(rows ? rows + 1 : 0, large ? sizeof(Int64) : sizeof(Int32), "offsets");
            nextBuffer();
            break;
        }
        case TypeKind::BinaryView:
        case TypeKind::Utf8View:
        {
            consume_buffer(rows, 16, "binary view");
            const Int64 num_data = variadic_index < variadic_counts.size() ? variadic_counts[variadic_index] : 0;
            ++variadic_index;
            if (validate_lengths && static_cast<size_t>(num_data) > buffer_slices.size() - buffer_index)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Arrow IPC binary view column declares {} data buffers but only {} remain",
                    num_data, buffer_slices.size() - buffer_index);
            for (Int64 i = 0; i < num_data; ++i)
                nextBuffer();
            break;
        }
        case TypeKind::List:
        case TypeKind::LargeList:
        case TypeKind::Map:
            consume_buffer(rows ? rows + 1 : 0, type.kind == TypeKind::LargeList ? sizeof(Int64) : sizeof(Int32), "offsets");
            advanceField(type.children.at(0), validate_lengths);
            break;
        case TypeKind::FixedSizeList:
            if (validate_lengths)
                expectNextNodeLength(fixedSizeListChildRows(type, rows), "fixed-size-list child");
            advanceField(type.children.at(0), validate_lengths);
            break;
        case TypeKind::Struct:
            for (const ArrowField & child : type.children)
            {
                if (validate_lengths)
                    expectNextNodeLength(rows, fmt::format("struct field '{}'", child.name));
                advanceField(child, validate_lengths);
            }
            break;
        case TypeKind::Null:
        case TypeKind::Union:
            break; /// These layouts are handled before the validity buffer.
        case TypeKind::Unsupported:
            /// List views can be skipped when unrequested; their values are not supported by the decoder.
            if (type.skip_layout == ArrowType::SkipLayout::ListView)
            {
                nextBuffer();
                nextBuffer();
                advanceField(type.children.at(0), validate_lengths);
                break;
            }
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Native Arrow IPC reader cannot skip the unsupported Arrow type {} of field '{}'",
                type.unsupported_type_name, field.name);
    }
}

RecordBatchDecoder::DecodedColumns RecordBatchDecoder::decodeBatch(
    const flatbuf::RecordBatch & batch, const PODArray<char> & body,
    const UnorderedSetWithMemoryTracking<String> * keep_top_level_fields,
    const UnorderedMapWithMemoryTracking<String, DataTypePtr> * target_types_,
    const VectorWithMemoryTracking<char> * reachable_buffers)
{
    beginBatch(batch, body, reachable_buffers, target_types_);

    DecodedColumns result;
    result.reserve(schema.fields.size());
    for (const ArrowField & field : schema.fields)
    {
        expectNextNodeLength(static_cast<size_t>(batch.length()), fmt::format("column '{}'", field.name));
        const String normalized_name = normalizedName(field.name);
        if (keep_top_level_fields && !keep_top_level_fields->contains(normalized_name))
        {
            /// Skip unrequested values after validating the column's root row count.
            advanceField(field);
            continue;
        }
        /// `normalized_name` seeds the requested-type lookup in `target_types`.
        /// Nested fields derive their hints through `decodeField` and `decodeInner`.
        result.push_back(decodeBatchColumn(field, /*target_hint=*/nullptr, normalized_name, /*list_depth=*/0));
    }

    finishBatch();
    return result;
}

DictionaryRegistry::Values RecordBatchDecoder::decodeDictionaryValues(
    const flatbuf::RecordBatch & batch, const PODArray<char> & body, const ArrowField & value_field, const DictionaryUse & use,
    const UnorderedMapWithMemoryTracking<String, DataTypePtr> * target_types_)
{
    beginBatch(batch, body, /*reachable_buffers=*/nullptr, target_types_);
    expectNextNodeLength(static_cast<size_t>(batch.length()), fmt::format("column '{}'", value_field.name));
    ColumnUInt8::Ptr null_map;
    DecodedColumn values = decodeBatchColumn(value_field, use.hint, use.position.path, use.position.list_depth, &null_map);
    finishBatch();
    ColumnPtr dictionary_null_map = std::move(null_map);
    if (dictionary_null_map && isColumnConst(*values.column))
        dictionary_null_map = ColumnConst::create(dictionary_null_map, values.column->size());
    return {std::move(values.column), std::move(values.type), std::move(dictionary_null_map)};
}

void RecordBatchDecoder::beginBatch(
    const flatbuf::RecordBatch & batch, const PODArray<char> & body,
    const VectorWithMemoryTracking<char> * reachable_buffers,
    const UnorderedMapWithMemoryTracking<String, DataTypePtr> * target_types_)
{
    if (batch.length() < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC record batch has a negative length {}", batch.length());

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

    prepareBuffers(batch, body, reachable_buffers);
}

RecordBatchDecoder::DecodedColumn RecordBatchDecoder::decodeBatchColumn(
    const ArrowField & field, const DataTypePtr & target_hint, const String & path, size_t list_depth,
    ColumnUInt8::Ptr * decoded_null_map)
{
    const size_t batch_rows = static_cast<size_t>(current_batch->length());

    DecodedColumn decoded;
    decoded.name = field.name;
    /// Dictionary values retain the type produced by their requested hints. The referencing field
    /// determines whether that type keeps its outer `Nullable` wrapper.
    if (field.dictionary)
    {
        const auto & dictionary = registry.get(field.dictionary->id, FieldPosition{path, list_depth});
        const DataTypePtr & value_type = dictionary.segments.front().type;
        decoded.type = field.nullable ? value_type : removeNullable(value_type);
        if (decoded.type->canBeInsideLowCardinality())
            decoded.type = std::make_shared<DataTypeLowCardinality>(decoded.type);
    }
    else
        decoded.type = fieldToCHType(field, settings, field.nullable, /*allow_null_type=*/true);
    /// Bufferless roots store one physical value. The reader chooses the output row count before
    /// expanding it, and dictionaries retain its compact representation across deltas.
    const bool constant = batch_rows != 0 && isSizeDeterminedSubtree(field);
    if (constant)
    {
        decoded.column = buildSizeDeterminedColumn(field, 1, target_hint, path, list_depth);
        if (decoded_null_map && field.type.kind == TypeKind::Null)
            *decoded_null_map = assert_cast<const ColumnNullable &>(*decoded.column).getNullMapColumn().getPtr();
    }
    else
    {
        checkRowCountWithinBody(batch_rows, fmt::format("column '{}'", field.name));
        /// Validate the complete subtree before allocating columns or visibility masks. A buffered
        /// descendant must justify its row count before any preceding bufferless sibling is materialized.
        const size_t first_node = node_index;
        const size_t first_buffer = buffer_index;
        const size_t first_variadic = variadic_index;
        advanceField(field, /*validate_lengths=*/true);
        node_index = first_node;
        buffer_index = first_buffer;
        variadic_index = first_variadic;
        decoded.column = decodeField(
            field,
            /*allow_low_cardinality=*/true,
            target_hint,
            path,
            list_depth,
            /*invisible_rows=*/nullptr,
            decoded_null_map);
    }
    /// Struct null maps survive decoding even when the inferred type has no nullable wrapper.
    decoded.type = matchColumnNullability(decoded.type, decoded.column);
    if (constant)
        decoded.column = ColumnConst::create(decoded.column, batch_rows);
    return decoded;
}

void RecordBatchDecoder::finishBatch()
{
    /// Decoded and skipped fields must consume every declared node, buffer, and variadic count.
    /// Surplus entries indicate that the batch layout does not match the schema.
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
}

String RecordBatchDecoder::normalizedName(const String & name) const
{
    String normalized = name;
    if (settings.arrow.case_insensitive_column_matching)
        boost::to_lower(normalized);
    return normalized;
}

String RecordBatchDecoder::childPath(const String & path, const String & child_name) const
{
    const String seg = normalizedName(child_name);
    return path.empty() ? seg : path + "." + seg;
}

void RecordBatchDecoder::prepareBuffers(const flatbuf::RecordBatch & batch, const PODArray<char> & body, const VectorWithMemoryTracking<char> * reachable)
{
    buffer_slices.clear();
    total_buffer_bytes = 0;
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
            /// without validating it or pointing at its absolute offset; `advanceField` consumes it unread.
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
            total_buffer_bytes += static_cast<size_t>(buffer->length());
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

    /// A decompressed body this large cannot be allocated on any real machine, and `PODArray::resize`
    /// rounds its request up to a power of two, so stay an octave below the allocator's own ceiling
    /// rather than restating its arithmetic here. The per-buffer frame bound below caps every
    /// `out_len` far short of this, so keep it as the backstop on the running total.
    static constexpr size_t MAX_DECOMPRESSED_BODY_SIZE = 1ULL << 62;
    auto check_body_size = [](size_t n)
    {
        if (n >= MAX_DECOMPRESSED_BODY_SIZE)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC decompressed body size {} is too large to allocate", n);
    };

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
        /// Check the aligned running total here, before the zero-length `continue` below can skip it.
        check_body_size(pos);
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

        /// Check this prefix against the payload before allocating for it. A size the codec pledges is
        /// a second copy of the prefix, so any difference means the prefix is forged; otherwise the
        /// payload's structure only bounds what it can produce, so only exceeding it is forged.
        if (uncompressed_length >= 0 && length > 8)
        {
            const auto bound = frameContentBound(codec, src + 8, static_cast<size_t>(length - 8));
            if (bound.exact ? out_len != bound.size : out_len > bound.size)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Arrow IPC compressed buffer declares {} uncompressed bytes but its {}-byte codec frame "
                    "declares {}", out_len, length - 8, bound.size);
        }

        if (out_len > std::numeric_limits<size_t>::max() - pos)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC decompressed body size overflows");
        placements[i] = {pos, out_len, src + 8, static_cast<size_t>(length - 8), uncompressed_length < 0};
        pos += out_len;
        check_body_size(pos);
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
    {
        buffer_slices.push_back(Slice{decompressed_body.data() + p.offset, static_cast<Int64>(p.length)});
        total_buffer_bytes += p.length;
    }
}

VectorWithMemoryTracking<char> RecordBatchDecoder::reachableTopLevelBuffers(
    const flatbuf::RecordBatch & batch, const UnorderedSetWithMemoryTracking<String> * keep_top_level_fields)
{
    const size_t num_buffers = batch.buffers() ? batch.buffers()->size() : 0;
    /// No pruning requested: every buffer is reachable (the caller reads the whole body).
    if (!keep_top_level_fields)
        return VectorWithMemoryTracking<char>(num_buffers, 1);

    VectorWithMemoryTracking<char> reachable(num_buffers, 0);

    /// Walk the schema with `advanceField` so buffer spans match decoding. Counts come from field types
    /// and variadic counts; the body is not read. Placeholder slices let `nextBuffer` advance the cursor.
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

        for (const ArrowField & field : schema.fields)
        {
            const size_t start = buffer_index;
            advanceField(field);
            const size_t end = buffer_index;
            if (keep_top_level_fields->contains(normalizedName(field.name)))
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
    /// `advanceField` pops slices via `nextBuffer`; give it placeholders so the cursor can advance. A batch
    /// declaring fewer buffers than the field needs makes `nextBuffer` throw here, before any materialization.
    buffer_slices.assign(total_buffers, Slice{});

    for (const ArrowField & field : fields)
        advanceField(field);

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

DataTypePtr RecordBatchDecoder::resolveTargetHint(const DataTypePtr & parent_hint, const String & path, size_t list_depth) const
{
    return resolveHint(parent_hint, path, list_depth, target_types);
}

UnorderedMapWithMemoryTracking<Int64, DictionaryUses> RecordBatchDecoder::collectDictionaryUses(
    const UnorderedSetWithMemoryTracking<String> * keep_top_level_fields,
    const UnorderedMapWithMemoryTracking<String, DataTypePtr> * target_types_) const
{
    UnorderedMapWithMemoryTracking<Int64, DictionaryUses> uses;
    for (const ArrowField & field : schema.fields)
    {
        /// The same kept-field test and starting position as `decodeBatch`.
        const String normalized_name = normalizedName(field.name);
        if (keep_top_level_fields && !keep_top_level_fields->contains(normalized_name))
            continue;
        collectDictionaryUses(field, /*target_hint=*/nullptr, normalized_name, /*list_depth=*/0, target_types_, uses);
    }
    return uses;
}

void RecordBatchDecoder::collectDictionaryUses(
    const ArrowField & field, const DataTypePtr & target_hint, const String & path, size_t list_depth,
    const UnorderedMapWithMemoryTracking<String, DataTypePtr> * lookup_types,
    UnorderedMapWithMemoryTracking<Int64, DictionaryUses> & uses) const
{
    const DataTypePtr effective_hint = resolveHint(target_hint, path, list_depth, lookup_types);
    if (field.dictionary)
    {
        /// Two fields share a position only below unions, which drop the path; they then resolve the same
        /// hint too, so one decoding serves both.
        const FieldPosition position{path, list_depth};
        DictionaryUses & id_uses = uses[field.dictionary->id];
        if (std::ranges::none_of(id_uses, [&](const DictionaryUse & use) { return use.position == position; }))
            id_uses.push_back(DictionaryUse{position, effective_hint});
    }

    const ArrowType & type = field.type;
    switch (type.kind)
    {
        case TypeKind::List:
        case TypeKind::LargeList:
        case TypeKind::FixedSizeList:
            collectDictionaryUses(
                type.children.at(0), arrayElementHint(effective_hint), path, list_depth + 1, lookup_types, uses);
            break;
        case TypeKind::Map:
            collectDictionaryUses(
                type.children.at(0), mapEntriesHint(effective_hint), path, list_depth + 1, lookup_types, uses);
            break;
        case TypeKind::Struct:
            for (size_t i = 0; i < type.children.size(); ++i)
            {
                const ArrowField & child = type.children[i];
                collectDictionaryUses(
                    child, tupleElementHint(effective_hint, child.name, i, settings.arrow.case_insensitive_column_matching),
                    childPath(path, child.name), list_depth, lookup_types, uses);
            }
            break;
        case TypeKind::Union:
            /// `decodeUnion` decodes its children with no hint, path or list depth.
            for (const ArrowField & child : type.children)
                collectDictionaryUses(child, /*target_hint=*/nullptr, /*path=*/{}, /*list_depth=*/0, lookup_types, uses);
            break;
        default:
            break;
    }
}

}

#endif
