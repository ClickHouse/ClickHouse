#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>

#if USE_ARROW

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeFactory.h>
#include <Common/DateLUTImpl.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/NetUtils.h>
#include <Common/PODArray.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wimplicit-int-conversion"
#include <cctz/time_zone.h>
#pragma clang diagnostic pop

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
    extern const int UNKNOWN_TYPE;
}
}

namespace DB::ArrowIPC
{

namespace
{

KeyValueMetadata parseMetadata(const flatbuffers::Vector<flatbuffers::Offset<flatbuf::KeyValue>> * kv)
{
    KeyValueMetadata result;
    if (!kv)
        return result;
    for (const auto * entry : *kv)
    {
        if (entry && entry->key())
            result[entry->key()->str()] = entry->value() ? entry->value()->str() : std::string{};
    }
    return result;
}

ArrowType parseType(const flatbuf::Field & field);

/// List-like and Map types carry exactly one child in a well-formed Arrow schema. FlatBuffers verification
/// proves the buffer shape but not this semantic child count, so reject a missing or extra child here as
/// INCORRECT_DATA rather than letting a later `type.children.at(0)` throw std::out_of_range.
void requireSingleChild(const ArrowType & type, const char * what)
{
    if (type.children.size() != 1)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Arrow IPC {} type must have exactly one child, got {}", what, type.children.size());
}

ArrowFields parseChildren(const flatbuffers::Vector<flatbuffers::Offset<flatbuf::Field>> * children)
{
    ArrowFields result;
    if (!children)
        return result;
    result.reserve(children->size());
    for (const auto * child : *children)
    {
        if (!child)
            continue;
        ArrowField f;
        f.name = child->name() ? child->name()->str() : std::string{};
        f.nullable = child->nullable();
        f.type = parseType(*child);
        if (const auto * dict = child->dictionary())
        {
            DictionaryEncoding enc;
            enc.id = dict->id();
            if (const auto * index_type = dict->indexType())
            {
                enc.index_bit_width = index_type->bitWidth();
                enc.index_is_signed = index_type->is_signed();
            }
            f.dictionary = enc;
        }
        f.custom_metadata = parseMetadata(child->custom_metadata());
        result.push_back(std::move(f));
    }
    return result;
}

/// Reject an out-of-range Arrow enum value from the (FlatBuffers-verified-shape-only) schema metadata.
/// Without this, an unknown enum would be silently coerced into a fabricated type by the fallbacks in
/// `fieldToCHType`/the decoder (e.g. an unknown `Precision` decoding as half-float) instead of failing.
template <typename Enum>
Enum validatedEnum(Enum value, Enum min_value, Enum max_value, const char * what)
{
    if (value < min_value || value > max_value)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC schema has an invalid {} enum value {}", what, static_cast<int>(value));
    return value;
}

ArrowType parseType(const flatbuf::Field & field)
{
    /// FlatBuffers verification only proves that a union value, *if present*, is a well-formed table; the
    /// `Field.type` union is not `(required)`, so a schema whose `type_type` discriminant is set while the
    /// type value offset is absent passes verification, and the typed `type_as_X()` accessors below return
    /// null. Reject it here so a single guard covers all those call sites instead of each one dereferencing
    /// null. A `Type_NONE` discriminant (no value) falls through to the `Unsupported` placeholder as before.
    if (field.type_type() != flatbuf::Type_NONE && field.type() == nullptr)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC schema field type is set but its value is missing");

    ArrowType type;
    switch (field.type_type())
    {
        case flatbuf::Type_Null:
            type.kind = TypeKind::Null;
            break;
        case flatbuf::Type_Int:
        {
            const auto * t = field.type_as_Int();
            type.kind = TypeKind::Int;
            type.bit_width = t->bitWidth();
            type.is_signed = t->is_signed();
            break;
        }
        case flatbuf::Type_FloatingPoint:
        {
            type.kind = TypeKind::FloatingPoint;
            type.float_precision = validatedEnum(
                field.type_as_FloatingPoint()->precision(), flatbuf::Precision_MIN, flatbuf::Precision_MAX, "Precision");
            break;
        }
        case flatbuf::Type_Bool:
            type.kind = TypeKind::Bool;
            break;
        case flatbuf::Type_Decimal:
        {
            const auto * t = field.type_as_Decimal();
            type.kind = TypeKind::Decimal;
            type.precision = t->precision();
            type.scale = t->scale();
            type.decimal_bit_width = t->bitWidth();
            /// Arrow decimals are 32-, 64-, 128- or 256-bit. The decoder uses `decimal_bit_width / 8` as
            /// the physical stride, so reject any other width here rather than fabricating a layout from it.
            if (type.decimal_bit_width != 32 && type.decimal_bit_width != 64 && type.decimal_bit_width != 128
                && type.decimal_bit_width != 256)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA, "Arrow IPC decimal has an unsupported bit width {}", type.decimal_bit_width);
            break;
        }
        case flatbuf::Type_Date:
            type.kind = TypeKind::Date;
            type.unit = validatedEnum(field.type_as_Date()->unit(), flatbuf::DateUnit_MIN, flatbuf::DateUnit_MAX, "DateUnit");
            break;
        case flatbuf::Type_Time:
        {
            const auto * t = field.type_as_Time();
            type.kind = TypeKind::Time;
            type.unit = validatedEnum(t->unit(), flatbuf::TimeUnit_MIN, flatbuf::TimeUnit_MAX, "TimeUnit");
            type.time_bit_width = t->bitWidth();
            /// Arrow `Time` allows only `time32` (SECOND/MILLISECOND, 32-bit) and `time64`
            /// (MICROSECOND/NANOSECOND, 64-bit). Reject any other unit/bit-width pair here, otherwise the
            /// decoder would read e.g. `unit = SECOND, bitWidth = 64` as an 8-byte value from a 4-byte type.
            const bool valid_time
                = (type.time_bit_width == 32 && (type.unit == flatbuf::TimeUnit_SECOND || type.unit == flatbuf::TimeUnit_MILLISECOND))
                || (type.time_bit_width == 64 && (type.unit == flatbuf::TimeUnit_MICROSECOND || type.unit == flatbuf::TimeUnit_NANOSECOND));
            if (!valid_time)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Arrow IPC Time has an invalid unit/bit-width combination (unit {}, bitWidth {})",
                    type.unit, type.time_bit_width);
            break;
        }
        case flatbuf::Type_Timestamp:
        {
            const auto * t = field.type_as_Timestamp();
            type.kind = TypeKind::Timestamp;
            type.unit = validatedEnum(t->unit(), flatbuf::TimeUnit_MIN, flatbuf::TimeUnit_MAX, "TimeUnit");
            if (t->timezone())
                type.timezone = t->timezone()->str();
            break;
        }
        case flatbuf::Type_Duration:
            type.kind = TypeKind::Duration;
            type.unit = validatedEnum(field.type_as_Duration()->unit(), flatbuf::TimeUnit_MIN, flatbuf::TimeUnit_MAX, "TimeUnit");
            break;
        case flatbuf::Type_Interval:
            type.kind = TypeKind::Interval;
            type.unit = validatedEnum(
                field.type_as_Interval()->unit(), flatbuf::IntervalUnit_MIN, flatbuf::IntervalUnit_MAX, "IntervalUnit");
            break;
        case flatbuf::Type_Utf8:
            type.kind = TypeKind::Utf8;
            break;
        case flatbuf::Type_LargeUtf8:
            type.kind = TypeKind::LargeUtf8;
            break;
        case flatbuf::Type_Binary:
            type.kind = TypeKind::Binary;
            break;
        case flatbuf::Type_LargeBinary:
            type.kind = TypeKind::LargeBinary;
            break;
        case flatbuf::Type_BinaryView:
            type.kind = TypeKind::BinaryView;
            break;
        case flatbuf::Type_Utf8View:
            type.kind = TypeKind::Utf8View;
            break;
        case flatbuf::Type_FixedSizeBinary:
            type.kind = TypeKind::FixedSizeBinary;
            type.byte_width = field.type_as_FixedSizeBinary()->byteWidth();
            break;
        case flatbuf::Type_List:
            type.kind = TypeKind::List;
            type.children = parseChildren(field.children());
            requireSingleChild(type, "List");
            break;
        case flatbuf::Type_LargeList:
            type.kind = TypeKind::LargeList;
            type.children = parseChildren(field.children());
            requireSingleChild(type, "LargeList");
            break;
        case flatbuf::Type_FixedSizeList:
            type.kind = TypeKind::FixedSizeList;
            type.list_size = field.type_as_FixedSizeList()->listSize();
            type.children = parseChildren(field.children());
            requireSingleChild(type, "FixedSizeList");
            break;
        case flatbuf::Type_Struct_:
            type.kind = TypeKind::Struct;
            type.children = parseChildren(field.children());
            break;
        case flatbuf::Type_Map:
            type.kind = TypeKind::Map;
            type.children = parseChildren(field.children());
            /// FlatBuffers verification proves the buffer shape but not these Arrow semantic child counts.
            /// A Map is List<Struct<key, value>>: exactly one "entries" struct child holding key and value.
            /// Validate it here so a malformed schema is rejected as INCORRECT_DATA instead of throwing
            /// std::out_of_range from a later `.at()` during schema inference or decoding.
            requireSingleChild(type, "Map");
            if (type.children[0].type.kind != TypeKind::Struct || type.children[0].type.children.size() != 2)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Arrow IPC Map entries must be a struct of (key, value), got {} children",
                    type.children[0].type.children.size());
            /// Arrow requires the `entries` struct itself to be non-nullable. A nullable entries child would
            /// make the decoder return `Nullable(Tuple(...))` from `decodeField(type.children.at(0))` (even
            /// with `null_count == 0`), which `decodeInner`'s Map branch then `assert_cast`s to `ColumnTuple`:
            /// a bad-cast exception in debug/sanitizer builds and a wrong static_cast in release. Reject it.
            if (type.children[0].nullable)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC Map entries struct must not be nullable");
            /// Arrow requires Map keys to be non-null, and the ClickHouse `Map` type forces a non-nullable
            /// key too. A nullable key would otherwise build a `ColumnMap` whose key column (wrapped in
            /// `Nullable` by the decoder) does not match its declared non-nullable key type.
            if (type.children[0].type.children[0].nullable)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC Map key must not be nullable");
            break;
        case flatbuf::Type_Union:
        {
            const auto * t = field.type_as_Union();
            type.kind = TypeKind::Union;
            type.union_mode = validatedEnum(t->mode(), flatbuf::UnionMode_MIN, flatbuf::UnionMode_MAX, "UnionMode");
            type.children = parseChildren(field.children());
            /// `typeIds` is semantic schema metadata that must stay one-to-one with the children: each
            /// entry maps a child to the type id carried in the union's Int8 type-ids buffer. FlatBuffers
            /// verification proves the buffer shape but not these Arrow semantics, so validate a present
            /// vector here. Otherwise `decodeUnion` stores the ids in a map where a duplicate would
            /// silently overwrite an earlier child (decoding rows as the wrong child), and an id outside
            /// the Int8 range could never match the type-ids buffer.
            if (const auto * type_ids = t->typeIds())
            {
                if (static_cast<size_t>(type_ids->size()) != type.children.size())
                    throw Exception(
                        ErrorCodes::INCORRECT_DATA,
                        "Arrow IPC Union has {} typeIds but {} children",
                        type_ids->size(), type.children.size());
                for (int id : *type_ids)
                {
                    if (id < -128 || id > 127)
                        throw Exception(
                            ErrorCodes::INCORRECT_DATA, "Arrow IPC Union type id {} does not fit the Int8 type-ids buffer", id);
                    for (int existing : type.union_type_ids)
                    {
                        if (existing == id)
                            throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC Union has duplicate type id {}", id);
                    }
                    type.union_type_ids.push_back(id);
                }
            }
            break;
        }
        /// View-list and run-end-encoded types the reader cannot decode, but whose physical buffer layout
        /// is known. Keep them as `Unsupported` placeholders (so schema inference can drop them and a
        /// `SELECT` of such a column errors clearly), but parse their children and record the layout so
        /// `skipField` can advance the node/buffer cursors past an *unrequested* column of this type and
        /// keep subset-of-columns reads working.
        case flatbuf::Type_ListView:
            type.kind = TypeKind::Unsupported;
            type.unsupported_type_name = flatbuf::EnumNameType(field.type_type());
            type.skip_layout = ArrowType::SkipLayout::ListView;
            type.children = parseChildren(field.children());
            requireSingleChild(type, "ListView");
            break;
        case flatbuf::Type_LargeListView:
            type.kind = TypeKind::Unsupported;
            type.unsupported_type_name = flatbuf::EnumNameType(field.type_type());
            type.skip_layout = ArrowType::SkipLayout::ListView;
            type.children = parseChildren(field.children());
            requireSingleChild(type, "LargeListView");
            break;
        case flatbuf::Type_RunEndEncoded:
            type.kind = TypeKind::Unsupported;
            type.unsupported_type_name = flatbuf::EnumNameType(field.type_type());
            type.skip_layout = ArrowType::SkipLayout::RunEndEncoded;
            type.children = parseChildren(field.children());
            if (type.children.size() != 2)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Arrow IPC RunEndEncoded type must have exactly two children (run_ends, values), got {}",
                    type.children.size());
            break;
        default:
            /// Keep an unsupported Arrow type as a placeholder rather than failing here, so schema
            /// inference can drop the column with
            /// `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference`. Mapping it to a
            /// ClickHouse type (`fieldToCHType`) or decoding/skipping it still fails with a clear error.
            type.kind = TypeKind::Unsupported;
            type.unsupported_type_name = flatbuf::EnumNameType(field.type_type());
            break;
    }
    return type;
}

}

ArrowSchema parseSchema(const flatbuf::Schema & schema)
{
    ArrowSchema result;
    result.little_endian = schema.endianness() == flatbuf::Endianness_Little;
    if (!result.little_endian)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Big-endian Arrow IPC streams are not supported by the native reader");

    const auto * fields = schema.fields();
    if (fields)
    {
        result.fields.reserve(fields->size());
        for (const auto * field : *fields)
        {
            if (!field)
                continue;
            ArrowField f;
            f.name = field->name() ? field->name()->str() : std::string{};
            f.nullable = field->nullable();
            f.type = parseType(*field);
            if (const auto * dict = field->dictionary())
            {
                DictionaryEncoding enc;
                enc.id = dict->id();
                if (const auto * index_type = dict->indexType())
                {
                    enc.index_bit_width = index_type->bitWidth();
                    enc.index_is_signed = index_type->is_signed();
                }
                f.dictionary = enc;
            }
            f.custom_metadata = parseMetadata(field->custom_metadata());
            result.fields.push_back(std::move(f));
        }
    }

    result.custom_metadata = parseMetadata(schema.custom_metadata());
    return result;
}

bool isUUIDField(const ArrowField & field)
{
    if (field.type.kind != TypeKind::FixedSizeBinary || field.type.byte_width != 16)
        return false;
    auto it = field.custom_metadata.find("ARROW:extension:name");
    if (it != field.custom_metadata.end() && it->second == "arrow.uuid")
        return true;
    auto logical = field.custom_metadata.find("PARQUET:logical_type");
    return logical != field.custom_metadata.end() && logical->second == "UUID";
}

namespace
{

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

/// Arrow permits a timestamp's timezone to be a fixed numeric UTC offset (e.g. "+05:30", "-08:00",
/// "00:00") or the non-IANA marker "fixed" as well as an IANA name. cctz (and therefore
/// `DataTypeDateTime64`) cannot load offset-named zones, so such strings would throw "Cannot load time
/// zone". Normalize only those two intended forms: a full [+|-]HH:MM[:SS] offset (the minute component
/// is mandatory) and the "fixed" marker. Everything else (misspelled IANA names, bare-hour strings like
/// "12" or "+05") is passed through unchanged so `DataTypeDateTime64` still rejects genuinely-malformed
/// producer metadata with "Cannot load time zone" instead of silently masking it. This mirrors
/// `normalizeArrowTimezone` in the Apache-Arrow-library reader (`ArrowColumnToCHColumn.cpp`), so the
/// native reader interprets fixed-offset timezones identically.
String normalizeArrowTimezone(const String & timezone)
{
    if (timezone.empty())
        return timezone;

    cctz::time_zone tz;
    if (cctz::load_time_zone(timezone, &tz))
        return timezone;

    /// "fixed" is a non-IANA marker some Arrow producers emit; values are UTC instants regardless.
    if (timezone == "fixed")
        return "UTC";

    /// Parse a fixed offset of the form [+|-]HH:MM or [+|-]HH:MM:SS (sign optional, defaults to +).
    std::string_view sv = timezone;
    int sign = 1;
    if (!sv.empty() && (sv.front() == '+' || sv.front() == '-'))
    {
        sign = sv.front() == '-' ? -1 : 1;
        sv.remove_prefix(1);
    }

    auto parse_two_digits = [](std::string_view & s, int & out) -> bool
    {
        if (s.size() < 2 || !isdigit(static_cast<unsigned char>(s[0])) || !isdigit(static_cast<unsigned char>(s[1])))
            return false;
        out = (s[0] - '0') * 10 + (s[1] - '0');
        s.remove_prefix(2);
        return true;
    };

    int hours = 0;
    int minutes = 0;
    int seconds = 0;
    /// Require a full [+|-]HH:MM offset: the minute component is mandatory. A bare hour such as "12" or
    /// "+05" is not a valid fixed-offset spelling, so it must pass through and be rejected rather than
    /// silently become Fixed/UTC+12:00:00 and shift displayed values.
    bool ok = parse_two_digits(sv, hours) && !sv.empty() && sv.front() == ':';
    if (ok)
    {
        sv.remove_prefix(1);
        ok = parse_two_digits(sv, minutes);
        if (ok && !sv.empty() && sv.front() == ':')
        {
            sv.remove_prefix(1);
            ok = parse_two_digits(sv, seconds);
        }
    }

    /// Not a parseable offset: pass through unchanged and let `DataTypeDateTime64` reject it. Reject
    /// leftovers, out-of-range fields, and offsets beyond cctz's supported +/-24h range.
    if (!ok || !sv.empty() || minutes >= 60 || seconds >= 60
        || (hours * 3600 + minutes * 60 + seconds) > 24 * 3600)
        return timezone;

    String name = fmt::format("Fixed/UTC{}{:02}:{:02}:{:02}", sign < 0 ? '-' : '+', hours, minutes, seconds);

    /// Final guard: only return the normalized name if cctz can actually load it; otherwise pass the
    /// original through so the error surfaces rather than being silently masked.
    if (cctz::load_time_zone(name, &tz))
        return name;
    return timezone;
}

flatbuffers::Offset<flatbuf::Field>
buildField(
    flatbuffers::FlatBufferBuilder & b, const std::string & name, const DataTypePtr & type, const FormatSettings & settings,
    const DictPlan & plan);

/// The child plan at position `i`, or an empty (no-dictionary) plan when the parent has none. A container
/// nested in a `Variant` recurses with an empty plan (`Variant` is a dictionary boundary), so its own
/// children must fall back to empty plans rather than indexing a non-existent vector entry.
const DictPlan & childPlan(const DictPlans & children, size_t i)
{
    static const DictPlan empty;
    return i < children.size() ? children[i] : empty;
}

flatbuffers::Offset<flatbuf::Field>
buildMapEntriesField(
    flatbuffers::FlatBufferBuilder & b, const DataTypeMap & map_type, const FormatSettings & settings,
    const DictPlans & kv_plans)
{
    /// Arrow map child: a non-nullable struct "entries" with children "key" (non-nullable) and "value".
    VectorWithMemoryTracking<flatbuffers::Offset<flatbuf::Field>> kv;
    kv.push_back(buildField(b, "key", removeNullable(map_type.getKeyType()), settings, childPlan(kv_plans, 0)));
    kv.push_back(buildField(b, "value", map_type.getValueType(), settings, childPlan(kv_plans, 1)));
    auto kv_vec = b.CreateVector(kv);
    auto entries_name = b.CreateString("entries");
    auto struct_type = flatbuf::CreateStruct_(b).Union();
    return flatbuf::CreateField(b, entries_name, false, flatbuf::Type_Struct_, struct_type, 0, kv_vec);
}

flatbuffers::Offset<flatbuf::Field>
buildField(
    flatbuffers::FlatBufferBuilder & b, const std::string & name, const DataTypePtr & type, const FormatSettings & settings,
    const DictPlan & plan)
{
    /// A `LowCardinality` node (top-level or nested) is written as an Arrow dictionary: the field carries
    /// the value type plus a `DictionaryEncoding`, and the record batch holds integer indices.
    flatbuffers::Offset<flatbuf::DictionaryEncoding> dictionary = 0;
    if (plan.here)
    {
        auto index_type = flatbuf::CreateInt(b, plan.here->index_bit_width, plan.here->index_is_signed);
        dictionary = flatbuf::CreateDictionaryEncoding(b, plan.here->id, index_type, /*isOrdered=*/false);
    }

    DataTypePtr t = removeLowCardinality(type);
    const bool nullable = t->isNullable();
    t = removeNullable(t);

    flatbuf::Type type_type = flatbuf::Type_NONE;
    flatbuffers::Offset<void> type_offset;
    VectorWithMemoryTracking<flatbuffers::Offset<flatbuf::Field>> children;

    auto make_int = [&](int bits, bool is_signed)
    {
        type_type = flatbuf::Type_Int;
        type_offset = flatbuf::CreateInt(b, bits, is_signed).Union();
    };

    if (isBool(t))
    {
        type_type = flatbuf::Type_Bool;
        type_offset = flatbuf::CreateBool(b).Union();
    }
    else
    {
        const WhichDataType which(t);
        switch (which.idx)
        {
            case TypeIndex::UInt8: make_int(8, false); break;
            case TypeIndex::UInt16: make_int(16, false); break;
            case TypeIndex::UInt32: make_int(32, false); break;
            case TypeIndex::UInt64: make_int(64, false); break;
            case TypeIndex::Int8: case TypeIndex::Enum8: make_int(8, true); break;
            case TypeIndex::Int16: case TypeIndex::Enum16: make_int(16, true); break;
            case TypeIndex::Int32: make_int(32, true); break;
            case TypeIndex::Int64: make_int(64, true); break;
            case TypeIndex::DateTime: make_int(32, false); break;
            case TypeIndex::Float32:
                type_type = flatbuf::Type_FloatingPoint;
                type_offset = flatbuf::CreateFloatingPoint(b, flatbuf::Precision_SINGLE).Union();
                break;
            case TypeIndex::Float64:
                type_type = flatbuf::Type_FloatingPoint;
                type_offset = flatbuf::CreateFloatingPoint(b, flatbuf::Precision_DOUBLE).Union();
                break;
            case TypeIndex::Date:
                if (settings.arrow.output_date_as_uint16)
                {
                    make_int(16, false);
                }
                else
                {
                    type_type = flatbuf::Type_Date;
                    type_offset = flatbuf::CreateDate(b, flatbuf::DateUnit_DAY).Union();
                }
                break;
            case TypeIndex::Date32:
                type_type = flatbuf::Type_Date;
                type_offset = flatbuf::CreateDate(b, flatbuf::DateUnit_DAY).Union();
                break;
            case TypeIndex::DateTime64:
            {
                const auto & dt64 = assert_cast<const DataTypeDateTime64 &>(*t);
                const int unit = arrowTimeUnitForScale(dt64.getScale());
                /// Always serialize the timezone, matching the Apache Arrow library writer
                /// (`arrow::timestamp(unit, datetime64_type->getTimeZone().getTimeZone())`): a
                /// `DateTime64` without an explicit timezone still carries the server/default one, and
                /// dropping it would emit a timezone-less Arrow `Timestamp` and break writer parity.
                auto tz_off = b.CreateString(dt64.getTimeZone().getTimeZone());
                type_type = flatbuf::Type_Timestamp;
                type_offset = flatbuf::CreateTimestamp(b, static_cast<flatbuf::TimeUnit>(unit), tz_off).Union();
                break;
            }
            case TypeIndex::Time:
                /// ClickHouse `Time` is seconds-of-day stored as Int32 → Arrow `time32[s]`, matching the
                /// library writer (`{"Time", arrow::time32(SECOND)}`).
                type_type = flatbuf::Type_Time;
                type_offset = flatbuf::CreateTime(b, flatbuf::TimeUnit_SECOND, 32).Union();
                break;
            case TypeIndex::Time64:
            {
                /// `Time64(scale)` → Arrow `time32` (SECOND/MILLISECOND, 32-bit) for scale <= 3, else
                /// `time64` (MICROSECOND/NANOSECOND, 64-bit), matching the library writer's `getArrowType`.
                const auto & time64 = assert_cast<const DataTypeTime64 &>(*t);
                const int unit = arrowTimeUnitForScale(time64.getScale());
                const int32_t bit_width
                    = (unit == flatbuf::TimeUnit_SECOND || unit == flatbuf::TimeUnit_MILLISECOND) ? 32 : 64;
                type_type = flatbuf::Type_Time;
                type_offset = flatbuf::CreateTime(b, static_cast<flatbuf::TimeUnit>(unit), bit_width).Union();
                break;
            }
            case TypeIndex::Decimal32: case TypeIndex::Decimal64: case TypeIndex::Decimal128: case TypeIndex::Decimal256:
            {
                const int bit_width = which.idx == TypeIndex::Decimal256 ? 256 : 128;
                type_type = flatbuf::Type_Decimal;
                type_offset = flatbuf::CreateDecimal(
                    b, static_cast<Int32>(getDecimalPrecision(*t)), static_cast<Int32>(getDecimalScale(*t)), bit_width).Union();
                break;
            }
            case TypeIndex::String:
                if (settings.arrow.output_string_as_string)
                {
                    type_type = flatbuf::Type_Utf8;
                    type_offset = flatbuf::CreateUtf8(b).Union();
                }
                else
                {
                    type_type = flatbuf::Type_Binary;
                    type_offset = flatbuf::CreateBinary(b).Union();
                }
                break;
            case TypeIndex::FixedString:
                if (settings.arrow.output_fixed_string_as_fixed_byte_array)
                {
                    type_type = flatbuf::Type_FixedSizeBinary;
                    type_offset = flatbuf::CreateFixedSizeBinary(
                        b, static_cast<Int32>(assert_cast<const DataTypeFixedString &>(*t).getN())).Union();
                }
                else if (settings.arrow.output_string_as_string)
                {
                    type_type = flatbuf::Type_Utf8;
                    type_offset = flatbuf::CreateUtf8(b).Union();
                }
                else
                {
                    type_type = flatbuf::Type_Binary;
                    type_offset = flatbuf::CreateBinary(b).Union();
                }
                break;
            case TypeIndex::Array:
                children.push_back(buildField(b, "item", assert_cast<const DataTypeArray &>(*t).getNestedType(), settings, childPlan(plan.children, 0)));
                type_type = flatbuf::Type_List;
                type_offset = flatbuf::CreateList(b).Union();
                break;
            case TypeIndex::Tuple:
            {
                const auto & tuple = assert_cast<const DataTypeTuple &>(*t);
                const auto & elems = tuple.getElements();
                const auto & elem_names = tuple.getElementNames();
                for (size_t i = 0; i < elems.size(); ++i)
                    children.push_back(buildField(b, elem_names[i], elems[i], settings, childPlan(plan.children, i)));
                type_type = flatbuf::Type_Struct_;
                type_offset = flatbuf::CreateStruct_(b).Union();
                break;
            }
            case TypeIndex::Map:
            {
                children.push_back(buildMapEntriesField(b, assert_cast<const DataTypeMap &>(*t), settings, plan.children));
                type_type = flatbuf::Type_Map;
                type_offset = flatbuf::CreateMap(b, false).Union();
                break;
            }
            case TypeIndex::IPv4:
                make_int(32, false);
                break;
            case TypeIndex::UUID:
            case TypeIndex::IPv6:
            case TypeIndex::Int128:
            case TypeIndex::UInt128:
                type_type = flatbuf::Type_FixedSizeBinary;
                type_offset = flatbuf::CreateFixedSizeBinary(b, 16).Union();
                break;
            case TypeIndex::Int256:
            case TypeIndex::UInt256:
                type_type = flatbuf::Type_FixedSizeBinary;
                type_offset = flatbuf::CreateFixedSizeBinary(b, 32).Union();
                break;
            case TypeIndex::Interval:
            {
                const auto kind = assert_cast<const DataTypeInterval &>(*t).getKind().kind;
                int unit = -1;
                if (kind == IntervalKind::Kind::Second) unit = flatbuf::TimeUnit_SECOND;
                else if (kind == IntervalKind::Kind::Millisecond) unit = flatbuf::TimeUnit_MILLISECOND;
                else if (kind == IntervalKind::Kind::Microsecond) unit = flatbuf::TimeUnit_MICROSECOND;
                else if (kind == IntervalKind::Kind::Nanosecond) unit = flatbuf::TimeUnit_NANOSECOND;
                if (unit >= 0)
                {
                    type_type = flatbuf::Type_Duration;
                    type_offset = flatbuf::CreateDuration(b, static_cast<flatbuf::TimeUnit>(unit)).Union();
                }
                else
                {
                    make_int(64, true);
                }
                break;
            }
            case TypeIndex::Variant:
            {
                /// Mirror the library writer: a dense union with one child per variant (in global order)
                /// plus a trailing `null`-typed child that NULL rows refer to.
                const auto & variant_type = assert_cast<const DataTypeVariant &>(*t);
                const auto & variants = variant_type.getVariants();
                /// An Arrow dense-union type id is a signed int8 and the trailing NULL child uses id
                /// `variants.size()`, so a Variant with more than 127 alternatives cannot be represented.
                /// Reject it here, during schema construction: the schema is written before any batch is
                /// encoded, so an empty result (which never reaches the encoder) is rejected too, instead
                /// of silently emitting a union schema with out-of-range type ids.
                if (variants.size() > 127)
                    throw Exception(
                        ErrorCodes::NOT_IMPLEMENTED,
                        "Native Arrow IPC writer does not support a Variant with {} alternatives (maximum is 127)",
                        variants.size());
                /// `Variant` is a dictionary boundary: a nested `LowCardinality` variant is written as
                /// plain values (matching the encoder), so the children get an empty plan.
                for (const auto & variant : variants)
                    children.push_back(buildField(b, variant->getFamilyName(), variant, settings, DictPlan{}));
                children.push_back(flatbuf::CreateField(
                    b, b.CreateString("NULL"), false, flatbuf::Type_Null, flatbuf::CreateNull(b).Union(), 0, 0));

                VectorWithMemoryTracking<Int32> type_id_list(variants.size() + 1);
                for (size_t i = 0; i < type_id_list.size(); ++i)
                    type_id_list[i] = static_cast<Int32>(i);
                auto type_ids_off = b.CreateVector(type_id_list);
                type_type = flatbuf::Type_Union;
                type_offset = flatbuf::CreateUnion(b, flatbuf::UnionMode_Dense, type_ids_off).Union();
                break;
            }
            default:
                /// A type with no first-class Arrow mapping: written as an Arrow `Binary` column when
                /// `output_format_arrow_unsupported_types_as_binary` is set (matching the encoder and the
                /// Apache Arrow library writer); otherwise rejected.
                if (settings.arrow.output_unsupported_types_as_binary)
                {
                    type_type = flatbuf::Type_Binary;
                    type_offset = flatbuf::CreateBinary(b).Union();
                    break;
                }
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Native Arrow IPC writer does not support type {}. Set "
                    "output_format_arrow_unsupported_types_as_binary = 1 to write it as binary",
                    type->getName());
        }
    }

    /// UUID is an Arrow extension type over fixed_size_binary(16); flag it in the field metadata.
    flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuf::KeyValue>>> custom_metadata_off = 0;
    if (isUUID(t))
    {
        VectorWithMemoryTracking<flatbuffers::Offset<flatbuf::KeyValue>> kvs;
        kvs.push_back(flatbuf::CreateKeyValue(b, b.CreateString("ARROW:extension:name"), b.CreateString("arrow.uuid")));
        kvs.push_back(flatbuf::CreateKeyValue(b, b.CreateString("ARROW:extension:metadata"), b.CreateString("")));
        custom_metadata_off = b.CreateVector(kvs);
    }

    auto name_off = b.CreateString(name);
    flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuf::Field>>> children_off = 0;
    if (!children.empty())
        children_off = b.CreateVector(children);
    return flatbuf::CreateField(b, name_off, nullable, type_type, type_offset, dictionary, children_off, custom_metadata_off);
}

flatbuffers::Offset<flatbuf::Schema> buildSchemaTable(
    flatbuffers::FlatBufferBuilder & builder, const Names & names, const DataTypes & types, const FormatSettings & settings)
{
    const auto plans = assignOutputDictionaries(types, settings);

    VectorWithMemoryTracking<flatbuffers::Offset<flatbuf::Field>> field_offsets;
    field_offsets.reserve(names.size());
    for (size_t i = 0; i < names.size(); ++i)
        field_offsets.push_back(buildField(builder, names[i], types[i], settings, plans[i]));
    auto fields_vec = builder.CreateVector(field_offsets);

    flatbuffers::Offset<flatbuffers::Vector<Int64>> features = 0;
    if (settings.arrow.output_compression_method != FormatSettings::ArrowCompression::NONE)
    {
        const VectorWithMemoryTracking<Int64> feature_list{flatbuf::Feature_COMPRESSED_BODY};
        features = builder.CreateVector(feature_list);
    }
    return flatbuf::CreateSchema(builder, flatbuf::Endianness_Little, fields_vec, 0, features);
}

}

void buildSchemaMessage(
    flatbuffers::FlatBufferBuilder & builder, const Names & names, const DataTypes & types, const FormatSettings & settings)
{
    auto schema = buildSchemaTable(builder, names, types, settings);
    auto message = flatbuf::CreateMessage(builder, flatbuf::MetadataVersion_V5, flatbuf::MessageHeader_Schema, schema.Union(), 0);
    builder.Finish(message);
}

namespace
{
/// Recursively assigns a sequential dictionary id to every dictionary-encoded `LowCardinality` node in
/// `type` (top-level or nested), walking children in the same order the schema builder recurses, so the
/// plan, the schema and the writer's column substitution all agree by position. `Variant` is a boundary:
/// a `LowCardinality` inside it is written as plain values (matching the encoder), not a nested dictionary.
DictPlan buildDictPlan(const DataTypePtr & type, const FormatSettings & settings, Int64 & next_id)
{
    DictPlan plan;
    if (settings.arrow.low_cardinality_as_dictionary && type->lowCardinality())
        plan.here = OutputDictionary{
            next_id++,
            settings.arrow.use_64_bit_indexes_for_dictionary ? 64 : 32,
            settings.arrow.use_signed_indexes_for_dictionary};

    const DataTypePtr t = removeNullable(removeLowCardinality(type));
    const WhichDataType which(t);
    if (which.isArray())
    {
        plan.children.push_back(buildDictPlan(assert_cast<const DataTypeArray &>(*t).getNestedType(), settings, next_id));
    }
    else if (which.isTuple())
    {
        for (const auto & elem : assert_cast<const DataTypeTuple &>(*t).getElements())
            plan.children.push_back(buildDictPlan(elem, settings, next_id));
    }
    else if (which.isMap())
    {
        const auto & map = assert_cast<const DataTypeMap &>(*t);
        /// Mirror `buildMapEntriesField`: the key field uses the non-nullable key type, then the value.
        plan.children.push_back(buildDictPlan(removeNullable(map.getKeyType()), settings, next_id));
        plan.children.push_back(buildDictPlan(map.getValueType(), settings, next_id));
    }
    return plan;
}
}

bool DictPlan::hasAnyDictionary() const
{
    if (here)
        return true;
    for (const auto & child : children)
    {
        if (child.hasAnyDictionary())
            return true;
    }
    return false;
}

DictPlans assignOutputDictionaries(const DataTypes & types, const FormatSettings & settings)
{
    DictPlans plans;
    plans.reserve(types.size());
    Int64 next_id = 0;
    for (const auto & type : types)
        plans.push_back(buildDictPlan(type, settings, next_id));
    return plans;
}

void buildFooter(
    flatbuffers::FlatBufferBuilder & builder,
    const Names & names,
    const DataTypes & types,
    const FormatSettings & settings,
    const ArrowFileBlocks & dictionary_blocks,
    const ArrowFileBlocks & record_blocks)
{
    auto schema = buildSchemaTable(builder, names, types, settings);

    VectorWithMemoryTracking<flatbuf::Block> dict_blocks;
    dict_blocks.reserve(dictionary_blocks.size());
    for (const auto & b : dictionary_blocks)
        dict_blocks.emplace_back(b.offset, b.metadata_length, b.body_length);
    auto dictionaries = builder.CreateVectorOfStructs(dict_blocks);

    VectorWithMemoryTracking<flatbuf::Block> blocks;
    blocks.reserve(record_blocks.size());
    for (const auto & b : record_blocks)
        blocks.emplace_back(b.offset, b.metadata_length, b.body_length);
    auto record_batches = builder.CreateVectorOfStructs(blocks);

    auto footer = flatbuf::CreateFooter(builder, flatbuf::MetadataVersion_V5, schema, dictionaries, record_batches, 0);
    builder.Finish(footer);
}

ArrowFileFooter readArrowFileFooter(SeekableReadBuffer & in, size_t file_size_)
{
    static constexpr std::string_view ARROW_MAGIC = "ARROW1";

    const off_t file_size = static_cast<off_t>(file_size_);
    /// Minimum: leading "ARROW1" + 2 padding bytes, a footer length and the trailing "ARROW1".
    if (file_size < static_cast<off_t>(2 * ARROW_MAGIC.size() + 2 + sizeof(Int32)))
        throw Exception(ErrorCodes::INCORRECT_DATA, "File is too small to be an Arrow file");

    char magic[6];
    in.seek(0, SEEK_SET);
    in.readStrict(magic, ARROW_MAGIC.size());
    if (std::string_view(magic, ARROW_MAGIC.size()) != ARROW_MAGIC)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Not an Arrow file: missing leading magic");

    /// Trailing layout: <footer> <int32 footer length LE> <"ARROW1">.
    in.seek(file_size - static_cast<off_t>(ARROW_MAGIC.size()), SEEK_SET);
    in.readStrict(magic, ARROW_MAGIC.size());
    if (std::string_view(magic, ARROW_MAGIC.size()) != ARROW_MAGIC)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Not an Arrow file: missing trailing magic");

    Int32 footer_length = 0;
    in.seek(file_size - static_cast<off_t>(ARROW_MAGIC.size()) - static_cast<off_t>(sizeof(Int32)), SEEK_SET);
    in.readStrict(reinterpret_cast<char *>(&footer_length), sizeof(footer_length));
    footer_length = DB::fromLittleEndian(footer_length);
    const off_t footer_offset
        = file_size - static_cast<off_t>(ARROW_MAGIC.size()) - static_cast<off_t>(sizeof(Int32)) - footer_length;
    if (footer_length <= 0 || footer_offset < static_cast<off_t>(ARROW_MAGIC.size()))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Corrupted Arrow file footer length {}", footer_length);

    PODArray<char> footer_storage(footer_length);
    in.seek(footer_offset, SEEK_SET);
    in.readStrict(footer_storage.data(), footer_length);

    flatbuffers::Verifier verifier(reinterpret_cast<const uint8_t *>(footer_storage.data()), footer_length);
    if (!flatbuf::VerifyFooterBuffer(verifier))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Corrupted Arrow file footer");
    const auto * footer = flatbuf::GetFooter(footer_storage.data());

    if (!footer->schema())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow file footer has no schema");

    ArrowFileFooter result;
    result.schema = parseSchema(*footer->schema());
    /// Footer blocks are untrusted: a negative offset/metadata/body length would, after being passed on as
    /// a seek target, a metadata bound, or a body size, become a huge unsigned value or seek out of range.
    /// And the block's metadata+body must lie within the data region (before the footer): otherwise a
    /// corrupt footer could declare a block that overlaps the footer or extends past the file, so the
    /// message body would be read from the wrong boundary. (`readBody` independently caps the materialized
    /// body by the buffers the batch actually references, so a forged `bodyLength` cannot itself drive an
    /// oversized allocation.) Reject both here so a malformed footer fails cleanly. The bound is checked
    /// incrementally against `footer_offset` so the sum cannot overflow.
    const Int64 data_region_end = footer_offset;
    auto add_block = [data_region_end](ArrowFileBlocks & blocks, const flatbuf::Block * block)
    {
        const Int64 offset = block->offset();
        const Int32 metadata_length = block->metaDataLength();
        const Int64 body_length = block->bodyLength();
        if (offset < 0 || metadata_length < 0 || body_length < 0)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Arrow file footer block has a negative offset/metadata/body length ({}, {}, {})",
                offset, metadata_length, body_length);
        if (offset > data_region_end || metadata_length > data_region_end - offset
            || body_length > data_region_end - offset - metadata_length)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Arrow file footer block (offset {}, metadata {}, body {}) extends past the data region end {}",
                offset, metadata_length, body_length, data_region_end);
        blocks.push_back({.offset = offset, .metadata_length = metadata_length, .body_length = body_length});
    };
    if (footer->dictionaries())
    {
        for (const auto * block : *footer->dictionaries())
            add_block(result.dictionary_blocks, block);
    }
    if (footer->recordBatches())
    {
        for (const auto * block : *footer->recordBatches())
            add_block(result.record_batch_blocks, block);
    }
    return result;
}

namespace
{

DataTypePtr intToCHType(const ArrowType & type)
{
    if (type.is_signed)
    {
        switch (type.bit_width)
        {
            case 8: return std::make_shared<DataTypeInt8>();
            case 16: return std::make_shared<DataTypeInt16>();
            case 32: return std::make_shared<DataTypeInt32>();
            case 64: return std::make_shared<DataTypeInt64>();
            default: break;
        }
    }
    else
    {
        switch (type.bit_width)
        {
            case 8: return std::make_shared<DataTypeUInt8>();
            case 16: return std::make_shared<DataTypeUInt16>();
            case 32: return std::make_shared<DataTypeUInt32>();
            case 64: return std::make_shared<DataTypeUInt64>();
            default: break;
        }
    }
    throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported Arrow integer bit width {}", type.bit_width);
}

IntervalKind::Kind timeUnitToIntervalKind(int unit)
{
    switch (unit)
    {
        case flatbuf::TimeUnit_SECOND: return IntervalKind::Kind::Second;
        case flatbuf::TimeUnit_MILLISECOND: return IntervalKind::Kind::Millisecond;
        case flatbuf::TimeUnit_MICROSECOND: return IntervalKind::Kind::Microsecond;
        case flatbuf::TimeUnit_NANOSECOND: return IntervalKind::Kind::Nanosecond;
        default: break;
    }
    throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported Arrow duration unit {}", unit);
}

}

DataTypePtr fieldToCHType(
    const ArrowField & field, const FormatSettings & settings, bool make_nullable, bool allow_null_type)
{
    const ArrowType & type = field.type;
    DataTypePtr result;

    switch (type.kind)
    {
        case TypeKind::Int:
            result = intToCHType(type);
            break;
        case TypeKind::FloatingPoint:
            result = type.float_precision == flatbuf::Precision_DOUBLE
                ? DataTypePtr(std::make_shared<DataTypeFloat64>())
                : DataTypePtr(std::make_shared<DataTypeFloat32>());
            break;
        case TypeKind::Bool:
            result = DataTypeFactory::instance().get("Bool");
            break;
        case TypeKind::Decimal:
            result = createDecimal<DataTypeDecimal>(type.precision, type.scale);
            break;
        case TypeKind::Date:
            result = type.unit == flatbuf::DateUnit_DAY
                ? DataTypePtr(std::make_shared<DataTypeDate32>())
                : DataTypePtr(std::make_shared<DataTypeDateTime>());
            break;
        case TypeKind::Timestamp:
        {
            UInt32 scale = static_cast<UInt32>(type.unit) * 3;
            /// Normalize a fixed numeric UTC offset ("+05:30") or the "fixed" marker to a cctz-loadable
            /// name; a genuinely-malformed zone is passed through so `DataTypeDateTime64` rejects it.
            result = type.timezone.empty()
                ? std::make_shared<DataTypeDateTime64>(scale)
                : std::make_shared<DataTypeDateTime64>(scale, normalizeArrowTimezone(type.timezone));
            break;
        }
        case TypeKind::Time:
        {
            /// Arrow `time32`/`time64` → ClickHouse `Time64(unit * 3)`. `Time` carries no timezone, so the
            /// stored value is timezone-independent, matching the library reader (`readColumnWithTimeData`).
            UInt32 scale = static_cast<UInt32>(type.unit) * 3;
            result = std::make_shared<DataTypeTime64>(scale);
            break;
        }
        case TypeKind::Duration:
            result = std::make_shared<DataTypeInterval>(timeUnitToIntervalKind(type.unit));
            break;
        case TypeKind::Utf8:
        case TypeKind::LargeUtf8:
        case TypeKind::Binary:
        case TypeKind::LargeBinary:
        case TypeKind::BinaryView:
        case TypeKind::Utf8View:
            result = std::make_shared<DataTypeString>();
            break;
        case TypeKind::FixedSizeBinary:
            if (isUUIDField(field))
                result = std::make_shared<DataTypeUUID>();
            else
                result = std::make_shared<DataTypeFixedString>(type.byte_width);
            break;
        case TypeKind::List:
        case TypeKind::LargeList:
        case TypeKind::FixedSizeList:
        {
            const ArrowField & element = type.children.at(0);
            result = std::make_shared<DataTypeArray>(fieldToCHType(element, settings, element.nullable, allow_null_type));
            break;
        }
        case TypeKind::Struct:
        {
            DataTypes elems;
            Names names;
            elems.reserve(type.children.size());
            names.reserve(type.children.size());
            for (const ArrowField & child : type.children)
            {
                elems.push_back(fieldToCHType(child, settings, child.nullable, allow_null_type));
                names.push_back(child.name);
            }
            result = std::make_shared<DataTypeTuple>(elems, names);
            break;
        }
        case TypeKind::Map:
        {
            /// Arrow Map is List<Struct<key, value>>: children[0] is the entries struct.
            const ArrowField & entries = type.children.at(0);
            const ArrowField & key = entries.type.children.at(0);
            const ArrowField & value = entries.type.children.at(1);
            /// ClickHouse map keys are never nullable.
            result = std::make_shared<DataTypeMap>(
                fieldToCHType(key, settings, /*make_nullable=*/false),
                fieldToCHType(value, settings, value.nullable, allow_null_type));
            break;
        }
        case TypeKind::Union:
        {
            /// Map an Arrow union to a ClickHouse Variant. Children of the Arrow `null` type are the
            /// placeholder used by the ClickHouse writer for NULL rows; they are not Variant elements.
            DataTypes variants;
            variants.reserve(type.children.size());
            for (const ArrowField & child : type.children)
            {
                if (child.type.kind == TypeKind::Null)
                    continue;
                variants.push_back(removeNullable(fieldToCHType(child, settings, /*make_nullable=*/false)));
            }
            result = std::make_shared<DataTypeVariant>(variants);
            break;
        }
        case TypeKind::Null:
            /// An Arrow `null`-typed field is an all-null column. For the data reader map it to
            /// `Nullable(Nothing)` (the library reader wraps its `Nothing` column the same way because the
            /// field's null count is non-zero); the all-null `Nullable` then casts to the requested target
            /// as NULLs (or column DEFAULTs with `null_as_default`). Return directly — it is already
            /// nullable. Schema inference keeps the flag off and, matching the library reader, treats it as
            /// an unsupported type (`UNKNOWN_TYPE`, so `*_skip_columns_*_in_schema_inference` can drop it).
            if (allow_null_type)
                return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE,
                "Native Arrow IPC reader does not support the `null` type in schema inference (field '{}')",
                field.name);
        case TypeKind::Interval:
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Native Arrow IPC reader does not support this type yet (field '{}')",
                field.name);
        case TypeKind::Unsupported:
            /// `UNKNOWN_TYPE` so schema inference can drop the column with
            /// `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference`.
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE,
                "Native Arrow IPC reader does not support the Arrow type {} (field '{}')",
                field.type.unsupported_type_name, field.name);
    }

    if (!result)
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Could not map Arrow field '{}' to a ClickHouse type", field.name);

    if (make_nullable && result->canBeInsideNullable())
    {
        /// A Tuple (from an Arrow Struct) is wrapped in Nullable only when `allow_experimental_nullable_tuple_type`
        /// is enabled; otherwise schema inference would return a `Nullable(Tuple)` that `CREATE TABLE` rejects.
        /// Without it the struct is read as a plain Tuple (its null map is dropped), as before `Nullable(Tuple)`
        /// was supported. The decode path applies the same gate.
        if (!WhichDataType(result).isTuple() || settings.schema_inference_allow_nullable_tuple_type)
            result = std::make_shared<DataTypeNullable>(result);
    }

    return result;
}

}

#endif
