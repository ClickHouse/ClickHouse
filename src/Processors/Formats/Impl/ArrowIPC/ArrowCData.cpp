#include <Processors/Formats/Impl/ArrowIPC/ArrowCData.h>

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/RecordBatchEncoder.h>
#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>
#include <Formats/FormatSettings.h>
#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>

#include <base/scope_guard.h>
#include <Common/PODArray.h>
#include <IO/BufferWithOwnMemory.h>

#include <cctype>
#include <charconv>
#include <memory>
#include <string>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace DB::ArrowIPC
{

namespace
{

/// The settings the export encodes with: dictionaries live in separate IPC batches, which a C array does not have,
/// so `LowCardinality` columns are exported as their plain values; and the buffers of a C array are not compressed.
FormatSettings exportSettings(const FormatSettings & settings)
{
    FormatSettings result = settings;
    result.arrow.low_cardinality_as_dictionary = false;
    result.arrow.output_compression_method = FormatSettings::ArrowCompression::NONE;
    return result;
}

ArrowIPC::ArrowSchema buildSchema(const Names & names, const DataTypes & types, const FormatSettings & settings)
{
    flatbuffers::FlatBufferBuilder builder;
    buildSchemaMessage(builder, names, types, settings);
    const auto * message = flatbuf::GetMessage(builder.GetBufferPointer());
    return parseSchema(*message->header_as_Schema(), message->version());
}

/// Arrow C format string of a field: https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings
std::string formatString(const ArrowField & field)
{
    if (field.dictionary)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Dictionary-encoded field '{}' cannot be exported to the Arrow C data interface", field.name);

    const ArrowType & type = field.type;
    auto time_unit = [&](int unit) -> char
    {
        switch (unit)
        {
            case flatbuf::TimeUnit_SECOND: return 's';
            case flatbuf::TimeUnit_MILLISECOND: return 'm';
            case flatbuf::TimeUnit_MICROSECOND: return 'u';
            case flatbuf::TimeUnit_NANOSECOND: return 'n';
            default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected Arrow time unit {}", unit);
        }
    };

    switch (type.kind)
    {
        case TypeKind::Null: return "n";
        case TypeKind::Bool: return "b";
        case TypeKind::Int:
            switch (type.bit_width)
            {
                case 8: return type.is_signed ? "c" : "C";
                case 16: return type.is_signed ? "s" : "S";
                case 32: return type.is_signed ? "i" : "I";
                case 64: return type.is_signed ? "l" : "L";
                default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected Arrow integer bit width {}", type.bit_width);
            }
        case TypeKind::FloatingPoint:
            switch (type.float_precision)
            {
                case flatbuf::Precision_HALF: return "e";
                case flatbuf::Precision_SINGLE: return "f";
                case flatbuf::Precision_DOUBLE: return "g";
                default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected Arrow floating point precision {}", type.float_precision);
            }
        case TypeKind::Decimal:
            if (type.decimal_bit_width == 128)
                return fmt::format("d:{},{}", type.precision, type.scale);
            return fmt::format("d:{},{},{}", type.precision, type.scale, type.decimal_bit_width);
        case TypeKind::Date:
            return type.unit == flatbuf::DateUnit_DAY ? "tdD" : "tdm";
        case TypeKind::Time:
            return std::string("tt") + time_unit(type.unit);
        case TypeKind::Timestamp:
            return std::string("ts") + time_unit(type.unit) + ":" + type.timezone;
        case TypeKind::Duration:
            return std::string("tD") + time_unit(type.unit);
        case TypeKind::Interval:
            switch (type.unit)
            {
                case flatbuf::IntervalUnit_YEAR_MONTH: return "tiM";
                case flatbuf::IntervalUnit_DAY_TIME: return "tiD";
                case flatbuf::IntervalUnit_MONTH_DAY_NANO: return "tin";
                default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected Arrow interval unit {}", type.unit);
            }
        case TypeKind::Utf8: return "u";
        case TypeKind::LargeUtf8: return "U";
        case TypeKind::Binary: return "z";
        case TypeKind::LargeBinary: return "Z";
        case TypeKind::BinaryView: return "vz";
        case TypeKind::Utf8View: return "vu";
        case TypeKind::FixedSizeBinary: return fmt::format("w:{}", type.byte_width);
        case TypeKind::List: return "+l";
        case TypeKind::LargeList: return "+L";
        case TypeKind::FixedSizeList: return fmt::format("+w:{}", type.list_size);
        case TypeKind::ListView: return "+vl";
        case TypeKind::LargeListView: return "+vL";
        case TypeKind::Struct: return "+s";
        case TypeKind::Map: return "+m";
        case TypeKind::RunEndEncoded: return "+r";
        case TypeKind::Union:
        {
            std::string result = type.union_mode == flatbuf::UnionMode_Dense ? "+ud:" : "+us:";
            for (size_t i = 0; i < type.children.size(); ++i)
            {
                if (i)
                    result += ',';
                result += std::to_string(type.union_type_ids.empty() ? static_cast<int>(i) : type.union_type_ids[i]);
            }
            return result;
        }
        case TypeKind::Unsupported:
            break;
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Arrow type {} of field '{}' cannot be exported to the Arrow C data interface",
        type.unsupported_type_name, field.name);
}

/// Key-value metadata in the binary layout of the C data interface: the number of pairs, then the length and
/// the bytes of each key and value, all lengths being native-endian int32.
std::string encodeMetadata(const KeyValueMetadata & metadata)
{
    std::string result;
    if (metadata.empty())
        return result;
    auto append_int32 = [&](size_t value)
    {
        const auto v = static_cast<int32_t>(value);
        result.append(reinterpret_cast<const char *>(&v), sizeof(v));
    };
    append_int32(metadata.size());
    for (const auto & [key, value] : metadata)
    {
        append_int32(key.size());
        result += key;
        append_int32(value.size());
        result += value;
    }
    return result;
}

struct ExportedSchema
{
    std::string format;
    std::string name;
    std::string metadata;
    /// A consumer may move a child out, leaving it released here; the children still owned are released with the parent.
    VectorWithMemoryTracking<std::unique_ptr<::ArrowSchema>> children;
    VectorWithMemoryTracking<::ArrowSchema *> children_pointers;

    ~ExportedSchema()
    {
        for (auto & child : children)
        {
            if (child->release)
                child->release(child.get());
        }
    }
};

void releaseSchema(::ArrowSchema * schema)
{
    if (!schema || !schema->release)
        return;
    delete static_cast<ExportedSchema *>(schema->private_data);
    schema->release = nullptr;
}

void fillSchema(
    ::ArrowSchema & out, std::string format, const std::string & name, bool nullable, const KeyValueMetadata & metadata,
    const ArrowFields & children)
{
    auto exported = std::make_unique<ExportedSchema>();
    exported->format = std::move(format);
    exported->name = name;
    exported->metadata = encodeMetadata(metadata);
    exported->children.reserve(children.size());
    for (const auto & child : children)
    {
        /// The child is owned by `exported` before it is filled, so it is released if anything below throws.
        /// A throwing `fillSchema` leaves it unreleasable (`release` is set last), which the destructor skips.
        exported->children.push_back(std::make_unique<::ArrowSchema>());
        auto & child_schema = *exported->children.back();
        child_schema = {};
        fillSchema(child_schema, formatString(child), child.name, child.nullable, child.custom_metadata, child.type.children);
    }
    for (auto & child : exported->children)
        exported->children_pointers.push_back(child.get());

    out.format = exported->format.c_str();
    out.name = exported->name.c_str();
    out.metadata = exported->metadata.empty() ? nullptr : exported->metadata.data();
    out.flags = nullable ? ARROW_FLAG_NULLABLE : 0;
    out.n_children = static_cast<int64_t>(exported->children_pointers.size());
    out.children = exported->children_pointers.empty() ? nullptr : exported->children_pointers.data();
    out.dictionary = nullptr;
    out.release = releaseSchema;
    out.private_data = exported.release();
}

/// A buffer of zeros that zero-length buffers point to: consumers may read the first offset of an empty
/// offsets buffer, and some reject null pointers for buffers other than the validity bitmap.
alignas(64) const char zero_buffer[64] = {};

struct ExportedArray
{
    /// The body of the encoded record batch, shared by all arrays of the tree.
    std::shared_ptr<const PODArray<char>> body;
    VectorWithMemoryTracking<const void *> buffers;
    /// A consumer may move a child out, leaving it released here; the children still owned are released with the parent.
    VectorWithMemoryTracking<std::unique_ptr<::ArrowArray>> children;
    VectorWithMemoryTracking<::ArrowArray *> children_pointers;
    /// Copies of the buffers of the body that are not aligned enough, see `ArrayBuilder::nextBuffer`.
    VectorWithMemoryTracking<Memory<>> aligned_copies;

    ~ExportedArray()
    {
        for (auto & child : children)
        {
            if (child->release)
                child->release(child.get());
        }
    }
};

void releaseArray(::ArrowArray * array)
{
    if (!array || !array->release)
        return;
    delete static_cast<ExportedArray *>(array->private_data);
    array->release = nullptr;
}

/// Builds C arrays from the pre-ordered nodes and buffers of an encoded record batch, walking the fields in
/// the order `RecordBatchDecoder::advanceField` consumes them. The encoder writes no view types, which are
/// the only ones whose C layout differs from the IPC one (a trailing buffer of the variadic buffer sizes).
class ArrayBuilder
{
public:
    ArrayBuilder(const RecordBatchEncoder::EncodedBatch & batch_, std::shared_ptr<const PODArray<char>> body_)
        : batch(batch_), body(std::move(body_))
    {
    }

    void build(const ArrowField & field, ::ArrowArray & out)
    {
        if (field.dictionary)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Dictionary-encoded field '{}' cannot be exported to the Arrow C data interface", field.name);

        const auto & node = nextNode();
        auto exported = std::make_unique<ExportedArray>();
        exported->body = body;
        const ArrowType & type = field.type;

        auto add_children = [&](const ArrowFields & children)
        {
            for (const auto & child : children)
            {
                /// The child is owned by `exported` before it is built, so it is released if anything later throws.
                exported->children.push_back(std::make_unique<::ArrowArray>());
                auto & child_array = *exported->children.back();
                child_array = {};
                build(child, child_array);
            }
        };

        switch (type.kind)
        {
            case TypeKind::Null:
                break;
            case TypeKind::RunEndEncoded:
                add_children(type.children);
                break;
            case TypeKind::Union:
                exported->buffers.push_back(nextBuffer(*exported, false));
                if (type.union_mode == flatbuf::UnionMode_Dense)
                    exported->buffers.push_back(nextBuffer(*exported, false));
                add_children(type.children);
                break;
            default:
            {
                exported->buffers.push_back(nextBuffer(*exported, /* is_validity */ true));
                switch (type.kind)
                {
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
                        exported->buffers.push_back(nextBuffer(*exported, false));
                        break;
                    case TypeKind::Utf8:
                    case TypeKind::LargeUtf8:
                    case TypeKind::Binary:
                    case TypeKind::LargeBinary:
                        exported->buffers.push_back(nextBuffer(*exported, false));
                        exported->buffers.push_back(nextBuffer(*exported, false));
                        break;
                    case TypeKind::List:
                    case TypeKind::LargeList:
                    case TypeKind::Map:
                        exported->buffers.push_back(nextBuffer(*exported, false));
                        add_children(type.children);
                        break;
                    case TypeKind::ListView:
                    case TypeKind::LargeListView:
                        exported->buffers.push_back(nextBuffer(*exported, false));
                        exported->buffers.push_back(nextBuffer(*exported, false));
                        add_children(type.children);
                        break;
                    case TypeKind::FixedSizeList:
                    case TypeKind::Struct:
                        add_children(type.children);
                        break;
                    default:
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Arrow field '{}' cannot be exported to the Arrow C data interface", field.name);
                }
                /// The validity bitmap may be null only when there are no nulls.
                if (node.null_count() == 0)
                    exported->buffers[0] = nullptr;
                break;
            }
        }

        for (auto & child : exported->children)
            exported->children_pointers.push_back(child.get());

        out.length = node.length();
        out.null_count = node.null_count();
        out.offset = 0;
        out.n_buffers = static_cast<int64_t>(exported->buffers.size());
        out.n_children = static_cast<int64_t>(exported->children_pointers.size());
        out.buffers = exported->buffers.empty() ? nullptr : exported->buffers.data();
        out.children = exported->children_pointers.empty() ? nullptr : exported->children_pointers.data();
        out.dictionary = nullptr;
        out.release = releaseArray;
        out.private_data = exported.release();
    }

    void finish() const
    {
        if (node_index != batch.nodes.size() || buffer_index != batch.buffers.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Exported {} of {} Arrow nodes and {} of {} buffers",
                node_index, batch.nodes.size(), buffer_index, batch.buffers.size());
    }

private:
    const flatbuf::FieldNode & nextNode()
    {
        if (node_index >= batch.nodes.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The encoded Arrow record batch has fewer nodes than its schema");
        return batch.nodes[node_index++];
    }

    /// The IPC format aligns the buffers of the body to 8 bytes, but a consumer may need the alignment of the values
    /// (arrow-rs checks it, and its 128-bit integers are aligned to 16 bytes): a buffer that is not aligned to 16
    /// bytes is copied.
    const void * nextBuffer(ExportedArray & exported, bool is_validity)
    {
        if (buffer_index >= batch.buffers.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The encoded Arrow record batch has fewer buffers than its schema");
        const auto & buffer = batch.buffers[buffer_index++];
        if (buffer.length() == 0)
            return is_validity ? nullptr : zero_buffer;
        const char * data = body->data() + buffer.offset();
        if (reinterpret_cast<uintptr_t>(data) % 16 == 0)
            return data;
        /// A small allocation is aligned only to its size (8 bytes for an 8-byte buffer), so ask for the alignment.
        auto & copy = exported.aligned_copies.emplace_back(static_cast<size_t>(buffer.length()), /* alignment */ 16);
        memcpy(copy.data(), data, copy.size());
        chassert(reinterpret_cast<uintptr_t>(copy.data()) % 16 == 0);
        return copy.data();
    }

    const RecordBatchEncoder::EncodedBatch & batch;
    std::shared_ptr<const PODArray<char>> body;
    size_t node_index = 0;
    size_t buffer_index = 0;
};

Names columnNames(size_t count)
{
    /// The names are not part of the exported array; they only have to make a valid schema to encode with.
    Names names;
    names.reserve(count);
    for (size_t i = 0; i < count; ++i)
        names.push_back(fmt::format("c{}", i));
    return names;
}

}

void exportSchemaToC(const Names & names, const DataTypes & types, const FormatSettings & settings, ::ArrowSchema & out)
{
    const auto schema = buildSchema(names, types, exportSettings(settings));
    fillSchema(out, "+s", "", /* nullable */ false, schema.custom_metadata, schema.fields);
}

void exportRecordBatchToC(
    const Columns & columns,
    const DataTypes & types,
    const FormatSettings & settings,
    size_t begin,
    size_t end,
    ::ArrowArray & out)
{
    if (columns.size() != types.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Exporting {} columns with {} types", columns.size(), types.size());

    const auto export_settings = exportSettings(settings);
    const auto schema = buildSchema(columnNames(columns.size()), types, export_settings);

    RecordBatchEncoder encoder(export_settings);
    auto batch = encoder.encode(columns, types, begin, end);
    if (batch.codec)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The Arrow record batch to export to the C data interface is compressed");
    auto body = std::make_shared<const PODArray<char>>(std::move(batch.body));

    ArrayBuilder builder(batch, body);
    auto exported = std::make_unique<ExportedArray>();
    exported->body = body;
    exported->buffers.push_back(nullptr);
    for (const auto & field : schema.fields)
    {
        /// The child is owned by `exported` before it is built, so it is released if anything later throws.
        exported->children.push_back(std::make_unique<::ArrowArray>());
        auto & child = *exported->children.back();
        child = {};
        builder.build(field, child);
    }
    builder.finish();
    for (auto & child : exported->children)
        exported->children_pointers.push_back(child.get());

    out.length = batch.num_rows;
    out.null_count = 0;
    out.offset = 0;
    out.n_buffers = 1;
    out.n_children = static_cast<int64_t>(exported->children_pointers.size());
    out.buffers = exported->buffers.data();
    out.children = exported->children_pointers.empty() ? nullptr : exported->children_pointers.data();
    out.dictionary = nullptr;
    out.release = releaseArray;
    out.private_data = exported.release();
}

namespace
{

[[noreturn]] void throwBadSchema(const String & message)
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid Arrow C data interface schema: {}", message);
}

[[noreturn]] void throwBadArray(const String & message)
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid Arrow C data interface array: {}", message);
}

int parseInt(std::string_view s, std::string_view format)
{
    int value = 0;
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), value);
    if (ec != std::errc() || ptr != s.data() + s.size())
        throwBadSchema(fmt::format("cannot parse the number '{}' in the format string '{}'", s, format));
    return value;
}

std::optional<flatbuf::TimeUnit> parseTimeUnit(char c)
{
    switch (c)
    {
        case 's': return flatbuf::TimeUnit_SECOND;
        case 'm': return flatbuf::TimeUnit_MILLISECOND;
        case 'u': return flatbuf::TimeUnit_MICROSECOND;
        case 'n': return flatbuf::TimeUnit_NANOSECOND;
        default: return std::nullopt;
    }
}

/// Builds the IPC `Field` that the native schema parser understands from a C schema, recursively.
flatbuffers::Offset<flatbuf::Field> buildFieldFromC(flatbuffers::FlatBufferBuilder & b, const ::ArrowSchema & schema, size_t depth)
{
    if (depth > 256)
        throwBadSchema("the schema is nested too deeply");
    if (!schema.format)
        throwBadSchema("a field has no format");

    const ::ArrowSchema & value_schema = schema.dictionary ? *schema.dictionary : schema;
    if (!value_schema.format)
        throwBadSchema("a dictionary has no format");
    const std::string_view format = value_schema.format;

    std::vector<flatbuffers::Offset<flatbuf::Field>> children; // STYLE_CHECK_ALLOW_STD_CONTAINERS -- FlatBuffers builds vectors from std::vector
    if (value_schema.n_children < 0 || (value_schema.n_children > 0 && !value_schema.children))
        throwBadSchema(fmt::format("the field of format '{}' has invalid children", format));
    for (int64_t i = 0; i < value_schema.n_children; ++i)
    {
        if (!value_schema.children[i])
            throwBadSchema(fmt::format("the field of format '{}' has a null child", format));
        children.push_back(buildFieldFromC(b, *value_schema.children[i], depth + 1));
    }

    flatbuf::Type type_type = flatbuf::Type_NONE;
    flatbuffers::Offset<void> type;
    auto set = [&](flatbuf::Type t, flatbuffers::Offset<void> value)
    {
        type_type = t;
        type = value;
    };
    auto unsupported = [&]() { throwBadSchema(fmt::format("unsupported format string '{}'", format)); };

    if (format.size() == 1)
    {
        switch (format[0])
        {
            case 'n': set(flatbuf::Type_Null, flatbuf::CreateNull(b).Union()); break;
            case 'b': set(flatbuf::Type_Bool, flatbuf::CreateBool(b).Union()); break;
            case 'c': set(flatbuf::Type_Int, flatbuf::CreateInt(b, 8, true).Union()); break;
            case 'C': set(flatbuf::Type_Int, flatbuf::CreateInt(b, 8, false).Union()); break;
            case 's': set(flatbuf::Type_Int, flatbuf::CreateInt(b, 16, true).Union()); break;
            case 'S': set(flatbuf::Type_Int, flatbuf::CreateInt(b, 16, false).Union()); break;
            case 'i': set(flatbuf::Type_Int, flatbuf::CreateInt(b, 32, true).Union()); break;
            case 'I': set(flatbuf::Type_Int, flatbuf::CreateInt(b, 32, false).Union()); break;
            case 'l': set(flatbuf::Type_Int, flatbuf::CreateInt(b, 64, true).Union()); break;
            case 'L': set(flatbuf::Type_Int, flatbuf::CreateInt(b, 64, false).Union()); break;
            case 'e': set(flatbuf::Type_FloatingPoint, flatbuf::CreateFloatingPoint(b, flatbuf::Precision_HALF).Union()); break;
            case 'f': set(flatbuf::Type_FloatingPoint, flatbuf::CreateFloatingPoint(b, flatbuf::Precision_SINGLE).Union()); break;
            case 'g': set(flatbuf::Type_FloatingPoint, flatbuf::CreateFloatingPoint(b, flatbuf::Precision_DOUBLE).Union()); break;
            case 'z': set(flatbuf::Type_Binary, flatbuf::CreateBinary(b).Union()); break;
            case 'Z': set(flatbuf::Type_LargeBinary, flatbuf::CreateLargeBinary(b).Union()); break;
            case 'u': set(flatbuf::Type_Utf8, flatbuf::CreateUtf8(b).Union()); break;
            case 'U': set(flatbuf::Type_LargeUtf8, flatbuf::CreateLargeUtf8(b).Union()); break;
            default: unsupported();
        }
    }
    else if (format == "vz")
        set(flatbuf::Type_BinaryView, flatbuf::CreateBinaryView(b).Union());
    else if (format == "vu")
        set(flatbuf::Type_Utf8View, flatbuf::CreateUtf8View(b).Union());
    else if (format.starts_with("d:"))
    {
        /// "d:precision,scale[,bitWidth]"
        const auto args = format.substr(2);
        const size_t comma = args.find(',');
        if (comma == std::string_view::npos)
            unsupported();
        const int precision = parseInt(args.substr(0, comma), format);
        auto rest = args.substr(comma + 1);
        int bit_width = 128;
        const size_t second_comma = rest.find(',');
        if (second_comma != std::string_view::npos)
        {
            bit_width = parseInt(rest.substr(second_comma + 1), format);
            rest = rest.substr(0, second_comma);
        }
        const int scale = parseInt(rest, format);
        set(flatbuf::Type_Decimal, flatbuf::CreateDecimal(b, precision, scale, bit_width).Union());
    }
    else if (format.starts_with("w:"))
        set(flatbuf::Type_FixedSizeBinary, flatbuf::CreateFixedSizeBinary(b, parseInt(format.substr(2), format)).Union());
    else if (format == "tdD")
        set(flatbuf::Type_Date, flatbuf::CreateDate(b, flatbuf::DateUnit_DAY).Union());
    else if (format == "tdm")
        set(flatbuf::Type_Date, flatbuf::CreateDate(b, flatbuf::DateUnit_MILLISECOND).Union());
    else if (format.size() == 3 && format.starts_with("tt"))
    {
        const auto unit = parseTimeUnit(format[2]);
        if (!unit)
            unsupported();
        const int bit_width = (*unit == flatbuf::TimeUnit_SECOND || *unit == flatbuf::TimeUnit_MILLISECOND) ? 32 : 64;
        set(flatbuf::Type_Time, flatbuf::CreateTime(b, *unit, bit_width).Union());
    }
    else if (format.size() >= 4 && format.starts_with("ts") && format[3] == ':')
    {
        const auto unit = parseTimeUnit(format[2]);
        if (!unit)
            unsupported();
        const auto timezone = format.substr(4);
        const auto timezone_offset = timezone.empty() ? 0 : b.CreateString(timezone.data(), timezone.size());
        set(flatbuf::Type_Timestamp, flatbuf::CreateTimestamp(b, *unit, timezone_offset).Union());
    }
    else if (format.size() == 3 && format.starts_with("tD"))
    {
        const auto unit = parseTimeUnit(format[2]);
        if (!unit)
            unsupported();
        set(flatbuf::Type_Duration, flatbuf::CreateDuration(b, *unit).Union());
    }
    else if (format == "tiM")
        set(flatbuf::Type_Interval, flatbuf::CreateInterval(b, flatbuf::IntervalUnit_YEAR_MONTH).Union());
    else if (format == "tiD")
        set(flatbuf::Type_Interval, flatbuf::CreateInterval(b, flatbuf::IntervalUnit_DAY_TIME).Union());
    else if (format == "tin")
        set(flatbuf::Type_Interval, flatbuf::CreateInterval(b, flatbuf::IntervalUnit_MONTH_DAY_NANO).Union());
    else if (format == "+l")
        set(flatbuf::Type_List, flatbuf::CreateList(b).Union());
    else if (format == "+L")
        set(flatbuf::Type_LargeList, flatbuf::CreateLargeList(b).Union());
    else if (format == "+vl")
        set(flatbuf::Type_ListView, flatbuf::CreateListView(b).Union());
    else if (format == "+vL")
        set(flatbuf::Type_LargeListView, flatbuf::CreateLargeListView(b).Union());
    else if (format.starts_with("+w:"))
        set(flatbuf::Type_FixedSizeList, flatbuf::CreateFixedSizeList(b, parseInt(format.substr(3), format)).Union());
    else if (format == "+s")
        set(flatbuf::Type_Struct_, flatbuf::CreateStruct_(b).Union());
    else if (format == "+m")
        set(flatbuf::Type_Map, flatbuf::CreateMap(b, (value_schema.flags & ARROW_FLAG_MAP_KEYS_SORTED) != 0).Union());
    else if (format == "+r")
        set(flatbuf::Type_RunEndEncoded, flatbuf::CreateRunEndEncoded(b).Union());
    else if (format.starts_with("+ud:") || format.starts_with("+us:"))
    {
        std::vector<int32_t> type_ids; // STYLE_CHECK_ALLOW_STD_CONTAINERS -- FlatBuffers builds vectors from std::vector
        auto ids = format.substr(4);
        while (!ids.empty())
        {
            const size_t comma = ids.find(',');
            type_ids.push_back(parseInt(ids.substr(0, comma), format));
            ids = comma == std::string_view::npos ? std::string_view{} : ids.substr(comma + 1);
        }
        const auto mode = format[2] == 'd' ? flatbuf::UnionMode_Dense : flatbuf::UnionMode_Sparse;
        set(flatbuf::Type_Union, flatbuf::CreateUnion(b, mode, b.CreateVector(type_ids)).Union());
    }
    else
        unsupported();

    flatbuffers::Offset<flatbuf::DictionaryEncoding> dictionary = 0;
    if (schema.dictionary)
    {
        const std::string_view index_format = schema.format;
        int bit_width = 0;
        bool is_signed = true;
        if (index_format == "c" || index_format == "C")
            bit_width = 8;
        else if (index_format == "s" || index_format == "S")
            bit_width = 16;
        else if (index_format == "i" || index_format == "I")
            bit_width = 32;
        else if (index_format == "l" || index_format == "L")
            bit_width = 64;
        else
            throwBadSchema(fmt::format("unsupported dictionary index format '{}'", index_format));
        is_signed = std::islower(static_cast<unsigned char>(index_format[0]));
        dictionary = flatbuf::CreateDictionaryEncoding(
            b, /* id */ static_cast<Int64>(depth), flatbuf::CreateInt(b, bit_width, is_signed), (schema.flags & ARROW_FLAG_DICTIONARY_ORDERED) != 0);
    }

    /// Key-value metadata: an int32 number of pairs, then the length and the bytes of each key and value.
    std::vector<flatbuffers::Offset<flatbuf::KeyValue>> metadata; // STYLE_CHECK_ALLOW_STD_CONTAINERS -- FlatBuffers builds vectors from std::vector
    if (schema.metadata)
    {
        const char * pos = schema.metadata;
        auto read_int32 = [&]
        {
            int32_t value = 0;
            memcpy(&value, pos, sizeof(value));
            pos += sizeof(value);
            if (value < 0)
                throwBadSchema("negative length in metadata");
            return value;
        };
        const int32_t count = read_int32();
        for (int32_t i = 0; i < count; ++i)
        {
            const int32_t key_length = read_int32();
            const auto key = b.CreateString(pos, key_length);
            pos += key_length;
            const int32_t value_length = read_int32();
            const auto value = b.CreateString(pos, value_length);
            pos += value_length;
            metadata.push_back(flatbuf::CreateKeyValue(b, key, value));
        }
    }

    const auto name = b.CreateString(schema.name ? schema.name : "");
    return flatbuf::CreateField(
        b,
        name,
        (schema.flags & ARROW_FLAG_NULLABLE) != 0,
        type_type,
        type,
        dictionary,
        b.CreateVector(children),
        metadata.empty() ? 0 : b.CreateVector(metadata));
}

/// Collects the layout of a C array in the form `RecordBatchDecoder` walks: pre-ordered nodes, then buffers in
/// the order of `RecordBatchDecoder::advanceField`. The C data interface lets an array start at an `offset`, which
/// for some types also applies to the children, while an IPC buffer always starts at the first row: buffers are
/// sliced accordingly, and validity bitmaps whose offset is not a multiple of 8 are re-packed.
class LayoutBuilder
{
public:
    explicit LayoutBuilder(RecordBatchDecoder::ExternalBatch & batch_) : batch(batch_) { }

    void add(const ArrowField & field, const ::ArrowArray & array, Int64 offset, Int64 length, size_t depth)
    {
        if (depth > 256)
            throwBadArray("the array is nested too deeply");
        if (field.dictionary)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Dictionary-encoded Arrow arrays are not supported by the Arrow C data interface import");
        if (offset < 0 || length < 0)
            throwBadArray(fmt::format("field '{}' has a negative offset or length", field.name));

        const ArrowType & type = field.type;
        auto expect_buffers = [&](int64_t count)
        {
            if (array.n_buffers != count || (count > 0 && !array.buffers))
                throwBadArray(fmt::format("field '{}' has {} buffers instead of {}", field.name, array.n_buffers, count));
        };
        auto child = [&](size_t i) -> const ::ArrowArray &
        {
            if (array.n_children != static_cast<int64_t>(type.children.size()) || !array.children || !array.children[i])
                throwBadArray(fmt::format("field '{}' has {} children instead of {}", field.name, array.n_children, type.children.size()));
            return *array.children[i];
        };

        /// Filled in once the null count is known; an index, as the children grow the vector.
        batch.nodes.emplace_back(length, 0);
        const size_t node_index = batch.nodes.size() - 1;

        switch (type.kind)
        {
            case TypeKind::Null:
                expect_buffers(0);
                batch.nodes[node_index] = flatbuf::FieldNode(length, length);
                return;
            case TypeKind::RunEndEncoded:
                expect_buffers(0);
                if (offset != 0)
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Sliced run-end encoded Arrow arrays are not supported by the Arrow C data interface import");
                for (size_t i = 0; i < type.children.size(); ++i)
                    add(type.children[i], child(i), child(i).offset, child(i).length, depth + 1);
                return;
            case TypeKind::Union:
            {
                const bool dense = type.union_mode == flatbuf::UnionMode_Dense;
                expect_buffers(dense ? 2 : 1);
                addFixed(array.buffers[0], offset, length, 1);
                if (dense)
                    addFixed(array.buffers[1], offset, length, sizeof(Int32));
                for (size_t i = 0; i < type.children.size(); ++i)
                {
                    const auto & c = child(i);
                    if (dense)
                        add(type.children[i], c, c.offset, c.length, depth + 1);
                    else
                        add(type.children[i], c, c.offset + offset, length, depth + 1);
                }
                return;
            }
            default:
                break;
        }

        const auto & first_buffer = array.n_buffers > 0 && array.buffers ? array.buffers[0] : nullptr;
        const Int64 null_count = addBitmap(first_buffer, offset, length, /* is_validity */ true);
        batch.nodes[node_index] = flatbuf::FieldNode(length, null_count);

        switch (type.kind)
        {
            case TypeKind::Bool:
                expect_buffers(2);
                addBitmap(array.buffers[1], offset, length, /* is_validity */ false);
                return;
            case TypeKind::Int:
                expect_buffers(2);
                addFixed(array.buffers[1], offset, length, static_cast<size_t>(type.bit_width / 8));
                return;
            case TypeKind::FloatingPoint:
                expect_buffers(2);
                addFixed(array.buffers[1], offset, length, type.float_precision == flatbuf::Precision_HALF ? 2 : (type.float_precision == flatbuf::Precision_SINGLE ? 4 : 8));
                return;
            case TypeKind::Decimal:
                expect_buffers(2);
                addFixed(array.buffers[1], offset, length, static_cast<size_t>(type.decimal_bit_width / 8));
                return;
            case TypeKind::Date:
                expect_buffers(2);
                addFixed(array.buffers[1], offset, length, type.unit == flatbuf::DateUnit_DAY ? 4 : 8);
                return;
            case TypeKind::Time:
                expect_buffers(2);
                addFixed(array.buffers[1], offset, length, static_cast<size_t>(type.time_bit_width / 8));
                return;
            case TypeKind::Timestamp:
            case TypeKind::Duration:
                expect_buffers(2);
                addFixed(array.buffers[1], offset, length, 8);
                return;
            case TypeKind::Interval:
                expect_buffers(2);
                addFixed(array.buffers[1], offset, length,
                    type.unit == flatbuf::IntervalUnit_YEAR_MONTH ? 4 : (type.unit == flatbuf::IntervalUnit_DAY_TIME ? 8 : 16));
                return;
            case TypeKind::FixedSizeBinary:
                expect_buffers(2);
                addFixed(array.buffers[1], offset, length, static_cast<size_t>(type.byte_width));
                return;
            case TypeKind::Utf8:
            case TypeKind::Binary:
                expect_buffers(3);
                addVariableWidth<Int32>(array.buffers[1], array.buffers[2], offset, length);
                return;
            case TypeKind::LargeUtf8:
            case TypeKind::LargeBinary:
                expect_buffers(3);
                addVariableWidth<Int64>(array.buffers[1], array.buffers[2], offset, length);
                return;
            case TypeKind::BinaryView:
            case TypeKind::Utf8View:
            {
                /// Validity, views, the data buffers, and the sizes of the data buffers.
                if (array.n_buffers < 3 || !array.buffers)
                    throwBadArray(fmt::format("field '{}' has {} buffers, fewer than a view array needs", field.name, array.n_buffers));
                addFixed(array.buffers[1], offset, length, 16);
                const int64_t num_data = array.n_buffers - 3;
                const auto * sizes = static_cast<const int64_t *>(array.buffers[array.n_buffers - 1]);
                if (num_data > 0 && !sizes)
                    throwBadArray(fmt::format("field '{}' has no buffer sizes", field.name));
                for (int64_t i = 0; i < num_data; ++i)
                {
                    if (sizes[i] < 0)
                        throwBadArray(fmt::format("field '{}' has a negative buffer size", field.name));
                    addSlice(array.buffers[2 + i], sizes[i]);
                }
                batch.variadic_counts.push_back(num_data);
                return;
            }
            case TypeKind::List:
            case TypeKind::Map:
            {
                expect_buffers(2);
                addOffsets<Int32>(array.buffers[1], offset, length);
                const auto & c = child(0);
                add(type.children[0], c, c.offset, c.length, depth + 1);
                return;
            }
            case TypeKind::LargeList:
            {
                expect_buffers(2);
                addOffsets<Int64>(array.buffers[1], offset, length);
                const auto & c = child(0);
                add(type.children[0], c, c.offset, c.length, depth + 1);
                return;
            }
            case TypeKind::ListView:
            case TypeKind::LargeListView:
            {
                expect_buffers(3);
                const size_t width = type.kind == TypeKind::ListView ? sizeof(Int32) : sizeof(Int64);
                addFixed(array.buffers[1], offset, length, width);
                addFixed(array.buffers[2], offset, length, width);
                const auto & c = child(0);
                add(type.children[0], c, c.offset, c.length, depth + 1);
                return;
            }
            case TypeKind::FixedSizeList:
            {
                expect_buffers(1);
                const auto & c = child(0);
                const Int64 size = type.list_size;
                add(type.children[0], c, c.offset + offset * size, length * size, depth + 1);
                return;
            }
            case TypeKind::Struct:
            {
                expect_buffers(1);
                for (size_t i = 0; i < type.children.size(); ++i)
                {
                    const auto & c = child(i);
                    add(type.children[i], c, c.offset + offset, length, depth + 1);
                }
                return;
            }
            default:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Arrow field '{}' cannot be imported from the Arrow C data interface", field.name);
        }
    }

private:
    void addSlice(const void * data, Int64 size)
    {
        if (size > 0 && !data)
            throwBadArray("a non-empty buffer is null");
        batch.buffers.push_back({static_cast<const char *>(data), size});
    }

    void addFixed(const void * data, Int64 offset, Int64 length, size_t width)
    {
        if (length == 0)
        {
            addSlice(nullptr, 0);
            return;
        }
        if (!data)
            throwBadArray("a non-empty buffer is null");
        addSlice(static_cast<const char *>(data) + offset * static_cast<Int64>(width), length * static_cast<Int64>(width));
    }

    /// A bitmap of `length` bits starting at bit `offset`. Returns the number of zero bits for a validity bitmap.
    Int64 addBitmap(const void * data, Int64 offset, Int64 length, bool is_validity)
    {
        if (!data)
        {
            if (!is_validity && length > 0)
                throwBadArray("a non-empty buffer is null");
            addSlice(nullptr, 0);
            return 0;
        }
        const auto * bytes = static_cast<const UInt8 *>(data);
        const Int64 num_bytes = (length + 7) / 8;
        const UInt8 * bits = bytes + offset / 8;
        if (offset % 8 != 0)
        {
            auto & repacked = owned.emplace_back(static_cast<size_t>(num_bytes), 0);
            for (Int64 i = 0; i < length; ++i)
            {
                const Int64 source = offset + i;
                if (bytes[source / 8] & (1u << (source % 8)))
                    repacked[i / 8] = static_cast<char>(static_cast<UInt8>(repacked[i / 8]) | (1u << (i % 8)));
            }
            bits = reinterpret_cast<const UInt8 *>(repacked.data());
        }
        addSlice(bits, num_bytes);
        if (!is_validity)
            return 0;
        Int64 valid = 0;
        for (Int64 i = 0; i < length; ++i)
            valid += (bits[i / 8] >> (i % 8)) & 1;
        return length - valid;
    }

    template <typename Offset>
    const Offset * addOffsets(const void * data, Int64 offset, Int64 length)
    {
        /// An empty array may have no offsets at all.
        if (!data)
        {
            if (length > 0)
                throwBadArray("a non-empty offsets buffer is null");
            static constexpr Offset zero = 0;
            addSlice(&zero, sizeof(zero));
            return &zero;
        }
        const auto * offsets = static_cast<const Offset *>(data) + offset;
        addSlice(offsets, (length + 1) * static_cast<Int64>(sizeof(Offset)));
        return offsets;
    }

    template <typename Offset>
    void addVariableWidth(const void * offsets_data, const void * values, Int64 offset, Int64 length)
    {
        const auto * offsets = addOffsets<Offset>(offsets_data, offset, length);
        /// The interface carries no buffer sizes: the values end where the last offset points.
        const Int64 end = static_cast<Int64>(offsets[length]);
        if (end < 0)
            throwBadArray("negative offset");
        addSlice(values, end);
    }

    RecordBatchDecoder::ExternalBatch & batch;
    /// Re-packed bitmaps; a `PODArray` keeps its data in place when the vector grows.
    VectorWithMemoryTracking<PODArray<char>> owned;

public:
    /// The re-packed bitmaps the buffers may point to; they must live until the batch is decoded.
    VectorWithMemoryTracking<PODArray<char>> takeOwned() { return std::move(owned); }
};

}

ArrowIPC::ArrowSchema importSchemaFromC(const ::ArrowSchema & schema)
{
    if (!schema.format || std::string_view(schema.format) != "+s")
        throwBadSchema(fmt::format("a record batch must be a struct, got format '{}'", schema.format ? schema.format : ""));

    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<flatbuf::Field>> fields; // STYLE_CHECK_ALLOW_STD_CONTAINERS -- FlatBuffers builds vectors from std::vector
    if (schema.n_children < 0 || (schema.n_children > 0 && !schema.children))
        throwBadSchema("the record batch has invalid children");
    for (int64_t i = 0; i < schema.n_children; ++i)
    {
        if (!schema.children[i])
            throwBadSchema("the record batch has a null child");
        fields.push_back(buildFieldFromC(builder, *schema.children[i], 1));
    }
    const auto fields_offset = builder.CreateVector(fields);
    const auto schema_offset = flatbuf::CreateSchema(builder, flatbuf::Endianness_Little, fields_offset);
    builder.Finish(schema_offset);
    return parseSchema(*flatbuffers::GetRoot<flatbuf::Schema>(builder.GetBufferPointer()), flatbuf::MetadataVersion_V5);
}

RecordBatchDecoder::DecodedColumns importRecordBatchFromC(
    ::ArrowArray & array,
    const ArrowIPC::ArrowSchema & schema,
    const FormatSettings & settings,
    const RequestedTypes * target_types)
{
    if (!array.release)
        throwBadArray("the array is released");
    /// Moved out, as the interface specifies for a consumer: the producer's struct is marked released.
    ::ArrowArray owned = array;
    array.release = nullptr;
    SCOPE_EXIT({ owned.release(&owned); });

    if (owned.length < 0 || owned.offset < 0)
        throwBadArray("the record batch has a negative length or offset");
    if (owned.n_children != static_cast<int64_t>(schema.fields.size()) || (owned.n_children > 0 && !owned.children))
        throwBadArray(fmt::format("the record batch has {} columns instead of {}", owned.n_children, schema.fields.size()));
    if (owned.null_count > 0)
        throwBadArray("the record batch has null rows");

    RecordBatchDecoder::ExternalBatch batch;
    batch.length = owned.length;
    LayoutBuilder layout(batch);
    for (size_t i = 0; i < schema.fields.size(); ++i)
    {
        const ::ArrowArray * column = owned.children[i];
        if (!column)
            throwBadArray("the record batch has a null column");
        /// The offset of the record batch applies to its columns, on top of their own.
        if (column->length < owned.offset + owned.length)
            throwBadArray(fmt::format("column '{}' has fewer rows than the record batch", schema.fields[i].name));
        layout.add(schema.fields[i], *column, column->offset + owned.offset, owned.length, 1);
    }
    const auto repacked = layout.takeOwned();

    DictionaryRegistry registry;
    RecordBatchDecoder decoder(schema, settings, registry);
    return decoder.decodeExternalBatch(batch, target_types);
}

}

#endif
