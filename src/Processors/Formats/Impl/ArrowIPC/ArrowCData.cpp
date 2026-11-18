#include <Processors/Formats/Impl/ArrowIPC/ArrowCData.h>

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/RecordBatchEncoder.h>
#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>
#include <Formats/FormatSettings.h>
#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>

#include <memory>
#include <string>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace DB::ArrowIPC
{

namespace
{

/// The settings the export encodes with: dictionaries live in separate IPC batches, which a C array does not have,
/// so `LowCardinality` columns are exported as their plain values.
FormatSettings exportSettings(const FormatSettings & settings)
{
    FormatSettings result = settings;
    result.arrow.low_cardinality_as_dictionary = false;
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
                exported->buffers.push_back(nextBuffer(false));
                if (type.union_mode == flatbuf::UnionMode_Dense)
                    exported->buffers.push_back(nextBuffer(false));
                add_children(type.children);
                break;
            default:
            {
                exported->buffers.push_back(nextBuffer(/* is_validity */ true));
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
                        exported->buffers.push_back(nextBuffer(false));
                        break;
                    case TypeKind::Utf8:
                    case TypeKind::LargeUtf8:
                    case TypeKind::Binary:
                    case TypeKind::LargeBinary:
                        exported->buffers.push_back(nextBuffer(false));
                        exported->buffers.push_back(nextBuffer(false));
                        break;
                    case TypeKind::List:
                    case TypeKind::LargeList:
                    case TypeKind::Map:
                        exported->buffers.push_back(nextBuffer(false));
                        add_children(type.children);
                        break;
                    case TypeKind::ListView:
                    case TypeKind::LargeListView:
                        exported->buffers.push_back(nextBuffer(false));
                        exported->buffers.push_back(nextBuffer(false));
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

    const void * nextBuffer(bool is_validity)
    {
        if (buffer_index >= batch.buffers.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The encoded Arrow record batch has fewer buffers than its schema");
        const auto & buffer = batch.buffers[buffer_index++];
        if (buffer.length() == 0)
            return is_validity ? nullptr : zero_buffer;
        return body->data() + buffer.offset();
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

}

#endif
