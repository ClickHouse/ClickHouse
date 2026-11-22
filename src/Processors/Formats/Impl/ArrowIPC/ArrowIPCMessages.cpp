#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCMessages.h>

#if USE_ARROW

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/Impl/ArrowIPC/FlatBuffersCommon.h>
#include <Processors/Formats/Impl/ArrowIPC/MessageWriter.h>
#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCBlockOutputFormat.h>
#include <Common/Exception.h>

#include <cstring>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_TYPE;
}

namespace DB::ArrowIPC
{

namespace
{

constexpr Int32 CONTINUATION = -1;

Int32 readInt32(std::string_view data, size_t pos)
{
    Int32 value = 0;
    memcpy(&value, data.data() + pos, sizeof(value));
    return value;
}

const flatbuf::Message & verifiedMessage(std::string_view header)
{
    flatbuffers::Verifier verifier(reinterpret_cast<const uint8_t *>(header.data()), header.size());
    if (!flatbuf::VerifyMessageBuffer(verifier))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid Arrow IPC message");
    return *flatbuf::GetMessage(header.data());
}

using FieldOffsets = std::vector<flatbuffers::Offset<flatbuf::Field>>; // STYLE_CHECK_ALLOW_STD_CONTAINERS -- FlatBuffers builds vectors from std::vector

flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuf::KeyValue>>>
buildMetadata(flatbuffers::FlatBufferBuilder & b, const KeyValueMetadata & metadata)
{
    if (metadata.empty())
        return 0;
    std::vector<flatbuffers::Offset<flatbuf::KeyValue>> entries; // STYLE_CHECK_ALLOW_STD_CONTAINERS -- FlatBuffers builds vectors from std::vector
    for (const auto & [key, value] : metadata)
        entries.push_back(flatbuf::CreateKeyValue(b, b.CreateString(key), b.CreateString(value)));
    return b.CreateVector(entries);
}

flatbuffers::Offset<flatbuf::Field> buildField(flatbuffers::FlatBufferBuilder & b, const ArrowField & field)
{
    const ArrowType & type = field.type;
    FieldOffsets children;
    for (const auto & child : type.children)
        children.push_back(buildField(b, child));

    flatbuf::Type type_type = flatbuf::Type_NONE;
    flatbuffers::Offset<void> type_offset;
    auto set = [&](flatbuf::Type t, flatbuffers::Offset<void> value)
    {
        type_type = t;
        type_offset = value;
    };

    switch (type.kind)
    {
        case TypeKind::Null: set(flatbuf::Type_Null, flatbuf::CreateNull(b).Union()); break;
        case TypeKind::Int: set(flatbuf::Type_Int, flatbuf::CreateInt(b, type.bit_width, type.is_signed).Union()); break;
        case TypeKind::FloatingPoint:
            set(flatbuf::Type_FloatingPoint, flatbuf::CreateFloatingPoint(b, static_cast<flatbuf::Precision>(type.float_precision)).Union());
            break;
        case TypeKind::Bool: set(flatbuf::Type_Bool, flatbuf::CreateBool(b).Union()); break;
        case TypeKind::Decimal:
            set(flatbuf::Type_Decimal, flatbuf::CreateDecimal(b, type.precision, type.scale, type.decimal_bit_width).Union());
            break;
        case TypeKind::Date: set(flatbuf::Type_Date, flatbuf::CreateDate(b, static_cast<flatbuf::DateUnit>(type.unit)).Union()); break;
        case TypeKind::Time:
            set(flatbuf::Type_Time, flatbuf::CreateTime(b, static_cast<flatbuf::TimeUnit>(type.unit), type.time_bit_width).Union());
            break;
        case TypeKind::Timestamp:
        {
            const auto timezone = type.timezone.empty() ? 0 : b.CreateString(type.timezone);
            set(flatbuf::Type_Timestamp, flatbuf::CreateTimestamp(b, static_cast<flatbuf::TimeUnit>(type.unit), timezone).Union());
            break;
        }
        case TypeKind::Duration:
            set(flatbuf::Type_Duration, flatbuf::CreateDuration(b, static_cast<flatbuf::TimeUnit>(type.unit)).Union());
            break;
        case TypeKind::Interval:
            set(flatbuf::Type_Interval, flatbuf::CreateInterval(b, static_cast<flatbuf::IntervalUnit>(type.unit)).Union());
            break;
        case TypeKind::Utf8: set(flatbuf::Type_Utf8, flatbuf::CreateUtf8(b).Union()); break;
        case TypeKind::LargeUtf8: set(flatbuf::Type_LargeUtf8, flatbuf::CreateLargeUtf8(b).Union()); break;
        case TypeKind::Binary: set(flatbuf::Type_Binary, flatbuf::CreateBinary(b).Union()); break;
        case TypeKind::LargeBinary: set(flatbuf::Type_LargeBinary, flatbuf::CreateLargeBinary(b).Union()); break;
        case TypeKind::BinaryView: set(flatbuf::Type_BinaryView, flatbuf::CreateBinaryView(b).Union()); break;
        case TypeKind::Utf8View: set(flatbuf::Type_Utf8View, flatbuf::CreateUtf8View(b).Union()); break;
        case TypeKind::FixedSizeBinary:
            set(flatbuf::Type_FixedSizeBinary, flatbuf::CreateFixedSizeBinary(b, type.byte_width).Union());
            break;
        case TypeKind::List: set(flatbuf::Type_List, flatbuf::CreateList(b).Union()); break;
        case TypeKind::LargeList: set(flatbuf::Type_LargeList, flatbuf::CreateLargeList(b).Union()); break;
        case TypeKind::FixedSizeList:
            set(flatbuf::Type_FixedSizeList, flatbuf::CreateFixedSizeList(b, type.list_size).Union());
            break;
        case TypeKind::ListView: set(flatbuf::Type_ListView, flatbuf::CreateListView(b).Union()); break;
        case TypeKind::LargeListView: set(flatbuf::Type_LargeListView, flatbuf::CreateLargeListView(b).Union()); break;
        case TypeKind::Struct: set(flatbuf::Type_Struct_, flatbuf::CreateStruct_(b).Union()); break;
        case TypeKind::Map: set(flatbuf::Type_Map, flatbuf::CreateMap(b, false).Union()); break;
        case TypeKind::RunEndEncoded: set(flatbuf::Type_RunEndEncoded, flatbuf::CreateRunEndEncoded(b).Union()); break;
        case TypeKind::Union:
        {
            std::vector<int32_t> type_ids(type.union_type_ids.begin(), type.union_type_ids.end()); // STYLE_CHECK_ALLOW_STD_CONTAINERS -- FlatBuffers builds vectors from std::vector
            const auto type_ids_offset = type_ids.empty() ? 0 : b.CreateVector(type_ids);
            set(flatbuf::Type_Union, flatbuf::CreateUnion(b, static_cast<flatbuf::UnionMode>(type.union_mode), type_ids_offset).Union());
            break;
        }
        case TypeKind::Unsupported:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot serialize the unsupported Arrow type {} of field '{}'",
                type.unsupported_type_name, field.name);
    }

    flatbuffers::Offset<flatbuf::DictionaryEncoding> dictionary = 0;
    if (field.dictionary)
        dictionary = flatbuf::CreateDictionaryEncoding(
            b, field.dictionary->id, flatbuf::CreateInt(b, field.dictionary->index_bit_width, field.dictionary->index_is_signed));

    const auto name = b.CreateString(field.name);
    const auto children_offset = b.CreateVector(children);
    const auto metadata = buildMetadata(b, field.custom_metadata);
    return flatbuf::CreateField(b, name, field.nullable, type_type, type_offset, dictionary, children_offset, metadata);
}

String frameFlatBuffer(const uint8_t * data, size_t size)
{
    String result;
    {
        WriteBufferFromString out(result);
        MessageWriter writer(out);
        writer.writeMessage(data, size, nullptr, 0);
        out.finalize();
    }
    return result;
}

}

namespace
{

bool hasArrowMapping(const DataTypePtr & type, const FormatSettings & settings)
{
    flatbuffers::FlatBufferBuilder builder;
    try
    {
        buildSchemaMessage(builder, {"c"}, {type}, settings);
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::NOT_IMPLEMENTED)
            return false;
        throw;
    }
    return true;
}

/// The innermost part of a column without an Arrow mapping: its path (the names of the tuple elements, joined by
/// dots) and its type.
std::pair<String, DataTypePtr> findTypeWithoutArrowMapping(const String & path, const DataTypePtr & type, const FormatSettings & settings)
{
    const auto t = removeNullable(removeLowCardinality(type));
    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(t.get()))
    {
        for (size_t i = 0; i < tuple->getElements().size(); ++i)
            if (!hasArrowMapping(tuple->getElement(i), settings))
                return findTypeWithoutArrowMapping(path + "." + tuple->getNameByPosition(i + 1), tuple->getElement(i), settings);
    }
    else if (const auto * array = typeid_cast<const DataTypeArray *>(t.get()))
    {
        if (!hasArrowMapping(array->getNestedType(), settings))
            return findTypeWithoutArrowMapping(path, array->getNestedType(), settings);
    }
    else if (const auto * map = typeid_cast<const DataTypeMap *>(t.get()))
    {
        for (const auto & element : {map->getKeyType(), map->getValueType()})
            if (!hasArrowMapping(element, settings))
                return findTypeWithoutArrowMapping(path, element, settings);
    }
    return {path, t};
}

/// Rejects a type without an Arrow mapping as the Apache Arrow library writer did, naming the column it is in, when
/// `output_format_arrow_unsupported_types` is `throw`.
void checkArrowMappings(const Names & names, const DataTypes & types, const FormatSettings & settings)
{
    if (settings.arrow.output_unsupported_types != FormatSettings::ArrowUnsupportedTypes::THROW)
        return;
    for (size_t i = 0; i < types.size(); ++i)
    {
        if (hasArrowMapping(types[i], settings))
            continue;
        const auto [path, type] = findTypeWithoutArrowMapping(names[i], types[i], settings);
        throw Exception(
            ErrorCodes::UNKNOWN_TYPE,
            "The type '{}' of a column '{}' is not supported for conversion into Arrow data format. Set "
            "output_format_arrow_unsupported_types to 'text' or 'binary' to write it as an opaque column",
            type->getName(), path);
    }
}

}

String frameMessage(const IPCMessage & message)
{
    String result;
    const size_t padded = (message.header.size() + 7) / 8 * 8;
    auto append_int32 = [&](Int32 value) { result.append(reinterpret_cast<const char *>(&value), sizeof(value)); };
    append_int32(CONTINUATION);
    append_int32(static_cast<Int32>(padded));
    result += message.header;
    result.append(padded - message.header.size(), '\0');
    result += message.body;
    return result;
}

EncodedStream encodeStream(const Block & header, const Chunks & chunks, const FormatSettings & settings)
{
    checkArrowMappings(header.getNames(), header.getDataTypes(), settings);
    String data;
    {
        WriteBufferFromString out(data);
        ArrowIPCBlockOutputFormat output(out, std::make_shared<const Block>(header), /* stream_ */ true, settings);
        for (const auto & chunk : chunks)
        {
            /// The writer expects full columns, as a pipeline gives it after `MaterializingTransform`. These
            /// chunks come straight from a query, where a constant expression is still a `ColumnConst`.
            Chunk materialized = chunk.clone();
            materializeChunk(materialized);
            output.write(header.cloneWithColumns(materialized.detachColumns()));
        }
        output.finalize();
        out.finalize();
    }

    auto messages = splitStream(data);
    if (messages.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "The Arrow IPC stream has no schema");

    EncodedStream result;
    result.schema = std::move(messages.front());
    for (size_t i = 1; i < messages.size(); ++i)
    {
        const auto & message = verifiedMessage(messages[i].header);
        if (message.header_type() == flatbuf::MessageHeader_RecordBatch && message.header_as_RecordBatch())
            result.num_rows += static_cast<size_t>(message.header_as_RecordBatch()->length());
        result.num_bytes += messages[i].body.size();
        result.batches.push_back(std::move(messages[i]));
    }
    return result;
}

VectorWithMemoryTracking<IPCMessage> splitStream(std::string_view stream)
{
    VectorWithMemoryTracking<IPCMessage> messages;
    size_t pos = 0;
    while (pos + 4 <= stream.size())
    {
        Int32 length = readInt32(stream, pos);
        pos += 4;
        if (length == CONTINUATION)
        {
            if (pos + 4 > stream.size())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Truncated Arrow IPC stream");
            length = readInt32(stream, pos);
            pos += 4;
        }
        if (length == 0)
            break;
        if (length < 0 || pos + static_cast<size_t>(length) > stream.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid length {} of an Arrow IPC message", length);

        IPCMessage message;
        message.header.assign(stream.data() + pos, static_cast<size_t>(length));
        pos += static_cast<size_t>(length);

        const Int64 body_length = verifiedMessage(message.header).bodyLength();
        if (body_length < 0 || pos + static_cast<size_t>(body_length) > stream.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid body length {} of an Arrow IPC message", body_length);
        message.body.assign(stream.data() + pos, static_cast<size_t>(body_length));
        pos += static_cast<size_t>(body_length);
        messages.push_back(std::move(message));
    }
    return messages;
}

String serializeSchemaMessage(const ArrowSchema & schema)
{
    flatbuffers::FlatBufferBuilder b;
    FieldOffsets fields;
    for (const auto & field : schema.fields)
        fields.push_back(buildField(b, field));
    const auto fields_offset = b.CreateVector(fields);
    const auto metadata = buildMetadata(b, schema.custom_metadata);
    const auto schema_offset = flatbuf::CreateSchema(
        b, schema.little_endian ? flatbuf::Endianness_Little : flatbuf::Endianness_Big, fields_offset, metadata);
    const auto message = flatbuf::CreateMessage(b, flatbuf::MetadataVersion_V5, flatbuf::MessageHeader_Schema, schema_offset.Union(), 0);
    b.Finish(message);
    return frameFlatBuffer(b.GetBufferPointer(), b.GetSize());
}

String serializeSchemaMessage(const Names & names, const DataTypes & types, const FormatSettings & settings)
{
    checkArrowMappings(names, types, settings);
    flatbuffers::FlatBufferBuilder b;
    buildSchemaMessage(b, names, types, settings);
    return frameFlatBuffer(b.GetBufferPointer(), b.GetSize());
}

ArrowSchema parseSchemaMessage(std::string_view message)
{
    std::string_view header = message;
    if (header.size() >= 8 && readInt32(header, 0) == CONTINUATION)
    {
        const Int32 length = readInt32(header, 4);
        if (length < 0 || 8 + static_cast<size_t>(length) > header.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid length {} of an Arrow IPC schema message", length);
        header = header.substr(8, static_cast<size_t>(length));
    }
    /// The FlatBuffer has to be aligned for the verifier.
    String aligned(header);
    const auto & parsed = verifiedMessage(aligned);
    if (parsed.header_type() != flatbuf::MessageHeader_Schema || !parsed.header_as_Schema())
        throw Exception(ErrorCodes::INCORRECT_DATA, "The Arrow IPC message is not a schema");
    return parseSchema(*parsed.header_as_Schema(), parsed.version());
}

IPCMessagesReadBuffer::IPCMessagesReadBuffer(NextMessage next_message_)
    : ReadBuffer(nullptr, 0)
    , next_message(std::move(next_message_))
{
}

bool IPCMessagesReadBuffer::nextImpl()
{
    if (finished)
        return false;

    IPCMessage message;
    current.clear();
    auto append_int32 = [&](Int32 value) { current.append(reinterpret_cast<const char *>(&value), sizeof(value)); };
    if (!next_message(message))
    {
        /// The end-of-stream marker.
        append_int32(CONTINUATION);
        append_int32(0);
        finished = true;
    }
    else
    {
        current = frameMessage(message);
    }
    working_buffer = Buffer(current.data(), current.data() + current.size());
    return true;
}

}

#endif
