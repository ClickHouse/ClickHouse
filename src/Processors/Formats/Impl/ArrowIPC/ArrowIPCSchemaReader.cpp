#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCSchemaReader.h>

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/MessageReader.h>
#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>
#include <Processors/Formats/Impl/ArrowBlockInputFormat.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <IO/ReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WithFileSize.h>
#include <Common/assert_cast.h>
#include <functional>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{
bool schemaNeedsLibrary(const ArrowIPC::ArrowSchema & schema)
{
    if (schema.custom_metadata.contains("geo"))
        return true;
    std::function<bool(const std::vector<ArrowIPC::ArrowField> &)> has_union = [&](const auto & fields)
    {
        for (const auto & field : fields)
            if (field.type.kind == ArrowIPC::TypeKind::Union || has_union(field.type.children))
                return true;
        return false;
    };
    return has_union(schema.fields);
}
}

ArrowIPCSchemaReader::ArrowIPCSchemaReader(ReadBuffer & in_, bool stream_, const FormatSettings & format_settings_)
    : ISchemaReader(in_), stream(stream_), format_settings(format_settings_)
{
}

NamesAndTypesList ArrowIPCSchemaReader::readSchema()
{
    ArrowIPC::ArrowSchema schema;
    if (stream)
    {
        /// Read the schema behind a checkpoint so the stream can be rewound for the library fallback.
        PeekableReadBuffer peekable(in);
        peekable.setCheckpoint();
        {
            ArrowIPC::MessageReader reader(peekable);
            ArrowIPC::MessageReader::Message msg;
            if (!reader.readNextMessage(msg))
                throw Exception(ErrorCodes::INCORRECT_DATA, "The Arrow stream is empty");
            if (msg.header->header_type() != ArrowIPC::flatbuf::MessageHeader_Schema)
                throw Exception(ErrorCodes::INCORRECT_DATA, "The first Arrow IPC message must be the schema");
            schema = ArrowIPC::parseSchema(*msg.header->header_as_Schema());
        }
        if (schemaNeedsLibrary(schema))
        {
            peekable.rollbackToCheckpoint(/*drop=*/true);
            return ArrowSchemaReader(peekable, /*stream_=*/true, format_settings).readSchema();
        }
    }
    else
    {
        /// The file format keeps its schema in the footer, which needs random access.
        SeekableReadBuffer * seekable = dynamic_cast<SeekableReadBuffer *>(&in);
        std::unique_ptr<ReadBuffer> memory_buffer;
        String file_data;
        size_t file_size = 0;
        std::optional<size_t> known_size;
        if (seekable)
            known_size = tryGetFileSizeFromReadBuffer(in);
        if (seekable && known_size && *known_size > 0)
        {
            file_size = *known_size;
        }
        else
        {
            readStringUntilEOF(file_data, in);
            file_size = file_data.size();
            memory_buffer = std::make_unique<ReadBufferFromMemory>(file_data.data(), file_data.size());
            seekable = assert_cast<SeekableReadBuffer *>(memory_buffer.get());
        }
        schema = ArrowIPC::readArrowFileFooter(*seekable, file_size).schema;
        if (schemaNeedsLibrary(schema))
        {
            seekable->seek(0, SEEK_SET);
            return ArrowSchemaReader(*seekable, /*stream_=*/false, format_settings).readSchema();
        }
    }

    /// `schema_inference_make_columns_nullable`: 0 = never nullable, 1 = always nullable,
    /// otherwise (auto) follow the Arrow field's own nullability. This mirrors the library reader.
    const UInt64 make_columns_nullable = format_settings.schema_inference_make_columns_nullable;

    NamesAndTypesList result;
    for (const ArrowIPC::ArrowField & field : schema.fields)
    {
        bool make_nullable;
        if (make_columns_nullable == 0)
            make_nullable = false;
        else if (make_columns_nullable == 1)
            make_nullable = true;
        else
            make_nullable = field.nullable;

        DataTypePtr type = ArrowIPC::fieldToCHType(field, format_settings, make_nullable);
        /// A dictionary-encoded Arrow column is inferred as LowCardinality of its value type.
        if (field.dictionary && type->canBeInsideLowCardinality())
            type = std::make_shared<DataTypeLowCardinality>(type);
        result.emplace_back(field.name, type);
    }
    return result;
}

std::optional<size_t> ArrowIPCSchemaReader::readNumberOrRows()
{
    return std::nullopt;
}

}

#endif
