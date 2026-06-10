#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCSchemaReader.h>

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/MessageReader.h>
#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>
#include <Processors/Formats/Impl/ArrowGeoTypes.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Formats/SchemaInferenceUtils.h>
#include <IO/ReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WithFileSize.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_TYPE;
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
        ArrowIPC::MessageReader reader(in);
        ArrowIPC::MessageReader::Message msg;
        if (!reader.readNextMessage(msg))
            throw Exception(ErrorCodes::INCORRECT_DATA, "The Arrow stream is empty");
        if (msg.header->header_type() != ArrowIPC::flatbuf::MessageHeader_Schema)
            throw Exception(ErrorCodes::INCORRECT_DATA, "The first Arrow IPC message must be the schema");
        schema = ArrowIPC::parseSchema(*msg.header->header_as_Schema());
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
        ArrowIPC::ArrowFileFooter footer = ArrowIPC::readArrowFileFooter(*seekable, file_size);
        schema = std::move(footer.schema);

        /// Preserve the file row-count optimization that the library schema reader exposes via `CountRows`:
        /// sum each record batch's length by reading just its (small) metadata message from the footer blocks.
        ArrowIPC::MessageReader count_reader(*seekable);
        size_t total_rows = 0;
        for (const auto & block : footer.record_batch_blocks)
        {
            seekable->seek(block.offset, SEEK_SET);
            ArrowIPC::MessageReader::Message msg;
            if (!count_reader.readNextMessage(msg) || msg.header->header_type() != ArrowIPC::flatbuf::MessageHeader_RecordBatch)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Expected a record batch in the Arrow file");
            const int64_t length = msg.header->header_as_RecordBatch()->length();
            if (length < 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC record batch has a negative length {}", length);
            total_rows += static_cast<size_t>(length);
            count_reader.skipBody(msg.body_length);
        }
        num_rows_in_file = total_rows;
    }

    /// GeoParquet geometry columns are inferred as their geo type (when the parser is enabled).
    std::unordered_map<String, GeoColumnMetadata> geo_columns;
    if (format_settings.parquet.allow_geoparquet_parser)
    {
        auto geo_it = schema.custom_metadata.find("geo");
        geo_columns = parseGeoMetadataEncoding(geo_it != schema.custom_metadata.end() ? &geo_it->second : nullptr);
    }

    /// `schema_inference_make_columns_nullable`: 0 = never nullable, 1 = always nullable,
    /// otherwise (auto) follow the Arrow field's own nullability. This mirrors the library reader.
    const UInt64 make_columns_nullable = format_settings.schema_inference_make_columns_nullable;

    NamesAndTypesList result;
    for (const ArrowIPC::ArrowField & field : schema.fields)
    {
        auto geo_it = geo_columns.find(field.name);
        if (geo_it != geo_columns.end())
        {
            result.emplace_back(field.name, getGeoDataType(geo_it->second.type));
            continue;
        }

        /// For the `0` (never) and `1` (always) modes, infer the bare type without wrapping the top level
        /// in `Nullable` and let `remove`/`makeNullableRecursively` below apply nullability uniformly to
        /// every leaf. Wrapping here would make the recursive pass short-circuit on the outer `Nullable`
        /// and leave nested elements (e.g. `Tuple` fields) with the wrong nullability. The `auto` mode
        /// keeps the per-field Arrow nullability flag.
        bool make_nullable = false;
        if (make_columns_nullable != 0 && make_columns_nullable != 1)
            make_nullable = field.nullable;

        DataTypePtr type;
        try
        {
            type = ArrowIPC::fieldToCHType(field, format_settings, make_nullable);
        }
        catch (const Exception & e)
        {
            /// `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference`: drop a
            /// top-level column whose Arrow type the reader cannot map, instead of failing inference.
            if (format_settings.arrow.skip_columns_with_unsupported_types_in_schema_inference
                && (e.code() == ErrorCodes::NOT_IMPLEMENTED || e.code() == ErrorCodes::UNKNOWN_TYPE))
                continue;
            throw;
        }

        /// A dictionary-encoded Arrow column is inferred as LowCardinality of its value type.
        if (field.dictionary && type->canBeInsideLowCardinality())
            type = std::make_shared<DataTypeLowCardinality>(type);

        /// `fieldToCHType` only applies the nullability mode to the top-level field; for `0`/`1` apply it
        /// recursively to nested types too, mirroring the library reader (which would otherwise infer a
        /// different nested schema for the same file and cache it under the same key).
        if (make_columns_nullable == 0)
            type = removeNullableRecursively(type, format_settings);
        else if (make_columns_nullable == 1)
            type = makeNullableRecursively(type, format_settings);

        result.emplace_back(field.name, type);
    }
    return result;
}

std::optional<size_t> ArrowIPCSchemaReader::readNumberOrRows()
{
    /// `ArrowStream` cannot be counted without consuming it; the `Arrow` file count is summed in `readSchema`.
    return num_rows_in_file;
}

}

#endif
