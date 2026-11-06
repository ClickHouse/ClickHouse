#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCSchemaReader.h>

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/MessageReader.h>
#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>
#include <Processors/Formats/Impl/ArrowGeoTypes.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <Formats/SchemaInferenceUtils.h>
#include <IO/ReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WithFileSize.h>
#include <IO/copyData.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <limits>

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

namespace
{

/// An Arrow `null` field holds only NULLs, so its `Nullable(Nothing)` type must stay nullable at every depth when
/// `schema_inference_make_columns_nullable = 0` removes the other `Nullable` wrappers.
DataTypePtr wrapNothingInNullable(const DataTypePtr & type)
{
    if (isNothing(type))
        return makeNullable(type);
    /// Named compound types such as the geo `Point` hold no `null` fields; keep their names.
    if (type->hasCustomName())
        return type;
    if (const auto * array = typeid_cast<const DataTypeArray *>(type.get()))
        return std::make_shared<DataTypeArray>(wrapNothingInNullable(array->getNestedType()));
    if (const auto * map = typeid_cast<const DataTypeMap *>(type.get()))
        return std::make_shared<DataTypeMap>(map->getKeyType(), wrapNothingInNullable(map->getValueType()));
    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        DataTypes elements;
        elements.reserve(tuple->getElements().size());
        for (const auto & element : tuple->getElements())
            elements.push_back(wrapNothingInNullable(element));
        if (tuple->hasExplicitNames())
            return std::make_shared<DataTypeTuple>(elements, tuple->getElementNames());
        return std::make_shared<DataTypeTuple>(elements);
    }
    if (const auto * variant = typeid_cast<const DataTypeVariant *>(type.get()))
    {
        /// `Variant` alternatives cannot be `Nullable` themselves, so restore nullability only inside them.
        DataTypes alternatives;
        alternatives.reserve(variant->getVariants().size());
        for (const auto & alternative : variant->getVariants())
            alternatives.push_back(removeNullable(wrapNothingInNullable(alternative)));
        return std::make_shared<DataTypeVariant>(alternatives);
    }
    return type;
}

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
        schema = ArrowIPC::parseSchema(*msg.header->header_as_Schema(), msg.header->version());
    }
    else
    {
        /// The file format keeps its schema in the footer, which needs random access. Use the input
        /// directly only when it is genuinely seekable, the size is known, and `input_format_allow_seeks`
        /// is not disabled (mirroring `asArrowFile` in the library reader); otherwise load it into memory.
        SeekableReadBuffer * seekable = format_settings.seekable_read ? dynamic_cast<SeekableReadBuffer *>(&in) : nullptr;
        std::unique_ptr<ArrowIPC::InMemoryFile> file_data;
        std::unique_ptr<SeekableReadBuffer> memory_buffer;
        size_t file_size = 0;
        std::optional<size_t> known_size;
        if (seekable)
            known_size = tryGetFileSizeFromReadBuffer(in);
        if (seekable && known_size && *known_size > 0 && seekable->checkIfActuallySeekable())
        {
            file_size = *known_size;
        }
        else
        {
            /// Validate the leading "ARROW1" magic before buffering the (possibly huge, possibly non-Arrow)
            /// remainder, so schema inference on a non-seekable pipe fails fast instead of slurping the
            /// whole stream first — matching the Apache Arrow library reader.
            static constexpr std::string_view ARROW_FILE_MAGIC = "ARROW1";
            char magic[ARROW_FILE_MAGIC.size()] = {};
            in.readStrict(magic, sizeof(magic));
            if (std::string_view(magic, sizeof(magic)) != ARROW_FILE_MAGIC)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Not an Arrow file: the leading {} magic is missing", ARROW_FILE_MAGIC);
            file_data = std::make_unique<ArrowIPC::InMemoryFile>();
            file_data->write(magic, sizeof(magic));
            copyData(in, *file_data);
            file_data->finalize();
            file_size = file_data->count();
            memory_buffer = file_data->getSeekableReadBuffer();
            seekable = memory_buffer.get();
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
            /// Bound the metadata read by the footer block's declared metadata size, so a malformed footer
            /// cannot drive a large metadata allocation from the length prefix at `block.offset`.
            if (!count_reader.readNextMessage(msg, block.metadata_length)
                || msg.header->header_type() != ArrowIPC::flatbuf::MessageHeader_RecordBatch)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Expected a record batch in the Arrow file");
            const Int64 length = msg.header->header_as_RecordBatch()->length();
            if (length < 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC record batch has a negative length {}", length);
            /// The loop seeks to each block before reading its metadata, so the body is never needed here.
            /// Do not read or skip it: that would defeat the metadata-only contract and could fail on a
            /// corrupt or truncated body even though the footer already provides the row counts.
            /// Untrusted lengths: guard the running total against wraparound rather than returning a forged
            /// (smaller) row count for a malformed file.
            if (static_cast<size_t>(length) > std::numeric_limits<size_t>::max() - total_rows)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC file total row count overflows");
            total_rows += static_cast<size_t>(length);
        }
        num_rows_in_file = total_rows;
    }

    /// GeoParquet geometry columns are inferred as their geo type (when the parser is enabled).
    std::unordered_map<String, GeoColumnMetadata> geo_columns; // STYLE_CHECK_ALLOW_STD_CONTAINERS
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
            make_nullable = ArrowIPC::isNullableField(field);

        DataTypePtr type;
        try
        {
            type = ArrowIPC::fieldToCHType(field, format_settings, make_nullable, /*allow_null_type=*/true);
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
            type = wrapNothingInNullable(removeNullableRecursively(type, format_settings));
        else if (make_columns_nullable == 1)
            type = makeNullableRecursively(type, format_settings);

        result.emplace_back(field.name, type);
    }

    /// Without this, `ReadSchemaUtils` would report the empty schema with an empty error message.
    if (result.empty() && !schema.fields.empty())
        throw Exception(
            ErrorCodes::UNKNOWN_TYPE,
            "The native Arrow IPC reader does not support the type of any of the {} columns, and "
            "`input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference` skipped all of them",
            schema.fields.size());
    return result;
}

std::optional<size_t> ArrowIPCSchemaReader::readNumberOrRows()
{
    /// `ArrowStream` cannot be counted without consuming it; the `Arrow` file count is summed in `readSchema`.
    return num_rows_in_file;
}

}

#endif
