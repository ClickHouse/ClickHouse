#include <cstring>
#include <unordered_map>
#include <vector>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WithFileSize.h>
#include <Core/Defines.h>
#include <IO/ReadBuffer.h>
#include <base/types.h>
#include <Processors/Formats/Impl/PuffinBlockInputFormat.h>
#include <IO/ReadBufferFromMemory.h>
#include <Storages/ObjectStorage/DataLakes/PuffinDeletionVectorReader.h>
#include <Storages/ObjectStorage/DataLakes/PuffinFile.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Puffin footer parsing (magic, LZ4 payload, blob metadata JSON) lives in
/// `Storages/ObjectStorage/DataLakes/PuffinFile` and is shared with the Iceberg DV loader.
/// DV blob / materialization ceilings live in `PuffinDeletionVectorReader.h` (`PUFFIN_DV_MAX_*`).

constexpr UInt8 PUFFIN_MAGIC[4] = {0x50, 0x46, 0x41, 0x31};

void checkMagic(const UInt8 * p, const char * context)
{
    if (std::memcmp(p, PUFFIN_MAGIC, 4) != 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Puffin magic ({})", context);
}

PuffinFooter readPuffinFooter(ReadBuffer & buf, bool seekable_read)
{
    PuffinFooter result;

    auto * seekable = dynamic_cast<SeekableReadBuffer *>(&buf);
    auto file_size_opt = tryGetFileSizeFromReadBuffer(buf);

    /// Pipes/FIFOs are SeekableReadBuffer subclasses but fstat reports size 0; require a real
    /// regular file before trusting seek+size (same pattern as ORC/Arrow). Also honor
    /// `input_format_allow_seeks` via FormatSettings.seekable_read.
    if (seekable_read && seekable && seekable->checkIfActuallySeekable() && file_size_opt)
    {
        result.blobs = readPuffinFooterBlobsFromSeekable(*seekable, *file_size_opt);
    }
    else
    {
        /// Fail fast on format mismatch before buffering the rest of a (possibly huge) stream —
        /// same pattern as `asArrowFileLoadIntoMemory`.
        result.data.resize(sizeof(PUFFIN_MAGIC));
        const size_t magic_read = buf.read(reinterpret_cast<char *>(result.data.data()), sizeof(PUFFIN_MAGIC));
        if (magic_read < sizeof(PUFFIN_MAGIC))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin file too small");
        checkMagic(result.data.data(), "header");

        std::vector<UInt8> tmp(DEFAULT_BLOCK_SIZE);
        while (!buf.eof())
        {
            size_t n = buf.read(reinterpret_cast<char *>(tmp.data()), tmp.size());
            result.data.insert(result.data.end(), tmp.data(), tmp.data() + n);
        }

        ReadBufferFromMemory mem_buf(result.data.data(), result.data.size());
        result.blobs = readPuffinFooterBlobsFromSeekable(mem_buf, result.data.size());
    }

    return result;
}

String readPuffinBlobBytes(
    const PuffinBlob & blob, ReadBuffer & buf, const std::vector<UInt8> & data, bool seekable_read)
{
    const size_t length = static_cast<size_t>(blob.length);

    /// When the footer path buffered the whole file, copy from that buffer — the original input
    /// may still look Seekable (e.g. a consumed pipe) but must not be seeked.
    if (!data.empty())
    {
        if (static_cast<UInt64>(blob.offset) + static_cast<UInt64>(blob.length) > data.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob offset/length out of bounds of buffered data");

        return String(reinterpret_cast<const char *>(data.data() + blob.offset), length);
    }

    auto * seekable = dynamic_cast<SeekableReadBuffer *>(&buf);
    if (!seekable_read || !seekable || !seekable->checkIfActuallySeekable())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot read Puffin blob: input is not seekable and was not buffered");

    seekable->seek(blob.offset, SEEK_SET);
    String result(length, '\0');
    seekable->readStrict(result.data(), length);
    return result;
}

void readDeletionVectorEnvelopePrefix(
    const PuffinBlob & blob, ReadBuffer & buf, const std::vector<UInt8> & data, bool seekable_read, UInt8 header[8])
{
    if (!data.empty())
    {
        if (static_cast<UInt64>(blob.offset) + 8 > data.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob offset/length out of bounds of buffered data");
        std::memcpy(header, data.data() + static_cast<size_t>(blob.offset), 8);
        return;
    }

    auto * seekable = dynamic_cast<SeekableReadBuffer *>(&buf);
    if (!seekable_read || !seekable || !seekable->checkIfActuallySeekable())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot read Puffin blob: input is not seekable and was not buffered");

    seekable->seek(blob.offset, SEEK_SET);
    seekable->readStrict(reinterpret_cast<char *>(header), 8);
}

/// Validate absolute size and the first 8 envelope bytes (combined_length + magic) before allocating
/// `blob.length`. CRC / roaring deserialize run later in `deserializeDeletionVectorV1Blob` after a
/// bounded read — we intentionally do not stream roaring here (larger redesign; CRC coverage is
/// over magic+vector).
String readDeletionVectorBlobBytes(
    const PuffinBlob & blob, ReadBuffer & buf, const std::vector<UInt8> & data, bool seekable_read)
{
    if (blob.length < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector blob length is negative");

    if (static_cast<UInt64>(blob.length) > PUFFIN_DV_MAX_BLOB_SIZE)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Deletion vector blob length {} exceeds absolute limit {}",
            blob.length,
            PUFFIN_DV_MAX_BLOB_SIZE);

    if (static_cast<UInt64>(blob.length) < 12)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector blob is too small");

    UInt8 header[8];
    readDeletionVectorEnvelopePrefix(blob, buf, data, seekable_read, header);
    validateDeletionVectorEnvelope(header, blob.length);

    return readPuffinBlobBytes(blob, buf, data, seekable_read);
}

NamesAndTypesList getPuffinMetadataSchema()
{
    return {
        {"blob_type", std::make_shared<DataTypeString>()},
        {"snapshot_id", std::make_shared<DataTypeInt64>()},
        {"sequence_number", std::make_shared<DataTypeInt64>()},
        {"fields", std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>())},
        {"offset", std::make_shared<DataTypeInt64>()},
        {"length", std::make_shared<DataTypeInt64>()},
        {"compression_codec", std::make_shared<DataTypeString>()},
        {"properties", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())},
    };
}

NamesAndTypesList getPuffinSchema()
{
    return {
        {"referenced_data_file", std::make_shared<DataTypeString>()},
        {"deleted_rows", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())},
    };
}

void checkPuffinFormatHeader(const Block & header, const NamesAndTypesList & expected_schema, const char * format_name)
{
    std::unordered_map<String, DataTypePtr> name_to_type;
    for (const auto & [name, type] : expected_schema)
        name_to_type[name] = type;

    String allowed_columns;
    for (const auto & [name, type] : expected_schema)
    {
        if (!allowed_columns.empty())
            allowed_columns += ", ";
        allowed_columns += name;
    }

    for (const auto & [name, type] : header.getNamesAndTypes())
    {
        auto it = name_to_type.find(name);
        if (it == name_to_type.end())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unexpected column: {}. {} format allows only the next columns: {}",
                name,
                format_name,
                allowed_columns);

        if (!it->second->equals(*type))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unexpected type {} for column {}. Expected type: {}",
                type->getName(),
                name,
                it->second->getName());
    }
}

void checkPuffinMetadataHeader(const Block & header)
{
    checkPuffinFormatHeader(header, getPuffinMetadataSchema(), "PuffinMetadata");
}

void checkPuffinHeader(const Block & header)
{
    checkPuffinFormatHeader(header, getPuffinSchema(), "Puffin");
}

}

PuffinMetadataInputFormat::PuffinMetadataInputFormat(ReadBuffer & buf, SharedHeader header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), &buf)
    , seekable_read(format_settings_.seekable_read)
{
    checkPuffinMetadataHeader(getPort().getHeader());
}

Chunk PuffinMetadataInputFormat::read()
{
    if (!initialized)
    {
        blob_index = 0;
        footer = readPuffinFooter(*in, seekable_read);
        /// Metadata never reads blob payloads; drop the full-file buffer from the non-seekable path.
        footer.data.clear();
        footer.data.shrink_to_fit();
        initialized = true;
    }
    if (footer.blobs.size() <= blob_index)
        return {};

    const PuffinBlob & blob = footer.blobs[blob_index++];

    auto col_type = ColumnString::create();
    auto col_snap = ColumnInt64::create();
    auto col_seq = ColumnInt64::create();
    auto col_fields_data = ColumnInt32::create();
    auto col_fields_offsets = ColumnArray::ColumnOffsets::create();
    auto col_offset = ColumnInt64::create();
    auto col_length = ColumnInt64::create();
    auto col_codec = ColumnString::create();
    auto col_props_keys = ColumnString::create();
    auto col_props_vals = ColumnString::create();
    auto col_props_offsets = ColumnArray::ColumnOffsets::create();

    col_type->insertData(blob.type.data(), blob.type.size());
    col_snap->insertValue(blob.snapshot_id);
    col_seq->insertValue(blob.sequence_number);
    for (Int32 f : blob.fields)
        col_fields_data->insertValue(f);
    col_fields_offsets->insertValue(blob.fields.size());
    col_offset->insertValue(blob.offset);
    col_length->insertValue(blob.length);
    col_codec->insertData(blob.compression_codec.data(), blob.compression_codec.size());
    for (const auto & [k, v] : blob.properties)
    {
        col_props_keys->insertData(k.data(), k.size());
        col_props_vals->insertData(v.data(), v.size());
    }
    col_props_offsets->insertValue(blob.properties.size());

    auto col_fields = ColumnArray::create(std::move(col_fields_data), std::move(col_fields_offsets));
    MutableColumns prop_cols;
    prop_cols.push_back(std::move(col_props_keys));
    prop_cols.push_back(std::move(col_props_vals));
    MutableColumnPtr col_props_tuple = ColumnTuple::create(std::move(prop_cols));
    MutableColumnPtr col_props_arr = ColumnArray::create(std::move(col_props_tuple), std::move(col_props_offsets));
    MutableColumnPtr col_props = ColumnMap::create(std::move(col_props_arr));

    std::unordered_map<String, MutableColumnPtr> built;
    built.emplace("blob_type",         std::move(col_type));
    built.emplace("snapshot_id",       std::move(col_snap));
    built.emplace("sequence_number",   std::move(col_seq));
    built.emplace("fields",            std::move(col_fields));
    built.emplace("offset",            std::move(col_offset));
    built.emplace("length",            std::move(col_length));
    built.emplace("compression_codec", std::move(col_codec));
    built.emplace("properties",        std::move(col_props));

    const Block & out_header = getPort().getHeader();
    MutableColumns result;
    result.reserve(out_header.columns());
    for (const auto & col_with_name : out_header)
        result.push_back(std::move(built.at(col_with_name.name)));
    return Chunk(std::move(result), 1);
}

PuffinInputFormat::PuffinInputFormat(ReadBuffer & buf, SharedHeader header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), &buf)
    , seekable_read(format_settings_.seekable_read)
{
    checkPuffinHeader(getPort().getHeader());
    need_deleted_rows = getPort().getHeader().has("deleted_rows");
}

Chunk PuffinInputFormat::read()
{
    if (!initialized)
    {
        blob_index = 0;
        footer = readPuffinFooter(*in, seekable_read);
        /// No deletion-vector payload will be read; drop the full-file buffer from the non-seekable path.
        if (!need_deleted_rows)
        {
            footer.data.clear();
            footer.data.shrink_to_fit();
        }
        initialized = true;
    }

    while (blob_index < footer.blobs.size())
    {
        const size_t current_blob_index = blob_index;
        const auto & blob = footer.blobs[blob_index++];

        if (blob.type != "deletion-vector-v1")
            continue;

        const auto & referenced_data_file = blob.properties.at("referenced-data-file");

        UInt64 expected_cardinality = 0;
        if (!tryParse(expected_cardinality, blob.properties.at("cardinality")))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Puffin blob {}: deletion-vector-v1 property 'cardinality' must be an unsigned integer",
                current_blob_index);

        auto col_file = ColumnString::create();
        col_file->insertData(referenced_data_file.data(), referenced_data_file.size());

        MutableColumnPtr col_rows;
        // Payload I/O and validation (envelope / CRC / roaring) run only when `deleted_rows` is
        // projected. Footer property checks above stay projection-independent.
        // Intentional: subset reads of `referenced_data_file` must not force reading up to
        // PUFFIN_DV_MAX_BLOB_SIZE for blobs that will never be materialized.
        // Materialization ceiling uses footer `cardinality` and must run before blob allocate.
        if (need_deleted_rows)
        {
            if (expected_cardinality > PUFFIN_DV_MAX_MATERIALIZED_POSITIONS)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Deletion vector cardinality {} exceeds materialization limit {}",
                    expected_cardinality,
                    PUFFIN_DV_MAX_MATERIALIZED_POSITIONS);

            const String blob_data = readDeletionVectorBlobBytes(blob, *in, footer.data, seekable_read);
            const auto positions = deserializeDeletionVectorV1Blob(blob_data, expected_cardinality);
            auto col_rows_data = ColumnUInt64::create();
            col_rows_data->getData().insert(positions.begin(), positions.end());

            auto col_rows_offsets = ColumnArray::ColumnOffsets::create();
            col_rows_offsets->insertValue(col_rows_data->size());
            col_rows = ColumnArray::create(std::move(col_rows_data), std::move(col_rows_offsets));
        }

        const Block & out_header = getPort().getHeader();
        std::unordered_map<String, MutableColumnPtr> built;
        built.emplace("referenced_data_file", std::move(col_file));
        if (need_deleted_rows)
            built.emplace("deleted_rows", std::move(col_rows));

        MutableColumns result;
        result.reserve(out_header.columns());
        for (const auto & col_with_name : out_header)
            result.push_back(std::move(built.at(col_with_name.name)));
        return Chunk(std::move(result), 1);
    }

    return {};
}

void PuffinMetadataInputFormat::resetParser()
{
    IInputFormat::resetParser();
    initialized = false;
    blob_index = 0;
    footer = {};
}

void PuffinInputFormat::resetParser()
{
    IInputFormat::resetParser();
    initialized = false;
    blob_index = 0;
    footer = {};
}

PuffinMetadataSchemaReader::PuffinMetadataSchemaReader(ReadBuffer & in_)
    : ISchemaReader(in_)
{
}

NamesAndTypesList PuffinMetadataSchemaReader::readSchema()
{
    return getPuffinMetadataSchema();
}

PuffinSchemaReader::PuffinSchemaReader(ReadBuffer & in_)
    : ISchemaReader(in_)
{
}

NamesAndTypesList PuffinSchemaReader::readSchema()
{
    return getPuffinSchema();
}

void registerInputFormatPuffin(FormatFactory & factory)
{
    factory.registerInputFormat(
        "PuffinMetadata",
        [](ReadBuffer & buf, const Block & sample, const RowInputFormatParams &, const FormatSettings & settings)
        { return std::make_shared<PuffinMetadataInputFormat>(buf, std::make_shared<const Block>(sample), settings); });
    factory.markFormatSupportsSubsetOfColumns("PuffinMetadata");

    factory.setDocumentation("PuffinMetadata", Documentation{
        .description = R"DOCS_MD(
## Description {#description}

Special input format for reading [Apache Iceberg Puffin](https://iceberg.apache.org/puffin-spec/) file footer metadata.
It outputs one row per blob entry from the footer `BlobMetadata` list.

Fixed output columns:
- `blob_type` (`String`) - blob type, for example `deletion-vector-v1`
- `snapshot_id` (`Int64`) - snapshot id of the blob
- `sequence_number` (`Int64`) - sequence number of the blob
- `fields` (`Array(Int32)`) - list of field ids the blob applies to
- `offset` (`Int64`) - offset of the blob payload in the file
- `length` (`Int64`) - length of the blob payload in bytes
- `compression_codec` (`String`) - compression codec of the blob payload, if present
- `properties` (`Map(String, String)`) - blob-specific properties

Optional top-level `FileMetadata.properties` in the footer (for example `created-by`) are type-checked when present but are not returned as columns. If the key is present it must be a JSON object with string values (null is rejected).

LZ4-compressed and uncompressed puffin footers are supported. Footer payload size (and declared LZ4 content size) is bounded by a compression ratio where applicable and an absolute ceiling; oversized footers are rejected before allocation.

## Example usage {#example-usage}

Inspect footer blobs:

```sql
SELECT blob_type, snapshot_id, sequence_number, offset, length, compression_codec,
       mapKeys(properties), mapValues(properties)
FROM file(deletes.puffin, PuffinMetadata);
```

Pair with the `Puffin` format to read `deletion-vector-v1` blob payloads.
)DOCS_MD",
        .related = {"Puffin"},
    });

    factory.registerInputFormat(
        "Puffin",
        [](ReadBuffer & buf, const Block & sample, const RowInputFormatParams &, const FormatSettings & settings)
        { return std::make_shared<PuffinInputFormat>(buf, std::make_shared<const Block>(sample), settings); });
    factory.markFormatSupportsSubsetOfColumns("Puffin");

    factory.setDocumentation("Puffin", Documentation{
        .description = R"DOCS_MD(
## Description {#description}

Input format for reading [Apache Iceberg Puffin](https://iceberg.apache.org/puffin-spec/) files.

The format exposes deleted row positions from `deletion-vector-v1` blobs. Other blob types (for example `apache-datasketches-theta-v1`) are skipped.
If a puffin file contains multiple `deletion-vector-v1` blobs, the format outputs one row per such blob.

Fixed output columns:
- `referenced_data_file` (`String`) - location of the data file the deletion vector applies to (`referenced-data-file` blob property)
- `deleted_rows` (`Array(UInt64)`) - 64-bit row positions deleted according to the deletion vector roaring bitmap

Deletion vectors whose declared `cardinality` exceeds an absolute materialization ceiling are rejected when `deleted_rows` is requested, before the blob is allocated. Footer `deletion-vector-v1` properties (including that `cardinality` parses as an unsigned integer) are always validated. Selecting only `referenced_data_file` skips on-disk payload I/O and therefore also skips envelope, CRC, roaring deserialize, and the materialization ceiling — intentionally, so a path-only projection does not read up to the blob-size cap.

On-disk `deletion-vector-v1` blob length is bounded by an absolute ceiling (aligned with Iceberg's 2 GiB content-size check). When `deleted_rows` is requested, the reader peeks the envelope header (combined length and magic) before allocating the full payload; CRC is verified after the bounded read.

LZ4-compressed and uncompressed puffin footers are supported. Footer payload size (and declared LZ4 content size) is bounded by a compression ratio where applicable and an absolute ceiling; oversized footers are rejected before allocation.

Only a subset of output columns can be requested. A user-provided structure with unexpected column names or types is rejected when the format is created.

## Example usage {#example-usage}

Read deleted row positions with the referenced data file:

```sql
SELECT referenced_data_file, deleted_rows
FROM file(deletes.puffin, Puffin);
```

Expand deleted positions into individual rows:

```sql
SELECT referenced_data_file, row_number
FROM file(deletes.puffin, Puffin)
ARRAY JOIN deleted_rows AS row_number
ORDER BY referenced_data_file, row_number;
```

Use `PuffinMetadata` to inspect footer blob descriptors before reading deletion vectors.
)DOCS_MD",
        .related = {"PuffinMetadata"},
    });
}

void registerPuffinSchemaReaders(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "PuffinMetadata",
        [](ReadBuffer & buf, const FormatSettings &)
        { return std::make_shared<PuffinMetadataSchemaReader>(buf); });

    factory.registerSchemaReader(
        "Puffin",
        [](ReadBuffer & buf, const FormatSettings &)
        { return std::make_shared<PuffinSchemaReader>(buf); });
}

}
