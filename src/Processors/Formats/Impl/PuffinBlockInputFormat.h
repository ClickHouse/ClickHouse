#pragma once

#include <Columns/ColumnsNumber.h>
#include <Formats/FormatSettings.h>
#include <IO/SeekableReadBuffer.h>

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

#include <optional>
#include <string_view>
#include <vector>

namespace DB
{

struct PuffinBlob
{
    String type;
    Int64 snapshot_id = 0;
    Int64 sequence_number = 0;
    std::vector<Int32> fields;
    Int64 offset = 0;
    Int64 length = 0;
    String compression_codec;
    std::map<String, String> properties;
};

struct PuffinFooter
{
    std::vector<PuffinBlob> blobs;
    std::vector<UInt8> data;
};

/// Shared with the Iceberg deletion-vector loader (seekable object-storage path).
std::vector<PuffinBlob> readPuffinFooterFromSeekable(SeekableReadBuffer & seekable, size_t file_size);

/// Shared deletion-vector-v1 payload helpers (also used by `PuffinDeletionVectorReader`).
std::string_view extractDeletionVectorPayload(std::string_view blob);
std::vector<UInt64> deserializeRoaringPositionBitmap(
    std::string_view bytes, std::optional<UInt64> expected_cardinality = std::nullopt);
void deserializeDeletionVectorV1(std::string_view blob, UInt64 expected_cardinality, ColumnUInt64 & positions);

/// Validate deletion-vector-v1 footer identity (`snapshot-id` / `sequence-number` / `fields` /
/// required string properties). Returns parsed `cardinality`. Used by the SQL `Puffin` path and
/// by `bindDeletionVectorBlob`.
UInt64 requireDeletionVectorV1Properties(const PuffinBlob & blob, size_t blob_index);

class PuffinMetadataInputFormat : public IInputFormat
{
public:
    PuffinMetadataInputFormat(ReadBuffer & buf, SharedHeader header_, const FormatSettings & format_settings_);

    String getName() const override { return "PuffinMetadata"; }

    void resetParser() override;

private:
    Chunk read() override;

    bool seekable_read = true;
    PuffinFooter footer;
    bool initialized = false;
    size_t blob_index = 0;
};

class PuffinInputFormat : public IInputFormat
{
public:
    PuffinInputFormat(ReadBuffer & buf, SharedHeader header_, const FormatSettings & format_settings_);

    String getName() const override { return "Puffin"; }

    void resetParser() override;

private:
    Chunk read() override;

    bool seekable_read = true;
    bool need_deleted_rows = true;
    PuffinFooter footer;
    bool initialized = false;
    size_t blob_index = 0;
};

class PuffinMetadataSchemaReader : public ISchemaReader
{
public:
    explicit PuffinMetadataSchemaReader(ReadBuffer & in_);
    NamesAndTypesList readSchema() override;
};

class PuffinSchemaReader : public ISchemaReader
{
public:
    explicit PuffinSchemaReader(ReadBuffer & in_);
    NamesAndTypesList readSchema() override;
};

class FormatFactory;
void registerInputFormatPuffin(FormatFactory & factory);
void registerPuffinSchemaReaders(FormatFactory & factory);

}
