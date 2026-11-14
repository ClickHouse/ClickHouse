#pragma once

#include <Formats/FormatSettings.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/MapWithMemoryTracking.h>

#include <Poco/JSON/Object.h>

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

#include <functional>

namespace DB
{

inline constexpr UInt8 PUFFIN_MAGIC[4] = {0x50, 0x46, 0x41, 0x31};

struct PuffinBlob
{
    String type;
    Int64 snapshot_id = 0;
    Int64 sequence_number = 0;
    VectorWithMemoryTracking<Int32> fields;
    Int64 offset = 0;
    Int64 length = 0;
    String compression_codec;
    MapWithMemoryTracking<String, String> properties;

    /// The `BlobMetadata` JSON. Without `in_footer`, the `blob-metadata` of an Iceberg table statistics entry: no offset, length or codec.
    Poco::JSON::Object::Ptr toJSON(bool in_footer) const;
};

struct PuffinFooter
{
    VectorWithMemoryTracking<PuffinBlob> blobs;
    VectorWithMemoryTracking<UInt8> data;
};

/// TODO: the input formats could read through `PuffinReader` (PuffinFile.h) instead; it would first need their
/// `input_format_allow_seeks` flag, the deletion vector envelope peek and dropping the buffered file when no blob is read.
/// Reads and validates the footer. Buffers the whole file in `data` if `buf` cannot seek.
PuffinFooter readPuffinFooter(ReadBuffer & buf, bool seekable_read);

/// The stored bytes of `blob`, from `data` if `readPuffinFooter` buffered the file.
String readPuffinBlobBytes(
    const PuffinBlob & blob, ReadBuffer & buf, const VectorWithMemoryTracking<UInt8> & data, bool seekable_read);

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

/// Validates the `deletion-vector-v1` blob envelope (length, magic, CRC) and calls `on_position`
/// for every deleted row position in ascending order.
void forEachDeletionVectorPosition(std::string_view blob, const std::function<void(UInt64)> & on_position);

class FormatFactory;
void registerInputFormatPuffin(FormatFactory & factory);
void registerPuffinSchemaReaders(FormatFactory & factory);

}
