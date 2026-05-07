#pragma once

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

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

class PuffinMetadataInputFormat : public IInputFormat
{
public:
    PuffinMetadataInputFormat(ReadBuffer & buf, SharedHeader header_);

    String getName() const override { return "PuffinMetadata"; }

private:
    Chunk read() override;

    PuffinFooter footer;
    bool initialized = false;
    size_t blob_index = 0;
};

class PuffinInputFormat : public IInputFormat
{
public:
    PuffinInputFormat(ReadBuffer & buf, SharedHeader header_);

    String getName() const override { return "Puffin"; }

private:
    Chunk read() override;

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
