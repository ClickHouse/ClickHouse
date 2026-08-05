#pragma once

#include <Formats/FormatSettings.h>

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Storages/ObjectStorage/DataLakes/PuffinFile.h>

namespace DB
{

struct PuffinFooter
{
    std::vector<PuffinBlob> blobs;
    std::vector<UInt8> data;
};

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
