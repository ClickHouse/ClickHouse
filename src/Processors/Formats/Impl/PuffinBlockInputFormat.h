#pragma once

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

namespace DB
{

class PuffinMetadataInputFormat : public IInputFormat
{
public:
    PuffinMetadataInputFormat(ReadBuffer & buf, SharedHeader header_);

    String getName() const override { return "PuffinMetadata"; }

private:
    Chunk read() override;

    bool done = false;
};

class PuffinInputFormat : public IInputFormat
{
public:
    PuffinInputFormat(ReadBuffer & buf, SharedHeader header_);

    String getName() const override { return "Puffin"; }

private:
    Chunk read() override;

    bool done = false;
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
