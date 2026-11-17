#pragma once

#include <Processors/Formats/Impl/JSONColumnsBlockInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

namespace DB
{

class JSONColumnsWithMetadataReader : public JSONColumnsReader
{
public:
    JSONColumnsWithMetadataReader(ReadBuffer & in_, const Block & header_, const FormatSettings & format_settings_);

    void readChunkStart() override;
    bool checkChunkEnd() override;

private:
    const Block header;
};

class JSONColumnsWithMetadataSchemaReader : public ISchemaReader
{
public:
    explicit JSONColumnsWithMetadataSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);

    NamesAndTypesList readSchema() override;

private:
    const FormatSettings format_settings;
};


}
