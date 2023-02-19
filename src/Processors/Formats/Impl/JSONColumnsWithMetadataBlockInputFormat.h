#pragma once

#include <Processors/Formats/Impl/JSONColumnsBlockInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

namespace DB
{

class JSONColumnsWithMetadataReader : public JSONColumnsReader
{
public:
    JSONColumnsWithMetadataReader(ReadBuffer & in_, const Block & header_, const FormatSettings & settings);

    void readChunkStart() override;
    bool checkChunkEnd() override;

private:
    const Block & header;
    const bool validate_types_from_metadata;
};

class JSONColumnsWithMetadataSchemaReader : public ISchemaReader
{
public:
    JSONColumnsWithMetadataSchemaReader(ReadBuffer & in_);

    NamesAndTypesList readSchema() override;
};


}
