#pragma once

#include <Processors/Formats/Impl/JSONColumnsBlockInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

namespace DB
{

class JSONColumnsWithMetadataReader final : public JSONColumnsReader
{
public:
    JSONColumnsWithMetadataReader(ReadBuffer & in_, const Block & header_, const FormatSettings & format_settings_);

    void readChunkStart() override;
    bool checkChunkEnd() override;

private:
    const Block header;
};

class JSONColumnsWithMetadataSchemaReader final : public ISchemaReader
{
public:
    explicit JSONColumnsWithMetadataSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);

    NamesAndTypesList readSchema() override;

    /// The types are read from the `meta` section, which the parser validates against the destination
    /// exactly iff `input_format_json_validate_types_from_metadata` is enabled; otherwise the parser
    /// ignores them and reads the data by value, so the inferred schema no longer describes the parse.
    bool hasExactTypesFromData() const override { return format_settings.json.validate_types_from_metadata; }
    bool schemaDescribesParsedData() const override { return format_settings.json.validate_types_from_metadata; }

private:
    const FormatSettings format_settings;
};


}
