#pragma once

#include <Core/Block.h>
#include <Processors/Formats/Impl/JSONEachRowRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>


namespace DB
{

class ReadBuffer;


class JSONRowInputFormat final : public JSONEachRowRowInputFormat
{
public:
    JSONRowInputFormat(
        ReadBuffer & in_,
        const Block & header_,
        Params params_,
        const FormatSettings & format_settings_);

    String getName() const override { return "JSONRowInputFormat"; }

private:
    void readPrefix() override;
    void readSuffix() override;

    bool use_metadata;
};

class JSONRowSchemaReader : public ISchemaReader
{
public:
    JSONRowSchemaReader(ReadBuffer & in_);

    NamesAndTypesList readSchema() override;

    bool hasStrictOrderOfColumns() const override { return false; }
};

}
