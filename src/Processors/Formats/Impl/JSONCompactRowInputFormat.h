#pragma once

#include <Processors/Formats/Impl/JSONCompactEachRowRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

namespace DB
{

class JSONCompactRowInputFormat final : public RowInputFormatWithNamesAndTypes
{
public:
    JSONCompactRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_);

    String getName() const override { return "JSONCompactRowInputFormat"; }

private:
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

    void readPrefix() override;
    void readSuffix() override;

    const bool validate_types_from_metadata;
};

class JSONCompactFormatReader : public JSONCompactEachRowFormatReader
{
public:
    JSONCompactFormatReader(ReadBuffer & in_, const FormatSettings & format_settings_);

    bool checkForSuffix() override;
};

}
