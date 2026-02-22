#pragma once

#include <Processors/Formats/Impl/JSONCompactEachRowRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

namespace DB
{
class JSONCompactFormatReader;
class JSONCompactRowInputFormat final : public RowInputFormatWithNamesAndTypes<JSONCompactFormatReader>
{
public:
    JSONCompactRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_);

    String getName() const override { return "JSONCompactRowInputFormat"; }

private:
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;
    bool supportsCountRows() const override { return true; }

    void readPrefix() override;
    void readSuffix() override;
};

class JSONCompactFormatReader : public JSONCompactEachRowFormatReader
{
public:
    JSONCompactFormatReader(ReadBuffer & in_, const FormatSettings & format_settings_);

    bool checkForSuffix() override;
};

}
