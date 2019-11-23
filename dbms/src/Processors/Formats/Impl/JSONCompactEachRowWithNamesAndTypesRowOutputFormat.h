#pragma once

#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <Processors/Formats/Impl/JSONCompactEachRowRowOutputFormat.h>
#include <Formats/FormatSettings.h>

namespace DB
{

class JSONCompactEachRowWithNamesAndTypesRowOutputFormat : public JSONCompactEachRowRowOutputFormat
{
public:
    JSONCompactEachRowWithNamesAndTypesRowOutputFormat(WriteBuffer & out_,
            const Block & header_,
            FormatFactory::WriteCallback callback,
            const FormatSettings & settings_);

    String getName() const override { return "JSONCompactEachRowWithNamesAndTypesRowOutputFormat"; }

    void writePrefix() override;

    void writeBeforeTotals() override;
    void writeTotals(const Columns & columns, size_t row_num) override;
    void writeAfterTotals() override;

protected:
    NamesAndTypes fields;

    FormatSettings settings;
};

}
