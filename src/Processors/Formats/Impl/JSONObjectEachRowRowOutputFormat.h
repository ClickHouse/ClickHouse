#pragma once

#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <Processors/Formats/Impl/JSONEachRowRowOutputFormat.h>
#include <Formats/FormatSettings.h>


namespace DB
{

class JSONObjectEachRowRowOutputFormat : public JSONEachRowRowOutputFormat
{
public:
    JSONObjectEachRowRowOutputFormat(
        WriteBuffer & out_,
        const Block & header_,
        const RowOutputFormatParams & params_,
        const FormatSettings & settings_);

    String getName() const override { return "JSONObjectEachRowRowOutputFormat"; }

private:
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writeRowBetweenDelimiter() override;

    void writePrefix() override;
    void writeSuffix() override;

    size_t row_num = 0;
};

}
