#pragma once

#include <Processors/Formats/IRowOutputFormat.h>
#include <Formats/ParsedTemplateFormatString.h>

namespace DB
{

class WriteBuffer;

class CustomSeparatedRowOutputFormat final : public IRowOutputFormat
{
public:
    CustomSeparatedRowOutputFormat(const Block & header_, WriteBuffer & out_, const RowOutputFormatParams & params_, const FormatSettings & format_settings_, bool with_names_, bool with_types_);

    String getName() const override { return "CustomSeparatedRowOutputFormat"; }

private:
    using EscapingRule = FormatSettings::EscapingRule;

    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writeRowBetweenDelimiter() override;
    void writePrefix() override;
    void writeSuffix() override;

    void writeLine(const std::vector<String> & values);
    bool with_names;
    bool with_types;
    const FormatSettings format_settings;
    EscapingRule escaping_rule;
};

}
