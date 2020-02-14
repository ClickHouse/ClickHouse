#pragma once

#include <regex>
#include <Core/Block.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Formats/FormatSettings.h>
#include <Formats/FormatFactory.h>

namespace DB
{

class ReadBuffer;


class RegexpRowInputFormat : public IRowInputFormat
{
public:
    RegexpRowInputFormat(ReadBuffer & in_, const Block & header_, Params params_,  const FormatSettings & format_settings_);

    String getName() const override { return "RegexpRowInputFormat"; }

    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;

private:
    enum class FieldFormat
    {
        Escaped,
        Quoted,
        Csv,
        Json,
    };

    bool readField(size_t index, MutableColumns & columns);
    void readFieldsFromMatch(MutableColumns & columns, RowReadExtension & ext);
    FieldFormat stringToFormat(const String & format);

    const FormatSettings format_settings;
    std::regex regexp;
    std::match_results<char *> matched_fields;
    FieldFormat field_format;
};

}
