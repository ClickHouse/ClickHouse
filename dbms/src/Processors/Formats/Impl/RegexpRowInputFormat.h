#pragma once

#include <re2/re2.h>
#include <string>
#include <vector>
#include <Core/Block.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Formats/FormatSettings.h>
#include <Formats/FormatFactory.h>
#include <IO/PeekableReadBuffer.h>

namespace DB
{

class ReadBuffer;


class RegexpRowInputFormat : public IRowInputFormat
{
public:
    RegexpRowInputFormat(ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_);

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

    PeekableReadBuffer buf;
    const FormatSettings format_settings;
    FieldFormat field_format;

    RE2 regexp;
    // The vector of fields extracted from line using regexp.
    std::vector<std::string> matched_fields;
    // These two vectors are needed to use RE2::FullMatchN (function for extracting fields).
    std::vector<RE2::Arg> re2_arguments;
    std::vector<RE2::Arg *> re2_arguments_ptrs;
};

}
