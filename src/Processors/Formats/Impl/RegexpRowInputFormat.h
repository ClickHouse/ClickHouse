#pragma once

#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <string>
#include <vector>
#include <Core/Block.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Formats/FormatSettings.h>
#include <Formats/FormatFactory.h>
#include <IO/PeekableReadBuffer.h>
#include <Formats/ParsedTemplateFormatString.h>

namespace DB
{

class ReadBuffer;

/// Regexp input format.
/// This format applies regular expression from format_regexp setting for every line of file
/// (the lines must be separated by newline character ('\n') or DOS-style newline ("\r\n")).
/// Every matched subpattern will be parsed with the method of corresponding data type
/// (according to format_regexp_escaping_rule setting). If the regexp did not match the line,
/// if format_regexp_skip_unmatched is 1, the line is silently skipped, if the setting is 0, exception will be thrown.

class RegexpRowInputFormat : public IRowInputFormat
{
    using ColumnFormat = ParsedTemplateFormatString::ColumnFormat;
public:
    RegexpRowInputFormat(ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_);

    String getName() const override { return "RegexpRowInputFormat"; }

    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    void resetParser() override;

private:
    bool readField(size_t index, MutableColumns & columns);
    void readFieldsFromMatch(MutableColumns & columns, RowReadExtension & ext);
    static ColumnFormat stringToFormat(const String & format);

    PeekableReadBuffer buf;
    const FormatSettings format_settings;
    const ColumnFormat field_format;

    const RE2 regexp;
    // The vector of fields extracted from line using regexp.
    std::vector<re2::StringPiece> matched_fields;
    // These two vectors are needed to use RE2::FullMatchN (function for extracting fields).
    std::vector<RE2::Arg> re2_arguments;
    std::vector<RE2::Arg *> re2_arguments_ptrs;
};

}
