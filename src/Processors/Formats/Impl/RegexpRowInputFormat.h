#pragma once

#include <re2_st/re2.h>
#include <re2_st/stringpiece.h>
#include <string>
#include <vector>
#include <Core/Block.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>
#include <Formats/FormatFactory.h>
#include <IO/PeekableReadBuffer.h>
#include <Formats/ParsedTemplateFormatString.h>


namespace DB
{

class ReadBuffer;

/// Class for extracting row fields from data by regexp.
class RegexpFieldExtractor
{
public:
    explicit RegexpFieldExtractor(const FormatSettings & format_settings);

    /// Return true if row was successfully parsed and row fields were extracted.
    bool parseRow(PeekableReadBuffer & buf);

    re2_st::StringPiece getField(size_t index) { return matched_fields[index]; }
    size_t getMatchedFieldsSize() const { return matched_fields.size(); }
    size_t getNumberOfGroups() const { return regexp.NumberOfCapturingGroups(); }

private:
    const re2_st::RE2 regexp;
    // The vector of fields extracted from line using regexp.
    std::vector<re2_st::StringPiece> matched_fields;
    // These two vectors are needed to use RE2::FullMatchN (function for extracting fields).
    std::vector<re2_st::RE2::Arg> re2_arguments;
    std::vector<re2_st::RE2::Arg *> re2_arguments_ptrs;
    bool skip_unmatched;
};

/// Regexp input format.
/// This format applies regular expression from format_regexp setting for every line of file
/// (the lines must be separated by newline character ('\n') or DOS-style newline ("\r\n")).
/// Every matched subpattern will be parsed with the method of corresponding data type
/// (according to format_regexp_escaping_rule setting). If the regexp did not match the line,
/// if format_regexp_skip_unmatched is 1, the line is silently skipped, if the setting is 0, exception will be thrown.

class RegexpRowInputFormat final : public IRowInputFormat
{
public:
    RegexpRowInputFormat(ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_);

    String getName() const override { return "RegexpRowInputFormat"; }
    void resetParser() override;
    void setReadBuffer(ReadBuffer & in_) override;

private:
    RegexpRowInputFormat(std::unique_ptr<PeekableReadBuffer> buf_, const Block & header_, Params params_, const FormatSettings & format_settings_);

    using EscapingRule = FormatSettings::EscapingRule;

    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;

    bool readField(size_t index, MutableColumns & columns);
    void readFieldsFromMatch(MutableColumns & columns, RowReadExtension & ext);

    std::unique_ptr<PeekableReadBuffer> buf;
    const FormatSettings format_settings;
    const EscapingRule escaping_rule;
    RegexpFieldExtractor field_extractor;
};

class RegexpSchemaReader : public IRowSchemaReader
{
public:
    RegexpSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings);

private:
    DataTypes readRowAndGetDataTypes() override;

    void transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type, size_t) override;


    using EscapingRule = FormatSettings::EscapingRule;
    RegexpFieldExtractor field_extractor;
    PeekableReadBuffer buf;
};

}
