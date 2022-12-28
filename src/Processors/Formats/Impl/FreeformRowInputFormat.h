#pragma once

#include "DataTypes/IDataType.h"
#include "DataTypes/Serializations/SerializationInfo.h"
#include "Formats/EscapingRuleUtils.h"
#include "Formats/FormatSettings.h"
#include "Formats/SchemaInferenceUtils.h"
#include "Processors/Formats/IRowInputFormat.h"
#include "Processors/Formats/ISchemaReader.h"

namespace DB
{

class FieldMatcher
{
public:
    explicit FieldMatcher(const FormatSettings::EscapingRule & rule_, const FormatSettings & settings_) : rule(rule_), settings(settings_)
    {
    }

    virtual String getName() const = 0;
    // parseFields returns a vector of fields (field_name, field_value), index is used as the column name if there isn't a better option
    virtual std::vector<std::pair<String, String>> parseFields(ReadBuffer & in, size_t index) const = 0;
    DataTypePtr getDataTypeFromField(const String & s) { return tryInferDataTypeByEscapingRule(s, settings, rule, &json_inference_info); }
    const FormatSettings::EscapingRule & getEscapingRule() const { return rule; }

    virtual ~FieldMatcher() = default;

protected:
    FormatSettings::EscapingRule rule;
    FormatSettings settings;
    JSONInferenceInfo json_inference_info;
};

class JSONFieldMatcher : public FieldMatcher
{
public:
    using FieldMatcher::FieldMatcher;
    String getName() const override { return "JSONFieldMatcher"; }
    std::vector<std::pair<String, String>> parseFields(ReadBuffer & in, size_t index) const override;
};

class CSVFieldMatcher : public FieldMatcher
{
public:
    using FieldMatcher::FieldMatcher;
    String getName() const override { return "CSVFieldMatcher"; }
    std::vector<std::pair<String, String>> parseFields(ReadBuffer & in, size_t index) const override;
};

class QuotedFieldMatcher : public FieldMatcher
{
public:
    using FieldMatcher::FieldMatcher;
    String getName() const override { return "QuotedFieldMatcher"; }
    std::vector<std::pair<String, String>> parseFields(ReadBuffer & in, size_t index) const override;
};

class EscapedFieldMatcher : public FieldMatcher
{
public:
    using FieldMatcher::FieldMatcher;
    String getName() const override { return "EscapedFieldMatcher"; }
    std::vector<std::pair<String, String>> parseFields(ReadBuffer & in, size_t index) const override;
};

class RawByWhitespaceFieldMatcher : public FieldMatcher
{
public:
    using FieldMatcher::FieldMatcher;
    String getName() const override { return "RawByWhitespaceFieldMatcher"; }
    std::vector<std::pair<String, String>> parseFields(ReadBuffer & in, size_t index) const override;
};

/// Class for matching generic data row by row.
/// Currently supported JSON, CSV, Quoted, Escaped and Raw.
class FreeformFieldMatcher
{
public:
    using FieldMatcherPtr = std::unique_ptr<FieldMatcher>;

    struct Fields
    {
        const NamesAndTypes columns;
        const uint8_t matcher_index;
        const size_t score;
        const bool parse_the_rest_as_one_string;
        char * pos;
        Fields(const NamesAndTypes & columns_, const uint8_t & index_, const size_t & score_, char * pos_, const bool & one_string)
            : columns(columns_), matcher_index(index_), score(score_), parse_the_rest_as_one_string(one_string), pos(pos_)
        {
        }
    };

    struct Solution
    {
        NamesAndTypes columns;
        std::vector<uint8_t> matchers_order;
        size_t score;
        size_t size;
    };

    explicit FreeformFieldMatcher(ReadBuffer & in, const FormatSettings & settings);
    // iterates over max_rows_to_read and pick the solution with the highest score. Returns false if no solution is found.
    bool generateSolutionsAndPickBest();
    // parse the row based on solution, generateSolutionsAndPickBest() must be called prior or it will throw an exception
    bool parseRow();

    const String & getField(size_t index) { return matched_fields[index]; }
    const FormatSettings::EscapingRule & getRule(size_t index) { return rules[index]; }
    NamesAndTypes & getNamesAndTypes() { return final_solution.columns; }
    size_t getSolutionLength() const { return final_solution.size; }

private:
    std::vector<FieldMatcherPtr> matchers;
    std::vector<FormatSettings::EscapingRule> rules;
    Solution final_solution;

    std::vector<String> matched_fields;
    std::unordered_map<String, size_t> field_name_to_index;
    bool first_row = true;

    const FormatSettings format_settings;
    size_t max_rows_to_check;
    ReadBuffer & in;

    void recursivelyGetNextFieldInRow(char * current_pos, Solution current_solution, std::vector<Solution> & solutions, bool one_string);
    void readRowAndGenerateSolutions(char * pos, std::vector<Solution> & solutions);
    std::vector<Fields> readNextFields(bool one_string, size_t index);
};

class FreeformRowInputFormat final : public IRowInputFormat
{
public:
    FreeformRowInputFormat(ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_);

    String getName() const override { return "FreeformRowInputFormat"; }

private:
    const FormatSettings format_settings;
    FreeformFieldMatcher matcher;

    bool readField(size_t index, MutableColumns & columns);
    bool readRow(MutableColumns &, RowReadExtension &) override;
    void syncAfterError() override;
};

class FreeformSchemaReader : public IRowSchemaReader
{
public:
    FreeformSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);
    // readSchema initiates the simultaneous iterations on multiple lines and pick the best solution
    NamesAndTypesList readSchema() override;

private:
    FreeformFieldMatcher matcher;

    DataTypes readRowAndGetDataTypes() override;
};

}
