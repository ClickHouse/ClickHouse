#pragma once

#include <optional>
#include "Core/NamesAndTypes.h"
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
        settings.try_infer_integers = true;
    }

    struct Result
    {
        const NameAndTypePair name_and_type;
        const String field;
        const size_t score = 0;
        const size_t type_score = 0;
        const size_t offset;
        const bool ok = true;
        const bool parse_till_newline_as_one_string = false;
    };

    Result parseField(PeekableReadBuffer & in, size_t index);
    const FormatSettings::EscapingRule & getEscapingRule() const { return rule; }
    void transformTypesIfPossible(DataTypePtr & first, DataTypePtr & second)
    {
        transformInferredTypesByEscapingRuleIfNeeded(first, second, settings, rule, &json_inference_info);
    }

    virtual String getName() const = 0;
    virtual ~FieldMatcher() = default;

protected:
    virtual String readFieldByEscapingRule(PeekableReadBuffer & in) const = 0;
    Result generateResult(String field, size_t offset, size_t index);
    DataTypePtr getDataTypeFromField(const String & s) { return tryInferDataTypeByEscapingRule(s, settings, rule, &json_inference_info); }

    FormatSettings::EscapingRule rule;
    FormatSettings settings;
    JSONInferenceInfo json_inference_info;
};

class JSONFieldMatcher : public FieldMatcher
{
public:
    using FieldMatcher::FieldMatcher;
    String getName() const override { return "JSONFieldMatcher"; }
    String readFieldByEscapingRule(PeekableReadBuffer & in) const override;
};

class CSVFieldMatcher : public FieldMatcher
{
public:
    using FieldMatcher::FieldMatcher;
    String getName() const override { return "CSVFieldMatcher"; }
    String readFieldByEscapingRule(PeekableReadBuffer & in) const override;
};

class QuotedFieldMatcher : public FieldMatcher
{
public:
    using FieldMatcher::FieldMatcher;
    String getName() const override { return "QuotedFieldMatcher"; }
    String readFieldByEscapingRule(PeekableReadBuffer & in) const override;
};

class EscapedFieldMatcher : public FieldMatcher
{
public:
    using FieldMatcher::FieldMatcher;
    String getName() const override { return "EscapedFieldMatcher"; }
    String readFieldByEscapingRule(PeekableReadBuffer & in) const override;
};

class RawByWhitespaceFieldMatcher : public FieldMatcher
{
public:
    using FieldMatcher::FieldMatcher;
    String getName() const override { return "RawByWhitespaceFieldMatcher"; }
    String readFieldByEscapingRule(PeekableReadBuffer & in) const override;
};

/// Class for matching generic data row by row.
/// Currently supported JSON, CSV, Quoted, Escaped and Raw.
class FreeformFieldMatcher
{
public:
    using FieldMatcherPtr = std::unique_ptr<FieldMatcher>;

    struct Fields
    {
        const FieldMatcher::Result parse_result;
        const uint8_t matcher_index;
        Fields(const FieldMatcher::Result parse_result_, const uint8_t index_) : parse_result(parse_result_), matcher_index(index_) { }
    };

    struct Solution
    {
        mutable NamesAndTypes columns;
        std::vector<uint8_t> matchers_order;
        size_t score;
        size_t size;
    };

    explicit FreeformFieldMatcher(ReadBuffer & in_, const FormatSettings & settings);
    // iterates over max_rows_to_read and pick the solution with the highest score. Returns false if no solution is found.
    bool buildSolutionsAndPickBest();
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

    // for now it's min(100, settings_.max_rows_to_read_for_schema_inference) to keep it fast
    // we could reconsider using settings_.max_rows_to_read_for_schema_inference once we are able to store solutions
    size_t max_rows_to_check;
    std::unique_ptr<PeekableReadBuffer> in;

    void buildSolutions(Solution current_solution, std::vector<Solution> & solutions, bool one_string) const;
    bool validateSolution(Solution solution) const;
    std::vector<Fields> readNextFields(bool one_string, size_t index) const;
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
