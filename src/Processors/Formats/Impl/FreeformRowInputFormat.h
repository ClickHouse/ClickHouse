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

// Base class for FieldMatcher, inheriting classes needs to implement readFieldsByEscapingRule() and getName()
class FieldMatcher
{
public:
    explicit FieldMatcher(const FormatSettings::EscapingRule & rule_, const FormatSettings & settings_) : rule(rule_), settings(settings_)
    {
        settings.try_infer_integers = true;
    }

    struct Result
    {
        const NamesAndTypesList names_and_types;
        const std::vector<String> fields;
        const size_t score = 0;
        const size_t type_score = 0;
        const size_t offset;
        const bool ok = true;
        const bool parse_till_newline_as_one_string = false;
    };

    using NamesAndFields = std::vector<std::pair<String, String>>;

    // We only need the offset on the initial run to determine the schema, we don't need it for successive runs to parse fields.
    template <bool with_offset>
    // parseField is the general interface that other classes will use to parse/build solutions.
    Result parseField(PeekableReadBuffer & in, unsigned index);
    const FormatSettings::EscapingRule & getEscapingRule() const { return rule; }
    // transformTypesIfPossible attempts to find the union between &first and &second and then reassign both of them to this new type if it exists.
    void transformTypesIfPossible(DataTypePtr & first, DataTypePtr & second)
    {
        transformInferredTypesByEscapingRuleIfNeeded(first, second, settings, rule, &json_inference_info);
    }

    virtual String getName() const = 0;
    virtual ~FieldMatcher() = default;

protected:
    virtual NamesAndFields readFieldsByEscapingRule(PeekableReadBuffer & in, unsigned index) const = 0;
    Result generateResult(NamesAndFields & fields, size_t offset);
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
    std::vector<std::pair<String, String>> readFieldsByEscapingRule(PeekableReadBuffer & in, unsigned index) const override;
};

class CSVFieldMatcher : public FieldMatcher
{
public:
    using FieldMatcher::FieldMatcher;
    String getName() const override { return "CSVFieldMatcher"; }
    NamesAndFields readFieldsByEscapingRule(PeekableReadBuffer & in, unsigned index) const override;
};

class QuotedFieldMatcher : public FieldMatcher
{
public:
    using FieldMatcher::FieldMatcher;
    String getName() const override { return "QuotedFieldMatcher"; }
    NamesAndFields readFieldsByEscapingRule(PeekableReadBuffer & in, unsigned index) const override;
};

class EscapedFieldMatcher : public FieldMatcher
{
public:
    using FieldMatcher::FieldMatcher;
    String getName() const override { return "EscapedFieldMatcher"; }
    NamesAndFields readFieldsByEscapingRule(PeekableReadBuffer & in, unsigned index) const override;
};

class RawByWhitespaceFieldMatcher : public FieldMatcher
{
public:
    using FieldMatcher::FieldMatcher;
    String getName() const override { return "RawByWhitespaceFieldMatcher"; }
    NamesAndFields readFieldsByEscapingRule(PeekableReadBuffer & in, unsigned index) const override;
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
        unsigned size;
    };

    explicit FreeformFieldMatcher(ReadBuffer & in_, const FormatSettings & settings);
    // iterates over max_rows_to_read and pick the solution with the highest score. Returns false if no solution is found.
    bool buildSolutionsAndPickBest();
    // parse the row based on solution, buildSolutionsAndPickBest() must be called prior or it will throw an exception
    bool parseRow();

    const String & getField(unsigned index) { return matched_fields[index]; }
    const FormatSettings::EscapingRule & getRule(unsigned index) { return rules[index]; }
    NamesAndTypes & getNamesAndTypes() { return final_solution.columns; }
    unsigned getSolutionLength() const { return final_solution.size; }

private:
    std::vector<FieldMatcherPtr> matchers;
    std::vector<FormatSettings::EscapingRule> rules;
    Solution final_solution;

    std::vector<String> matched_fields;
    std::unordered_map<String, unsigned> field_name_to_index;
    bool first_row = true;

    // for now it's min(100, settings_.max_rows_to_read_for_schema_inference) to keep it fast
    // we could reconsider using settings_.max_rows_to_read_for_schema_inference once we are able to store solutions
    size_t max_rows_to_check;
    std::unique_ptr<PeekableReadBuffer> in;

    void buildSolutions(Solution current_solution, std::vector<Solution> & solutions, bool one_string) const;
    // validateSolution iterates over the current row and try to parse and infer the types of the parsed fields. A solution is valid when the parsed types are valid.
    bool validateSolution(Solution solution) const;
    // readNextFields iterates over the list of matchers and try to parse all the possible fields.
    std::vector<Fields> readNextFields(bool one_string, unsigned index) const;
};

class FreeformRowInputFormat final : public IRowInputFormat
{
public:
    FreeformRowInputFormat(ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_);

    String getName() const override { return "FreeformRowInputFormat"; }

private:
    const FormatSettings format_settings;
    FreeformFieldMatcher matcher;

    bool readField(unsigned index, MutableColumns & columns);
    bool readRow(MutableColumns &, RowReadExtension &) override;
    void syncAfterError() override;
};

class FreeformSchemaReader : public IRowSchemaReader
{
public:
    FreeformSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);
    // readSchema initiates the solutions building process, run them against 100 rows and pick the best solution according to the scoring criteria.
    NamesAndTypesList readSchema() override;

private:
    FreeformFieldMatcher matcher;
    DataTypes readRowAndGetDataTypes() override;
};

}
