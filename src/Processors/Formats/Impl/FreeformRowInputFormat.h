#pragma once

#include "DataTypes/IDataType.h"
#include "Formats/EscapingRuleUtils.h"
#include "Formats/FormatSettings.h"
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
    virtual void parseField(String & s, ReadBuffer & in) const = 0;
    DataTypePtr getTypeFromField(String & s) const { return determineDataTypeByEscapingRule(s, settings, rule); }
    const FormatSettings::EscapingRule & getEscapingRule() const { return rule; }

    virtual ~FieldMatcher() = default;

protected:
    FormatSettings::EscapingRule rule;
    FormatSettings settings;
};

class JSONFieldMatcher : public FieldMatcher
{
public:
    using FieldMatcher::FieldMatcher;
    String getName() const override { return "JSONFieldMatcher"; }
    void parseField(String & s, ReadBuffer & in) const override;
};

class CSVFieldMatcher : public FieldMatcher
{
public:
    using FieldMatcher::FieldMatcher;
    String getName() const override { return "CSVFieldMatcher"; }
    void parseField(String & s, ReadBuffer & in) const override;
};

class QuotedFieldMatcher : public FieldMatcher
{
public:
    using FieldMatcher::FieldMatcher;
    String getName() const override { return "QuotedFieldMatcher"; }
    void parseField(String & s, ReadBuffer & in) const override;
};

class EscapedFieldMatcher : public FieldMatcher
{
public:
    using FieldMatcher::FieldMatcher;
    String getName() const override { return "EscapedFieldMatcher"; }
    void parseField(String & s, ReadBuffer & in) const override;
};

class RawByWhitespaceFieldMatcher : public FieldMatcher
{
public:
    using FieldMatcher::FieldMatcher;
    String getName() const override { return "RawByWhitespaceFieldMatcher"; }
    void parseField(String & s, ReadBuffer & in) const override;
};

/// Class for matching generic data row by row.
/// Currently supported JSON, CSV, Quoted, Escaped and Raw.
class FreeformFieldMatcher
{
public:
    using FieldMatcherPtr = std::unique_ptr<FieldMatcher>;

    struct Field
    {
        const DataTypePtr type;
        const size_t matcher_index;
        const size_t score;
        char * pos;
        Field(const DataTypePtr & type_, const size_t & index_, const size_t & score_, char * pos_)
            : type(type_), matcher_index(index_), score(score_), pos(pos_)
        {
        }
    };

    struct Solution
    {
        DataTypes matched_types;
        std::vector<int> matchers_order;
        size_t score;
    };

    explicit FreeformFieldMatcher(ReadBuffer & in, const FormatSettings & settings);
    // iterates over max_rows_to_read and pick the solution with the highest score. Returns false if no solution is found.
    bool generateSolutionsAndPickBest();
    // parse the row based on solution, generateSolutionsAndPickBest() must be called prior or it will throw an exception
    bool parseRow();

    const String & getField(size_t index) { return matched_fields[index]; }
    FormatSettings::EscapingRule getRule(size_t index) { return matchers[final_solution.matchers_order[index]]->getEscapingRule(); }
    DataTypes & getDataTypes() { return final_solution.matched_types; }
    size_t getSolutionLength() const { return final_solution.matchers_order.size(); }

private:
    std::vector<FieldMatcherPtr> matchers;
    std::vector<String> matched_fields;
    Solution final_solution;

    const FormatSettings format_settings;
    int max_rows_to_check;
    ReadBuffer & in;

    void recursivelyGetNextFieldInRow(char * current_pos, Solution current_solution, std::vector<Solution> & solutions);
    void readRowAndGenerateSolutions(char * pos, std::vector<Solution> & solutions);
    std::vector<Field> readNextPossibleFields(const size_t & index);
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
