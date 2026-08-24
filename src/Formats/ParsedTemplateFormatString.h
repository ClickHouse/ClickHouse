#pragma once

#include <base/types.h>
#include <functional>
#include <optional>
#include <vector>
#include <Formats/FormatSchemaInfo.h>
#include <Formats/FormatSettings.h>

namespace DB
{

class Block;
using Strings = std::vector<String>;

struct ParsedTemplateFormatString
{
    using EscapingRule = FormatSettings::EscapingRule;

    /// Format string has syntax: "Delimiter0 ${ColumnName0:Format0} Delimiter1 ${ColumnName1:Format1} Delimiter2"
    /// The following vectors is filled with corresponding values, delimiters.size() - 1 = formats.size() = format_idx_to_column_idx.size()
    /// If format_idx_to_column_idx[i] has no value, then TemplateRowInputFormat will skip i-th column.

    std::vector<String> delimiters;
    std::vector<EscapingRule> escaping_rules;
    std::vector<std::optional<size_t>> format_idx_to_column_idx;

    /// For diagnostic info
    Strings column_names;

    using ColumnIdxGetter = std::function<std::optional<size_t>(const String &)>;

    ParsedTemplateFormatString() = default;
    ParsedTemplateFormatString(const FormatSchemaInfo & schema, const ColumnIdxGetter & idx_by_name, bool allow_indexes = true);

    void parse(const String & format_string, const ColumnIdxGetter & idx_by_name, bool allow_indexes = true);

    static const char * readMayBeQuotedColumnNameInto(const char * pos, size_t size, String & s);
    size_t columnsCount() const;

    String dump() const;
    [[noreturn]] void throwInvalidFormat(const String & message, size_t column) const;
};

}

