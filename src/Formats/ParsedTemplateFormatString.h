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
    enum class ColumnFormat
    {
        None,
        Escaped,
        Quoted,
        Csv,
        Json,
        Xml,
        Raw
    };

    /// Format string has syntax: "Delimiter0 ${ColumnName0:Format0} Delimiter1 ${ColumnName1:Format1} Delimiter2"
    /// The following vectors is filled with corresponding values, delimiters.size() - 1 = formats.size() = format_idx_to_column_idx.size()
    /// If format_idx_to_column_idx[i] has no value, then TemplateRowInputFormat will skip i-th column.

    std::vector<String> delimiters;
    std::vector<ColumnFormat> formats;
    std::vector<std::optional<size_t>> format_idx_to_column_idx;

    /// For diagnostic info
    Strings column_names;

    typedef std::function<std::optional<size_t>(const String &)> ColumnIdxGetter;

    ParsedTemplateFormatString() = default;
    ParsedTemplateFormatString(const FormatSchemaInfo & schema, const ColumnIdxGetter & idx_by_name);

    void parse(const String & format_string, const ColumnIdxGetter & idx_by_name);

    static ColumnFormat stringToFormat(const String & format);
    static String formatToString(ColumnFormat format);
    static const char * readMayBeQuotedColumnNameInto(const char * pos, size_t size, String & s);
    size_t columnsCount() const;

    String dump() const;
    [[noreturn]] void throwInvalidFormat(const String & message, size_t column) const;

    static ParsedTemplateFormatString setupCustomSeparatedResultsetFormat(const FormatSettings::Custom & settings);
    static ParsedTemplateFormatString setupCustomSeparatedRowFormat(const FormatSettings::Custom & settings, const Block & sample);
};

}

