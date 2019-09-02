#pragma once

#include <Core/Types.h>
#include <functional>
#include <optional>

namespace DB
{

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
    ParsedTemplateFormatString(const String & format_string, const ColumnIdxGetter & idx_by_name);

    void parse(const String & format_string, const ColumnIdxGetter & idx_by_name);

    ColumnFormat stringToFormat(const String & format) const;
    static String formatToString(ColumnFormat format);
    static const char * readMayBeQuotedColumnNameInto(const char * pos, size_t size, String & s);
    size_t columnsCount() const;

    String dump() const;
    [[noreturn]] void throwInvalidFormat(const String & message, size_t column) const;
};

}

