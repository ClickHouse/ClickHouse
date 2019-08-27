#pragma once

#include <Common/Stopwatch.h>
#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IOutputFormat.h>


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
    /// If format_idx_to_column_idx[i] has no value, then TemplateRowInputStream will skip i-th column.

    std::vector<String> delimiters;
    std::vector<ColumnFormat> formats;
    std::vector<std::optional<size_t>> format_idx_to_column_idx;

    typedef std::function<std::optional<size_t>(const String &)> ColumnIdxGetter;

    ParsedTemplateFormatString() = default;
    ParsedTemplateFormatString(const String & format_string, const ColumnIdxGetter & idxByName);
    static ColumnFormat stringToFormat(const String & format);
    static String formatToString(ColumnFormat format);
    static const char * readMayBeQuotedColumnNameInto(const char * pos, size_t size, String & s);
    size_t columnsCount() const;
};

class TemplateBlockOutputFormat : public IOutputFormat
{
    using ColumnFormat = ParsedTemplateFormatString::ColumnFormat;
public:
    TemplateBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & settings_);

    String getName() const override { return "TemplateBlockOutputFormat"; }

    void doWritePrefix() override;

    void setRowsBeforeLimit(size_t rows_before_limit_) override { rows_before_limit = rows_before_limit_; rows_before_limit_set = true; }
    void onProgress(const Progress & progress_) override { progress.incrementPiecewiseAtomically(progress_); }

protected:
    void consume(Chunk chunk) override;
    void consumeTotals(Chunk chunk) override { totals = std::move(chunk); }
    void consumeExtremes(Chunk chunk) override { extremes = std::move(chunk); }
    void finalize() override;

    enum class OutputPart : size_t
    {
        Data,
        Totals,
        ExtremesMin,
        ExtremesMax,
        Rows,
        RowsBeforeLimit,
        TimeElapsed,
        RowsRead,
        BytesRead
    };

    OutputPart stringToOutputPart(const String & part);
    void writeRow(const Chunk & chunk, size_t row_num);
    void serializeField(const IColumn & column, const IDataType & type, size_t row_num, ColumnFormat format);
    template <typename U, typename V> void writeValue(U value, ColumnFormat col_format);

protected:
    const FormatSettings settings;
    DataTypes types;

    ParsedTemplateFormatString format;
    ParsedTemplateFormatString row_format;

    size_t rows_before_limit = 0;
    bool rows_before_limit_set = false;
    Chunk totals;
    Chunk extremes;
    Progress progress;
    Stopwatch watch;

    size_t row_count = 0;
    bool need_write_prefix = true;
};

}
