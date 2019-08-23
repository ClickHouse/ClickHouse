#pragma once

#include <Common/Stopwatch.h>
#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IOutputFormat.h>


namespace DB
{

struct ParsedTemplateFormat
{
    enum class ColumnFormat
    {
        Default,
        Escaped,
        Quoted,
        Csv,
        Json,
        Xml,
        Raw
    };
    std::vector<String> delimiters;
    std::vector<ColumnFormat> formats;
    std::vector<size_t> format_idx_to_column_idx;

    typedef std::function<size_t(const String &)> ColumnIdxGetter;

    ParsedTemplateFormat() = default;
    ParsedTemplateFormat(const String & format_string, const ColumnIdxGetter & idxByName);
    static ColumnFormat stringToFormat(const String & format);
    static String formatToString(ColumnFormat format);
    size_t columnsCount() const;
};

class TemplateBlockOutputFormat : public IOutputFormat
{
    using ColumnFormat = ParsedTemplateFormat::ColumnFormat;
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

    ParsedTemplateFormat format;
    ParsedTemplateFormat row_format;

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
