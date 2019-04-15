#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <DataStreams/IBlockOutputStream.h>
#include <IO/Progress.h>
#include <Common/Stopwatch.h>


namespace DB
{

struct ParsedTemplateFormat
{
    enum class ColumnFormat
    {
        Default,
        Escaped,
        Quoted,
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

class TemplateBlockOutputStream : public IBlockOutputStream
{
    using ColumnFormat = ParsedTemplateFormat::ColumnFormat;
public:
    TemplateBlockOutputStream(WriteBuffer & ostr_, const Block & sample, const FormatSettings & settings_);
    Block getHeader() const override { return header; }

    void write(const Block & block) override;
    void writePrefix() override;
    void writeSuffix() override;

    void flush() override;

    void setRowsBeforeLimit(size_t rows_before_limit_) override { rows_before_limit = rows_before_limit_; }
    void setTotals(const Block & totals_) override { totals = totals_; }
    void setExtremes(const Block & extremes_) override { extremes = extremes_; }
    void onProgress(const Progress & progress_) override { progress.incrementPiecewiseAtomically(progress_); }

private:
    enum class OutputPart : size_t
    {
        Result,
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
    void writeRow(const Block & block, size_t row_num);
    void serializeField(const IColumn & column, const IDataType & type, size_t row_num, ColumnFormat format);
    template <typename U, typename V> void writeValue(U value, ColumnFormat col_format);

private:
    WriteBuffer & ostr;
    Block header;
    const FormatSettings settings;

    ParsedTemplateFormat format;
    ParsedTemplateFormat row_format;

    size_t rows_before_limit;
    Block totals;
    Block extremes;
    Progress progress;
    Stopwatch watch;

    size_t row_count = 0;
};

}
