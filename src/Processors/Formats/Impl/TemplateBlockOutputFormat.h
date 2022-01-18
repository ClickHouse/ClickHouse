#pragma once

#include <Common/Stopwatch.h>
#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Formats/ParsedTemplateFormatString.h>


namespace DB
{

class TemplateBlockOutputFormat : public IOutputFormat
{
    using ColumnFormat = ParsedTemplateFormatString::ColumnFormat;
public:
    TemplateBlockOutputFormat(const Block & header_, WriteBuffer & out_, const FormatSettings & settings_,
                              ParsedTemplateFormatString format_, ParsedTemplateFormatString row_format_,
                              std::string row_between_delimiter_);

    String getName() const override { return "TemplateBlockOutputFormat"; }

    void doWritePrefix() override;

    void setRowsBeforeLimit(size_t rows_before_limit_) override { rows_before_limit = rows_before_limit_; rows_before_limit_set = true; }
    void onProgress(const Progress & progress_) override { progress.incrementPiecewiseAtomically(progress_); }

    enum class ResultsetPart : size_t
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

    static ResultsetPart stringToResultsetPart(const String & part);

protected:
    void consume(Chunk chunk) override;
    void consumeTotals(Chunk chunk) override { totals = std::move(chunk); }
    void consumeExtremes(Chunk chunk) override { extremes = std::move(chunk); }
    void finalize() override;

    void writeRow(const Chunk & chunk, size_t row_num);
    void serializeField(const IColumn & column, const ISerialization & serialization, size_t row_num, ColumnFormat format);
    template <typename U, typename V> void writeValue(U value, ColumnFormat col_format);

protected:
    const FormatSettings settings;
    Serializations serializations;

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

    std::string row_between_delimiter;
};

}
