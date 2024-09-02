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
    using EscapingRule = FormatSettings::EscapingRule;
public:
    TemplateBlockOutputFormat(const Block & header_, WriteBuffer & out_, const FormatSettings & settings_,
                              ParsedTemplateFormatString format_, ParsedTemplateFormatString row_format_,
                              std::string row_between_delimiter_);

    String getName() const override { return "TemplateBlockOutputFormat"; }

    void setRowsBeforeLimit(size_t rows_before_limit_) override { statistics.rows_before_limit = rows_before_limit_; statistics.applied_limit = true; }
    void setRowsBeforeAggregation(size_t rows_before_aggregation_) override
    {
        statistics.rows_before_aggregation = rows_before_aggregation_;
        statistics.applied_aggregation = true;
    }
    void onProgress(const Progress & progress_) override { statistics.progress.incrementPiecewiseAtomically(progress_); }

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
        BytesRead,
        RowsBeforeAggregation
    };

    static ResultsetPart stringToResultsetPart(const String & part);

private:
    void writePrefix() override;
    void consume(Chunk chunk) override;
    void consumeTotals(Chunk chunk) override { statistics.totals = std::move(chunk); }
    void consumeExtremes(Chunk chunk) override { statistics.extremes = std::move(chunk); }
    void finalizeImpl() override;
    void resetFormatterImpl() override;

    void writeRow(const Chunk & chunk, size_t row_num);
    template <typename U, typename V> void writeValue(U value, EscapingRule escaping_rule);

    void onRowsReadBeforeUpdate() override { row_count = getRowsReadBefore(); }
    bool areTotalsAndExtremesUsedInFinalize() const override { return true; }

    const FormatSettings settings;
    Serializations serializations;

    ParsedTemplateFormatString format;
    ParsedTemplateFormatString row_format;

    size_t row_count = 0;

    std::string row_between_delimiter;
};

}
