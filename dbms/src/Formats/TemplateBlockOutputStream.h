#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <DataStreams/IBlockOutputStream.h>
#include <IO/Progress.h>


namespace DB
{

class TemplateBlockOutputStream : public IBlockOutputStream
{
public:
    enum class ColumnFormat
    {
        Default,
        Escaped,
        Quoted,
        Json,
        Xml,
        Raw
    };

    TemplateBlockOutputStream(WriteBuffer & ostr_, const Block & sample, const FormatSettings & settings_, const String & format_template);
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
    ColumnFormat stringToFormat(const String & format);
    void parseFormatString(const String & s);
    void serializeField(const ColumnWithTypeAndName & col, size_t row_num, ColumnFormat format);

private:
    WriteBuffer & ostr;
    Block header;
    const FormatSettings settings;
    std::vector<String> delimiters;
    std::vector<ColumnFormat> formats;
    std::vector<size_t> format_idx_to_column_idx;

    size_t rows_before_limit;
    Block totals;
    Block extremes;
    Progress progress;
};

}
