#pragma once

#include <Core/Block.h>
#include <IO/Progress.h>
#include <IO/WriteBuffer.h>
#include <Common/Stopwatch.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IRowOutputFormat.h>


namespace DB
{

/** A stream for outputting data in XML format.
  */
class XMLRowOutputFormat final : public IRowOutputFormat
{
public:
    XMLRowOutputFormat(WriteBuffer & out_, const Block & header_, const RowOutputFormatParams & params_, const FormatSettings & format_settings_);

    String getName() const override { return "XMLRowOutputFormat"; }

private:
    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writePrefix() override;
    void writeSuffix() override;
    void finalizeImpl() override;

    void writeMinExtreme(const Columns & columns, size_t row_num) override;
    void writeMaxExtreme(const Columns & columns, size_t row_num) override;
    void writeTotals(const Columns & columns, size_t row_num) override;

    void writeBeforeTotals() override;
    void writeAfterTotals() override;
    void writeBeforeExtremes() override;
    void writeAfterExtremes() override;

    void flush() override
    {
        ostr->next();

        if (validating_ostr)
            out.next();
    }

    void finalizeBuffers() override
    {
        if (validating_ostr)
            validating_ostr->finalize();
    }

    void setRowsBeforeLimit(size_t rows_before_limit_) override
    {
        statistics.applied_limit = true;
        statistics.rows_before_limit = rows_before_limit_;
    }

    void onRowsReadBeforeUpdate() override { row_count = getRowsReadBefore(); }

    void onProgress(const Progress & value) override;

    String getContentType() const override { return "application/xml; charset=UTF-8"; }

    void writeExtremesElement(const char * title, const Columns & columns, size_t row_num);
    void writeRowsBeforeLimitAtLeast();
    void writeStatistics();

    std::unique_ptr<WriteBuffer> validating_ostr;    /// Validates UTF-8 sequences, replaces bad sequences with replacement character.
    WriteBuffer * ostr;

    size_t field_number = 0;
    size_t row_count = 0;
    NamesAndTypes fields;
    Names field_tag_names;

    Statistics statistics;
    const FormatSettings format_settings;
};

}
