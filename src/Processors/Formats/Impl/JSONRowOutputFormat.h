#pragma once

#include <Core/Block.h>
#include <IO/Progress.h>
#include <IO/WriteBuffer.h>
#include <Common/Stopwatch.h>
#include <Processors/Formats/OutputFormatWithUTF8ValidationAdaptor.h>
#include <Formats/FormatSettings.h>


namespace DB
{

/** Stream for output data in JSON format.
  */
class JSONRowOutputFormat : public RowOutputFormatWithUTF8ValidationAdaptor
{
public:
    JSONRowOutputFormat(
        WriteBuffer & out_,
        const Block & header,
        const RowOutputFormatParams & params_,
        const FormatSettings & settings_,
        bool yield_strings_);

    String getName() const override { return "JSONRowOutputFormat"; }

    void onProgress(const Progress & value) override;

    String getContentType() const override { return "application/json; charset=UTF-8"; }

    void setRowsBeforeLimit(size_t rows_before_limit_) override
    {
        statistics.applied_limit = true;
        statistics.rows_before_limit = rows_before_limit_;
    }

protected:
    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writeRowBetweenDelimiter() override;
    void writePrefix() override;
    void writeSuffix() override;

    void writeMinExtreme(const Columns & columns, size_t row_num) override;
    void writeMaxExtreme(const Columns & columns, size_t row_num) override;
    void writeTotals(const Columns & columns, size_t row_num) override;

    bool supportTotals() const override { return true; }
    bool supportExtremes() const override { return true; }

    void writeBeforeTotals() override;
    void writeAfterTotals() override;
    void writeBeforeExtremes() override;
    void writeAfterExtremes() override;

    void finalizeImpl() override;

    virtual void writeExtremesElement(const char * title, const Columns & columns, size_t row_num);

    void onRowsReadBeforeUpdate() override { row_count = getRowsReadBefore(); }

    size_t field_number = 0;
    size_t row_count = 0;
    Names names;   /// The column names are pre-escaped to be put into JSON string literal.

    Statistics statistics;
    FormatSettings settings;

    bool yield_strings;
};

}
