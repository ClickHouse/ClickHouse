#pragma once

#include <Core/NamesAndTypes.h>
#include <Formats/FormatSettings.h>
#include <IO/Progress.h>
#include <IO/WriteBuffer.h>
#include <Processors/Formats/OutputFormatWithUTF8ValidationAdaptor.h>
#include <Processors/Formats/RowOutputFormatWithExceptionHandlerAdaptor.h>


namespace DB
{

/** A stream for outputting data in XML format.
  */
class XMLRowOutputFormat final : public RowOutputFormatWithExceptionHandlerAdaptor<RowOutputFormatWithUTF8ValidationAdaptor, bool>
{
public:
    XMLRowOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_);

    String getName() const override { return "XMLRowOutputFormat"; }

private:
    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writePrefix() override;
    void writeSuffix() override;
    void finalizeImpl() override;
    void resetFormatterImpl() override;

    void writeMinExtreme(const Columns & columns, size_t row_num) override;
    void writeMaxExtreme(const Columns & columns, size_t row_num) override;
    void writeTotals(const Columns & columns, size_t row_num) override;

    bool supportTotals() const override { return true; }
    bool supportExtremes() const override { return true; }

    void writeBeforeTotals() override;
    void writeAfterTotals() override;
    void writeBeforeExtremes() override;
    void writeAfterExtremes() override;

    void setRowsBeforeLimit(size_t rows_before_limit_) override
    {
        statistics.applied_limit = true;
        statistics.rows_before_limit = rows_before_limit_;
    }

    void setRowsBeforeAggregation(size_t rows_before_aggregation_) override
    {
        statistics.applied_aggregation = true;
        statistics.rows_before_aggregation = rows_before_aggregation_;
    }
    void onRowsReadBeforeUpdate() override { row_count = getRowsReadBefore(); }

    void writeExtremesElement(const char * title, const Columns & columns, size_t row_num);
    void writeRowsBeforeLimitAtLeast();
    void writeRowsBeforeAggregationAtLeast();
    void writeStatistics();
    void writeException();

    size_t field_number = 0;
    size_t row_count = 0;
    NamesAndTypes fields;
    Names field_tag_names;

    const FormatSettings format_settings;
    WriteBuffer * ostr;
};

}
