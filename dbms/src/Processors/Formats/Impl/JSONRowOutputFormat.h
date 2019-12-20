#pragma once

#include <Core/Block.h>
#include <IO/Progress.h>
#include <IO/WriteBuffer.h>
#include <Common/Stopwatch.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Formats/FormatSettings.h>


namespace DB
{

/** Stream for output data in JSON format.
  */
class JSONRowOutputFormat : public IRowOutputFormat
{
public:
    JSONRowOutputFormat(WriteBuffer & out_, const Block & header, FormatFactory::WriteCallback callback, const FormatSettings & settings_);

    String getName() const override { return "JSONRowOutputFormat"; }

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writeRowBetweenDelimiter() override;
    void writePrefix() override;
    void writeSuffix() override;

    void writeMinExtreme(const Columns & columns, size_t row_num) override;
    void writeMaxExtreme(const Columns & columns, size_t row_num) override;
    void writeTotals(const Columns & columns, size_t row_num) override;

    void writeBeforeTotals() override;
    void writeAfterTotals() override;
    void writeBeforeExtremes() override;
    void writeAfterExtremes() override;

    void writeLastSuffix() override;

    void flush() override
    {
        ostr->next();

        if (validating_ostr)
            out.next();
    }

    void setRowsBeforeLimit(size_t rows_before_limit_) override
    {
        applied_limit = true;
        rows_before_limit = rows_before_limit_;
    }

    void onProgress(const Progress & value) override;

    String getContentType() const override { return "application/json; charset=UTF-8"; }

protected:
    virtual void writeTotalsField(const IColumn & column, const IDataType & type, size_t row_num);
    virtual void writeExtremesElement(const char * title, const Columns & columns, size_t row_num);
    virtual void writeTotalsFieldDelimiter() { writeFieldDelimiter(); }

    void writeRowsBeforeLimitAtLeast();
    void writeStatistics();


    std::unique_ptr<WriteBuffer> validating_ostr;    /// Validates UTF-8 sequences, replaces bad sequences with replacement character.
    WriteBuffer * ostr;

    size_t field_number = 0;
    size_t row_count = 0;
    bool applied_limit = false;
    size_t rows_before_limit = 0;
    NamesAndTypes fields;

    Progress progress;
    Stopwatch watch;
    FormatSettings settings;
};

}
