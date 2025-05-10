#pragma once

#include <memory>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/Progress.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Processors/Formats/Impl/CSVRowOutputFormat.h>
#include <Processors/Formats/Impl/JSONEachRowRowOutputFormat.h>
#include <Processors/Formats/Impl/JSONEachRowWithProgressRowOutputFormat.h>
#include <Processors/Port.h>


namespace DB
{

template <typename Format, bool support_progress>
class SSEFormat : public IRowOutputFormat
{
public:
    SSEFormat(WriteBuffer & out_, const Block & header_, const FormatSettings &)
        : IRowOutputFormat(header_, out_)
    {
    }

    String getName() const override { return formatter->getName() + "EventStream"; }

    std::string getContentType() const override { return "text/event-stream"; }

protected:
    bool supportTotals() const override { return support_progress; }
    bool supportExtremes() const override { return support_progress; }

    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override
    {
        formatter->writeField(column, serialization, row_num);
    }

    void writeFieldDelimiter() override { formatter->writeFieldDelimiter(); }

    void writeRowStartDelimiter() override
    {
        buffer.restart();
        writeCString("event: results\n", buffer);
        writeCString("data: ", buffer);
        formatter->writeRowStartDelimiter();
    }

    void writeRowEndDelimiter() override
    {
        formatter->writeRowEndDelimiter();
        writeCString("\n", buffer);
        flushBuffer(false);
    }
    void writeRowBetweenDelimiter() override { formatter->writeRowBetweenDelimiter(); }

    void writePrefix() override
    {
        buffer.restart();
        writeCString("event: results\n", buffer);
        writeCString("data: ", buffer);
        auto len_before = buffer.stringView().length();
        formatter->writePrefix();
        auto len_after = buffer.stringView().length();
        writeCString("\n", buffer);
        flushBuffer(len_before == len_after);
    }

    void writeSuffix() override
    {
        buffer.restart();
        writeCString("event: results\n", buffer);
        writeCString("data: ", buffer);
        auto len_before = buffer.stringView().length();
        formatter->writeSuffix();
        auto len_after = buffer.stringView().length();
        writeCString("\n", buffer);
        flushBuffer(len_before == len_after);
    }

    bool writesProgressConcurrently() const override { return support_progress; }

    void writeProgress(const Progress & value) override
    {
        if (!support_progress)
        {
            return;
        }

        if (value.empty())
        {
            return;
        }

        buffer.restart();
        writeCString("event: progress\n", buffer);
        writeCString("data: ", buffer);
        formatter->writeProgress(value);
        writeCString("\n", buffer);
        flushBuffer(false);
    }

    void writeMinExtreme(const Columns & columns, size_t row_num) override
    {
        if (!support_progress)
        {
            return;
        }

        buffer.restart();
        writeCString("event: results\n", buffer);
        writeCString("data: ", buffer);
        auto len_before = buffer.stringView().length();
        formatter->writeMinExtreme(columns, row_num);
        auto len_after = buffer.stringView().length();
        writeCString("\n", buffer);
        flushBuffer(len_after == len_before);
    }

    void writeMaxExtreme(const Columns & columns, size_t row_num) override
    {
        if (!support_progress)
        {
            return;
        }

        buffer.restart();
        writeCString("event: results\n", buffer);
        writeCString("data: ", buffer);
        auto len_before = buffer.stringView().length();
        formatter->writeMaxExtreme(columns, row_num);
        auto len_after = buffer.stringView().length();
        writeCString("\n", buffer);
        flushBuffer(len_after == len_before);
    }

    void writeTotals(const Columns & columns, size_t row_num) override
    {
        if (!support_progress)
        {
            return;
        }

        buffer.restart();
        writeCString("event: results\n", buffer);
        writeCString("data: ", buffer);
        auto len_before = buffer.stringView().length();
        formatter->writeTotals(columns, row_num);
        auto len_after = buffer.stringView().length();
        writeCString("\n", buffer);
        flushBuffer(len_after == len_before);
    }

    void finalizeImpl() override
    {
        buffer.restart();
        writeCString("event: results\n", buffer);
        writeCString("data: ", buffer);
        auto len_before = buffer.stringView().length();
        formatter->finalizeImpl();
        auto len_after = buffer.stringView().length();
        writeCString("\n", buffer);
        flushBuffer(len_after == len_before);
    }

    void resetFormatterImpl() override { formatter->resetFormatterImpl(); }

    void setRowsBeforeLimit(size_t rows_before_limit_) override { formatter->setRowsBeforeLimit(rows_before_limit_); }

    void setRowsBeforeAggregation(size_t rows_before_aggregation_) override
    {
        formatter->setRowsBeforeAggregation(rows_before_aggregation_);
    }

private:
    void flushBuffer(bool is_empty)
    {
        auto & res = buffer.str();
        if (!is_empty)
        {
            out.write(res.data(), res.size());
            out.next();
        }

        formatter->flush();
        buffer.restart();
    }

protected:
    WriteBufferFromOwnString buffer;
    std::shared_ptr<Format> formatter;
};


class SSEFormatJSON : public SSEFormat<JSONEachRowRowOutputFormat, false>
{
public:
    SSEFormatJSON(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_)
        : SSEFormat<JSONEachRowRowOutputFormat, false>(out_, header_, format_settings_)
    {
        formatter = std::make_shared<JSONEachRowRowOutputFormat>(buffer, header_, format_settings_);
    }
};

class SSEFormatJSONWithProgress : public SSEFormat<JSONEachRowWithProgressRowOutputFormat, true>
{
public:
    SSEFormatJSONWithProgress(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_)
        : SSEFormat<JSONEachRowWithProgressRowOutputFormat, true>(out_, header_, format_settings_)
    {
        formatter = std::make_shared<JSONEachRowWithProgressRowOutputFormat>(buffer, header_, format_settings_);
    }
};

class SSEFormatCSV : public SSEFormat<CSVRowOutputFormat, false>
{
public:
    SSEFormatCSV(WriteBuffer & out_, const Block & header_, bool with_names_, bool with_types_, const FormatSettings & format_settings_)
        : SSEFormat<CSVRowOutputFormat, false>(out_, header_, format_settings_)
    {
        formatter = std::make_shared<CSVRowOutputFormat>(buffer, header_, with_names_, with_types_, format_settings_);
    }
};

}
