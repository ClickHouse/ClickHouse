#pragma once

#include <Processors/Formats/IRowOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Processors/Port.h>
#include <Processors/Formats/Impl/JSONEachRowRowOutputFormat.h>
#include "Formats/FormatSettings.h"
#include "IO/Progress.h"
#include "IO/WriteBufferFromString.h"
#include "IO/WriteHelpers.h"
#include "Processors/Formats/Impl/JSONEachRowWithProgressRowOutputFormat.h"


namespace DB 
{

template <typename Format, bool support_progress>
class SSEFormat : public IRowOutputFormat
{
public:
    SSEFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & fs) 
    : IRowOutputFormat(header_, out_)
    , json_format(json_buffer, getPort(IOutputFormat::PortKind::Main).getHeader(), fs)
    {
        json_format.auto_flush = false;
    }

    String getName() const override { return "SSEFormat"; }

    std::string getContentType() const override { return "text/event-stream"; }

protected:

    bool supportTotals() const override { return support_progress; }
    bool supportExtremes() const override { return support_progress; }

    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override 
    {
        json_format.writeField(column, serialization, row_num);
    }

    void writeFieldDelimiter() override 
    {
        json_format.writeFieldDelimiter();
    }

    void writeRowStartDelimiter() override 
    {
        json_buffer.restart();
        writeCString("event: results\n", json_buffer);
        writeCString("data: ", json_buffer);
        json_format.writeRowStartDelimiter();
    }

    void writeRowEndDelimiter() override 
    {
        json_format.writeRowEndDelimiter();
        writeCString("\n\n", json_buffer);
        flushBuffer();
    }
    void writeRowBetweenDelimiter() override 
    {
        json_format.writeRowBetweenDelimiter();
    }

    void writePrefix() override 
    {
        json_buffer.restart();
        writeCString("event: prefix\n", json_buffer);
        writeCString("data: ", json_buffer);
        json_format.writePrefix();
        writeCString("\n\n", json_buffer);
        flushBuffer();
    }

    void writeSuffix() override {
        json_buffer.restart();
        writeCString("event: suffix\n", json_buffer);
        writeCString("data: ", json_buffer);
        json_format.writeSuffix();
        writeCString("\n\n", json_buffer);
        flushBuffer();
    }

    bool writesProgressConcurrently() const override { return support_progress; }

    void writeProgress(const Progress & value) override 
    {
        if (!support_progress) {
            return;
        }

        if (value.empty()) 
        {
            return;
        }
    
        json_buffer.restart();
        writeCString("event: progress\n", json_buffer);
        writeCString("data: ", json_buffer);
        json_format.writeProgress(value);
        writeCString("\n\n", json_buffer);
        flushBuffer();
    }

    void writeMinExtreme(const Columns & columns, size_t row_num) override
    {
        if (!support_progress) {
            return;
        }

        json_buffer.restart();
        writeCString("event: min_extreme\n", json_buffer);
        writeCString("data: ", json_buffer);
        json_format.writeMinExtreme(columns,row_num);
        writeCString("\n\n", json_buffer);
        flushBuffer();
    }

    void writeMaxExtreme(const Columns & columns, size_t row_num) override
    {
        if (!support_progress) {
            return;
        }

        json_buffer.restart();
        writeCString("event: max_extreme\n", json_buffer);
        writeCString("data: ", json_buffer);
        json_format.writeMaxExtreme(columns,row_num);
        writeCString("\n\n", json_buffer);
        flushBuffer();
    }

    void writeTotals(const Columns & columns, size_t row_num) override
    {
        if (!support_progress) {
            return;
        }

        json_buffer.restart();
        writeCString("event: totals\n", json_buffer);
        writeCString("data: ", json_buffer);
        json_format.writeTotals(columns, row_num);
        writeCString("\n\n", json_buffer);
        flushBuffer();
    }

    void finalizeImpl() override
    {
        json_buffer.restart();
        writeCString("event: finalize\n", json_buffer);
        writeCString("data: ", json_buffer);
        json_format.finalizeImpl();
        writeCString("\n\n", json_buffer);
        flushBuffer();
    }

    void resetFormatterImpl() override
    {
        json_format.resetFormatterImpl();
    }

    void setRowsBeforeLimit(size_t rows_before_limit_) override
    {
        json_format.setRowsBeforeLimit(rows_before_limit_);
    }

    void setRowsBeforeAggregation(size_t rows_before_aggregation_) override
    {
        json_format.setRowsBeforeAggregation(rows_before_aggregation_);
    }

private:

    void flushBuffer() 
    {
        auto &res = json_buffer.str();
        out.write(res.data(), res.size());
        out.next();
        json_format.flush();
        json_buffer.restart();
    }

    WriteBufferFromOwnString json_buffer;
    Format json_format;
};

}
