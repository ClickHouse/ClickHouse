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

class SSEFormat : public IRowOutputFormat
{
public:
    SSEFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & fs) 
    : IRowOutputFormat(header_, out_)
    , json_format(json_buffer, getPort(IOutputFormat::PortKind::Main).getHeader(), fs, false)
    {
    }

    String getName() const override { return "SSEFormat"; }

    std::string getContentType() const override { return "text/event-stream"; }

protected:
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
        writeCString("event: progress\n", json_buffer);
        writeCString("data: ", json_buffer);
        json_format.writePrefix();
        writeCString("\n\n", json_buffer);
        flushBuffer();
    }

    void writeSuffix() override {
        json_format.writeSuffix();
    }

    bool writesProgressConcurrently() const override { return true; }

    void writeProgress(const Progress & value) override 
    {
        writeCString("event: progress\n", json_buffer);
        writeCString("data: ", json_buffer);
        json_format.writeProgress(value);
        writeCString("\n\n", json_buffer);
        flushBuffer();
    }

    void writeMinExtreme(const Columns & columns, size_t row_num) override
    {
        writeCString("event: min_extreme\n", json_buffer);
        writeCString("data: ", json_buffer);
        json_format.writeMinExtreme(columns,row_num);
        writeCString("\n\n", json_buffer);
        flushBuffer();
    }

    void writeMaxExtreme(const Columns & columns, size_t row_num) override
    {
        writeCString("event: max_extreme\n", json_buffer);
        writeCString("data: ", json_buffer);
        json_format.writeMaxExtreme(columns,row_num);
        writeCString("\n\n", json_buffer);
        flushBuffer();
    }

    void writeTotals(const Columns & columns, size_t row_num) override
    {
        writeCString("event: totals\n", json_buffer);
        writeCString("data: ", json_buffer);
        json_format.writeTotals(columns, row_num);
        writeCString("\n\n", json_buffer);
        flushBuffer();
    }

    void finalizeImpl() override
    {
        writeCString("event: progress\n", json_buffer);
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
    JSONEachRowWithProgressRowOutputFormat json_format;
};

}
