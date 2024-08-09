#pragma once

#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Formats/Impl/JSONRowOutputFormat.h>


namespace DB
{

struct FormatSettings;

class JSONCompactWithProgressRowOutputFormat final : public JSONRowOutputFormat
{
public:
    JSONCompactWithProgressRowOutputFormat(WriteBuffer & out_, const Block & header, const FormatSettings & settings_, bool yield_strings_);

    String getName() const override { return "JSONCompactWithProgressRowOutputFormat"; }

    void onProgress(const Progress & value) override;
    void flush() override;

private:
    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writeRowBetweenDelimiter() override;
    bool supportTotals() const override { return true; }
    bool supportExtremes() const override { return true; }
    void writeBeforeTotals() override;
    void writeAfterTotals() override;
    void writeExtremesElement(const char * title, const Columns & columns, size_t row_num) override;
    void writeTotals(const Columns & columns, size_t row_num) override;

    void writeProgress();
    void writePrefix() override;
    void writeSuffix() override;
    void finalizeImpl() override;


    std::vector<String> progress_lines;
    std::mutex progress_lines_mutex;
    /// To not lock mutex and check progress_lines every row,
    /// we will use atomic flag that progress_lines is not empty.
    std::atomic_bool has_progress = false;
};

}
