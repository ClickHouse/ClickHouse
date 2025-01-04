#pragma once

#include <Processors/Formats/Impl/JSONEachRowRowOutputFormat.h>
#include <mutex>

namespace DB
{

class JSONEachRowWithProgressRowOutputFormat final : public JSONEachRowRowOutputFormat
{
public:
    using JSONEachRowRowOutputFormat::JSONEachRowRowOutputFormat;

    void writePrefix() override;
    bool writesProgressConcurrently() const override { return true; }
    void writeProgress(const Progress & value) override;

private:
    bool supportTotals() const override { return true; }
    bool supportExtremes() const override { return true; }

    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writeMinExtreme(const Columns & columns, size_t row_num) override;
    void writeMaxExtreme(const Columns & columns, size_t row_num) override;
    void writeTotals(const Columns & columns, size_t row_num) override;

    void writeSpecialRow(const char * kind, const Columns & columns, size_t row_num);


};

}
