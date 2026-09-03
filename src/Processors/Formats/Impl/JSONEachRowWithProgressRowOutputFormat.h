#pragma once

#include <Processors/Formats/Impl/JSONEachRowRowOutputFormat.h>
#include <mutex>

namespace DB
{

class JSONEachRowWithProgressRowOutputFormat final : public JSONEachRowRowOutputFormat
{
public:
    using JSONEachRowRowOutputFormat::JSONEachRowRowOutputFormat;

private:
    bool supportTotals() const override { return true; }
    bool supportExtremes() const override { return true; }

    void writePrefix() override;
    void writeSuffix() override;
    bool writesProgressConcurrently() const override { return true; }
    void writeProgress(const Progress & value) override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writeMinExtreme(const Columns & columns, size_t row_num) override;
    void writeMaxExtreme(const Columns & columns, size_t row_num) override;
    void writeTotals(const Columns & columns, size_t row_num) override;
    void finalizeImpl() override;

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

    void writeSpecialRow(const char * kind, const Columns & columns, size_t row_num);
};

}
