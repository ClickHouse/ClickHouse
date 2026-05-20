#pragma once

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <DataTypes/Serializations/ISerialization.h>

#include <map>
#include <optional>
#include <string>
#include <vector>


namespace DB
{

class WriteBuffer;

/// OpenMetrics text exposition (https://openmetrics.io/).
/// Standalone writer; intentionally does not share code with `FORMAT Prometheus`
/// so that adding/changing OpenMetrics behavior cannot affect Prometheus output.
class OpenMetricsTextOutputFormat final : public IRowOutputFormat
{
public:
    OpenMetricsTextOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_);

    String getName() const override { return "OpenMetricsTextOutputFormat"; }

private:
    struct ColumnPositions
    {
        size_t name;
        size_t value;
        std::optional<size_t> help;
        std::optional<size_t> type;
        std::optional<size_t> unit;
        std::optional<size_t> labels;
        std::optional<size_t> timestamp;
    };

    struct CurrentMetric
    {
        struct RowValue
        {
            std::map<String, String> labels;
            String value;
            String timestamp;
        };

        CurrentMetric() = default;
        explicit CurrentMetric(const String & name_) : name(name_) {}

        String name;
        String help;
        String type;
        String unit;
        std::vector<RowValue> values;
    };

    void write(const Columns & columns, size_t row_num) override;
    void writeField(const IColumn &, const ISerialization &, size_t) override {}
    void finalizeImpl() override;

    void flushCurrentMetric();
    String getString(const Columns & columns, size_t row_num, size_t column_pos);

    static void fixupBucketLabels(CurrentMetric & metric);

    ColumnPositions pos;
    CurrentMetric current_metric;
    SerializationPtr string_serialization;
    const FormatSettings format_settings;
};

}
