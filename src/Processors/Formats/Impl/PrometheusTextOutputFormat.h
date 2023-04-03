#pragma once

#include <string>
#include <unordered_map>

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <DataTypes/Serializations/ISerialization.h>


namespace DB
{

class WriteBuffer;

class PrometheusTextOutputFormat : public IRowOutputFormat
{
public:
    PrometheusTextOutputFormat(
        WriteBuffer & out_,
        const Block & header_,
        const RowOutputFormatParams & params_,
        const FormatSettings & format_settings_);

    String getName() const override { return "PrometheusTextOutputFormat"; }

    /// https://github.com/prometheus/docs/blob/86386ed25bc8a5309492483ec7d18d0914043162/content/docs/instrumenting/exposition_formats.md
    String getContentType() const override { return "text/plain; version=0.0.4; charset=UTF-8"; }

protected:

    struct ColumnPositions
    {
        size_t name;
        size_t value;
        std::optional<size_t> help;
        std::optional<size_t> type;
        std::optional<size_t> labels;
        std::optional<size_t> timestamp;
    };

    /// One metric can be represented by multiple rows (e.g. containing different labels).
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
        std::vector<RowValue> values;
    };

    /// Input rows should be grouped by the same metric.
    void write(const Columns & columns, size_t row_num) override;
    void writeField(const IColumn &, const ISerialization &, size_t) override {}
    void finalizeImpl() override;

    void flushCurrentMetric();
    String getString(const Columns & columns, size_t row_num, size_t column_pos);
    String getString(const IColumn & column, size_t row_num, SerializationPtr serialization);

    static void fixupBucketLabels(CurrentMetric & metric);

    ColumnPositions pos;
    CurrentMetric current_metric;
    SerializationPtr string_serialization;
    const FormatSettings format_settings;
};

}
