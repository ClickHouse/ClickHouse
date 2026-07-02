#pragma once

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IRowOutputFormat.h>

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>


namespace DB
{

class WriteBuffer;

/// OpenMetrics text exposition (https://openmetrics.io/).
/// Standalone writer; intentionally does not share code with `FORMAT Prometheus`
/// so that adding/changing OpenMetrics behavior cannot affect Prometheus output.
///
/// The column model mirrors the `TimeSeries` engine's outer columns, so a `TimeSeries` table can be
/// exported (and, with `SELECT * FROM ts`, round-tripped) directly:
///   * `metric_name`  (String)                          — the on-wire sample name (e.g. `foo_total`)
///   * `metric_family` (String, optional)               — the family named in `# HELP`/`# TYPE`/`# UNIT`
///   * `help` / `type` / `unit` (String, optional)      — family metadata
///   * `tags` (Map(String,String) or Array(Tuple(String,String)), optional) — labels
///   * `time_series` (Array(Tuple(DateTime64(3), Float64))) — the series' (timestamp, value) points
class OpenMetricsTextOutputFormat final : public IRowOutputFormat
{
public:
    OpenMetricsTextOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_);

    String getName() const override { return "OpenMetricsTextOutputFormat"; }

private:
    /// `metric_name` and `time_series` are required and always assigned by `getColumnPos`; the
    /// explicit zero-initializers exist purely to satisfy `cppcoreguidelines-pro-type-member-init`.
    struct ColumnPositions
    {
        size_t metric_name = 0;
        std::optional<size_t> metric_family;
        std::optional<size_t> help;
        std::optional<size_t> type;
        std::optional<size_t> unit;
        std::optional<size_t> tags;
        size_t time_series = 0;
    };

    /// One buffered time series (a single input row): its on-wire sample name, its label set, and its
    /// (timestamp-ms, value) points.
    struct Series
    {
        String metric_name;
        std::vector<std::pair<String, String>> tags;
        std::vector<std::pair<Int64, double>> points;
    };

    /// Series are grouped by metric family so a family's `# HELP`/`# TYPE`/`# UNIT` lines are emitted
    /// exactly once, ahead of all of that family's samples (OpenMetrics `metricfamily` grammar).
    struct CurrentFamily
    {
        String key;      /// grouping key: `metric_family` when non-empty, else `metric_name`
        String name;     /// name emitted in the `# HELP`/`# TYPE`/`# UNIT` lines
        bool started = false;
        String help;
        String type;
        String unit;
        std::vector<Series> series;
    };

    void write(const Columns & columns, size_t row_num) override;
    void writeField(const IColumn &, const ISerialization &, size_t) override {}
    void finalizeImpl() override;

    void flushCurrentFamily();
    String getString(const Columns & columns, size_t row_num, size_t column_pos);

    ColumnPositions pos;
    CurrentFamily current_family;
    /// Scale of the `DateTime64` in the `time_series` tuple; used to normalize points to milliseconds.
    UInt32 timestamp_scale = 3;
    /// Set while assembling a row in `write()`; if still true when `finalizeImpl` runs, a validation
    /// error aborted the row and the stream terminator must not be emitted.
    bool row_write_in_progress = false;
    const FormatSettings format_settings;
};

}
