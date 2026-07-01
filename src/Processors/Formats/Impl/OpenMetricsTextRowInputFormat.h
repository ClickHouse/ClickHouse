#pragma once

#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace DB
{

class Block;
class ReadBuffer;

/// Parses OpenMetrics text (and common Prometheus text exposition) into the `TimeSeries`-aligned
/// per-series column model: one row per (metric_name, tags) series, its (timestamp, value) points
/// collected into a `time_series` array. Because a series' samples can be spread across the stream,
/// the whole exposition is read to `# EOF` and grouped before the first row is produced.
class OpenMetricsTextRowInputFormat final : public IRowInputFormat
{
public:
    OpenMetricsTextRowInputFormat(SharedHeader header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_);

    String getName() const override { return "OpenMetricsTextRowInputFormat"; }
    void resetParser() override;

private:
    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    void readPrefix() override;

    struct ColumnLoc
    {
        std::optional<size_t> metric_name;
        std::optional<size_t> metric_family;
        std::optional<size_t> help;
        std::optional<size_t> type;
        std::optional<size_t> unit;
        std::optional<size_t> tags;
        std::optional<size_t> time_series;
    };

    static ColumnLoc buildColumnLoc(const Block & header);

    struct FamilyMeta
    {
        String help;
        String type;
        String unit;
        /// Track which descriptors were already seen so a duplicate `# HELP` / `# TYPE` / `# UNIT`
        /// for the same family is rejected rather than silently overwriting the first.
        bool has_help = false;
        bool has_type = false;
        bool has_unit = false;
        /// Set once a sample row has been emitted under this exact on-wire name. A `# HELP` / `# TYPE`
        /// / `# UNIT` for the owning family is then rejected: family membership (`_bucket` / `_sum` /
        /// `_count`, counter `_total`, `_created` / `_gcount` / `_gsum` / `_info`) is driven by that
        /// metadata, so late metadata would make the parse order-dependent.
        bool samples_emitted = false;
    };

    /// One grouped output row: a time series identified by (metric_name, tags), with its family
    /// metadata and accumulated (millisecond-timestamp, value) points.
    struct OutputSeries
    {
        String metric_name;
        String metric_family;
        String help;
        String type;
        String unit;
        std::vector<std::pair<String, String>> tags;   /// sorted by label name
        std::vector<std::pair<Int64, double>> points;   /// (millisecond timestamp, value)
    };

    /// Reads the whole exposition, grouping samples into `output_rows`.
    void parseAll();
    /// Family this on-wire sample name belongs to (its own name, unless it is a `# TYPE`-declared
    /// family's suffixed sample such as `<family>_bucket` / `<family>_total`).
    String deriveMetricFamily(const String & metric_name) const;

    const FormatSettings format_settings;

    std::unordered_map<String, FamilyMeta> family_meta;
    bool saw_eof = false;

    ColumnLoc column_loc;
    bool column_loc_initialized = false;
    /// Scale of the `DateTime64` in the target `time_series` tuple; points are stored at this scale.
    UInt32 timestamp_scale = 3;

    bool parsed = false;
    std::vector<OutputSeries> output_rows;
    size_t next_output_row = 0;
};

class OpenMetricsTextSchemaReader : public IExternalSchemaReader
{
public:
    NamesAndTypesList readSchema() override;
};

}
