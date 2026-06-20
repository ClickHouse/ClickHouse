#pragma once

#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

#include <optional>
#include <unordered_map>

namespace DB
{

class Block;
class ReadBuffer;

/// Parses OpenMetrics text (and common Prometheus text exposition).
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
        std::optional<size_t> name;
        std::optional<size_t> value;
        std::optional<size_t> help;
        std::optional<size_t> type;
        std::optional<size_t> labels;
        std::optional<size_t> timestamp;
        std::optional<size_t> unit;
    };

    static ColumnLoc buildColumnLoc(const Block & header);

    const FormatSettings format_settings;

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
    };

    std::unordered_map<String, FamilyMeta> family_meta;
    bool saw_eof = false;
    ColumnLoc column_loc;
    bool column_loc_initialized = false;
};

class OpenMetricsTextSchemaReader : public IExternalSchemaReader
{
public:
    NamesAndTypesList readSchema() override;
};

}
