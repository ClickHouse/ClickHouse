#pragma once

#include <optional>
#include <unordered_map>
#include <Columns/ColumnVariant.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Processors/Formats/IRowInputFormat.h>

namespace DB
{

class FormatFactory;

/// Reads a GeoJSON FeatureCollection, yielding one row per feature.
/// The output schema is: id String, geometry Geometry, properties JSON.
class GeoJSONRowInputFormat final : public IRowInputFormat
{
public:
    GeoJSONRowInputFormat(
        ReadBuffer & in_,
        SharedHeader header_,
        Params params_,
        const FormatSettings & format_settings_);

    String getName() const override { return "GeoJSONRowInputFormat"; }
    void resetParser() override;

protected:
    void readPrefix() override;
    void readSuffix() override;

private:
    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    void readGeometry(IColumn & col);

    const FormatSettings format_settings;
    bool first_row = true;
    bool done = false;

    std::optional<size_t> id_col_idx;
    std::optional<size_t> geometry_col_idx;
    std::optional<size_t> properties_col_idx;

    std::unordered_map<String, ColumnVariant::Discriminator> geometry_discriminants;
};

class GeoJSONExternalSchemaReader : public IExternalSchemaReader
{
public:
    NamesAndTypesList readSchema() override;
};

void registerInputFormatGeoJSON(FormatFactory & factory);
void registerGeoJSONSchemaReader(FormatFactory & factory);

}
