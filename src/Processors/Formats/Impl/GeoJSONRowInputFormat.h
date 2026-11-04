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

/// The discriminators of the types of a `Geometry` column by the name of the type.
using GeoJSONGeometryDiscriminators = std::unordered_map<String, ColumnVariant::Discriminator>;
GeoJSONGeometryDiscriminators getGeoJSONGeometryDiscriminators(const DataTypePtr & type);

/// Read and validate a GeoJSON geometry object or a JSON `null`. When `col` is null the geometry is only
/// validated (and nothing is inserted), so a `geometry` that is not a requested output column is
/// still checked for structural validity instead of being skipped as generic JSON.
void readGeoJSONGeometry(
    ReadBuffer & buf, IColumn * col, const GeoJSONGeometryDiscriminators & geometry_discriminators, const FormatSettings & format_settings);

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
    void validateTopLevelTypeMember(ReadBuffer & buf);

    const FormatSettings format_settings;
    bool first_row = true;
    bool done = false;
    /// Whether the required top-level `type` member was seen and validated as `FeatureCollection`.
    bool top_level_type_validated = false;

    std::optional<size_t> id_col_idx;
    std::optional<size_t> geometry_col_idx;
    std::optional<size_t> properties_col_idx;

    GeoJSONGeometryDiscriminators geometry_discriminators;
};

class GeoJSONExternalSchemaReader : public IExternalSchemaReader
{
public:
    NamesAndTypesList readSchema() override;
};

void registerInputFormatGeoJSON(FormatFactory & factory);
void registerGeoJSONSchemaReader(FormatFactory & factory);

}
