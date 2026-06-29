#pragma once

#include <array>
#include <optional>
#include <vector>

#include <Columns/ColumnVariant.h>
#include <Core/Names.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/OutputFormatWithUTF8ValidationAdaptor.h>
#include <Processors/Formats/RowOutputFormatWithExceptionHandlerAdaptor.h>

namespace DB
{

class Block;
class WriteBuffer;
class FormatFactory;
class IColumn;
class ISerialization;

/// Writes a result set as a single GeoJSON `FeatureCollection`, one `Feature` per row (RFC 7946).
///
/// Column mapping:
///   - geometry: the single geo-typed column (`Point`, `Ring`, `LineString`, `MultiLineString`,
///     `Polygon`, `MultiPolygon`, or `Geometry`). Exactly one is required.
///   - id: a column named `id`, emitted as the Feature `id` when present and non-NULL.
///   - properties: the remaining columns. A lone object-typed column named `properties` is emitted
///     directly as the `properties` object (so GeoJSON read by the input format round-trips);
///     otherwise each remaining column becomes one property keyed by its column name.
///
/// `Ring` has no GeoJSON geometry type (a linear ring is a `Polygon` component), so a `Ring` value is
/// emitted as a single-ring `Polygon`.
class GeoJSONRowOutputFormat final : public RowOutputFormatWithExceptionHandlerAdaptor<RowOutputFormatWithUTF8ValidationAdaptor, bool>
{
public:
    GeoJSONRowOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & settings_);

    String getName() const override { return "GeoJSONRowOutputFormat"; }

    bool supportsSpecialSerializationKinds() const override { return settings.allow_special_serialization_kinds; }

protected:
    /// A `Feature` is not a 1:1 mapping of columns, so the whole row is written here rather than
    /// through the per-field path; `writeField` is therefore unused.
    void write(const Columns & columns, size_t row_num) override;
    void writeField(const IColumn &, const ISerialization &, size_t) override { }

    void writeRowBetweenDelimiter() override;
    void writePrefix() override;
    void writeSuffix() override;

    void resetFormatterImpl() override;

private:
    /// How a concrete (non-Variant) geometry value is emitted as GeoJSON.
    struct GeometryKind
    {
        const char * geojson_type = nullptr; /// GeoJSON `"type"` value, e.g. "Polygon".
        size_t depth = 0; /// Number of nested coordinate-array levels in storage.
        bool wrap_in_array = false; /// Emit one extra array level (a `Ring` -> a single-ring `Polygon`).
        size_t min_points = 0; /// Minimum points in each innermost array (a line or ring); 0 = no check.
        bool ring = false; /// The innermost array is a polygon ring (must have >= 4 points and be closed).
    };

    /// The emission kind for a non-Variant geo type name, or nullopt if `type_name` is not one of
    /// them. `Geometry` returns nullopt because it is handled as a `Variant`.
    static std::optional<GeometryKind> geometryKindFor(const String & type_name);

    void writeId(const Columns & columns, size_t row_num);
    void writeGeometry(const IColumn & column, size_t row_num);
    void writeProperties(const Columns & columns, size_t row_num);

    void writeGeometryObject(const GeometryKind & kind, const IColumn & column, size_t row_num);
    /// Recursively emit `depth` nested coordinate-array levels, bottoming out at a position `[x, y]`.
    /// `kind` carries the validation rules applied when `validate_geometry` is enabled.
    void writeCoordinates(const IColumn & column, size_t row_num, size_t depth, const GeometryKind & kind);
    void writePosition(const IColumn & tuple_column, size_t row_num);

    FormatSettings settings;
    /// `settings` with number quoting disabled, used for coordinates and numeric feature ids.
    FormatSettings number_settings;
    /// `settings` but forcing JSON-object serialization (named tuples and maps as objects), used only to
    /// emit a lone object-typed `properties` column directly as the Feature's `properties` object.
    FormatSettings properties_object_settings;
    WriteBuffer * ostr;

    /// When set (the `format_geojson_validate_geometry` setting), geometries that violate RFC 7946 shape
    /// rules are rejected on write, mirroring the input format, so written documents round-trip.
    bool validate_geometry = true;

    std::optional<size_t> id_col_idx;
    bool id_is_float = false;

    /// Always set after construction (the constructor rejects a header without exactly one geo column).
    size_t geometry_col_idx = 0;
    bool geometry_is_variant = false;
    GeometryKind concrete_kind;
    /// For the `Geometry` Variant: emission kind per global discriminator (NULL handled separately).
    std::array<GeometryKind, ColumnVariant::MAX_NESTED_COLUMNS> variant_kind{};

    /// Columns forming `properties`. When `emit_properties_column_directly` is set there is exactly one such column,
    /// emitted directly as the properties object.
    std::vector<size_t> property_col_indices;
    Names property_json_names;
    bool emit_properties_column_directly = false;
};

void registerOutputFormatGeoJSON(FormatFactory & factory);

}
