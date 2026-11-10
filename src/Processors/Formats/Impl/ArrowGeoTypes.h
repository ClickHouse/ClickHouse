#pragma once

#include <Common/WKB.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <base/types.h>

#if USE_ARROW
#    include <arrow/util/key_value_metadata.h>
#    include <arrow/array/array_binary.h>
#endif

namespace DB
{

enum class GeoEncoding : uint8_t
{
    WKB,
    WKT,
    /// One of the native (GeoArrow) encodings of GeoParquet 1.1: `point`, `linestring`, `polygon`,
    /// `multipoint`, `multilinestring`, `multipolygon`. The geometry is stored as nested lists of
    /// `struct<x: double, y: double>`, which have the same layout as the corresponding geo type
    /// (e.g. `Polygon` is `Array(Array(Tuple(Float64, Float64)))`), so the column is read as a
    /// regular nested column with the geo type requested, instead of being decoded from bytes.
    Native,
};

enum class GeoType : uint8_t
{
    Point,
    MultiPoint,
    LineString,
    Polygon,
    MultiLineString,
    MultiPolygon,
    /// Mixed means the column has multiple or unknown geometry types.
    /// It maps to the Geometry type (Variant of all geo types).
    Mixed,
};

struct GeoColumnMetadata
{
    GeoEncoding encoding = GeoEncoding::WKT;
    GeoType type = GeoType::Mixed;

    /// GeoParquet covering.bbox: names of the four Float64 scalar columns that store per-row
    /// bounding box coordinates. Row group min/max statistics on these columns give the spatial
    /// extent of each row group, enabling spatial predicate pushdown.
    struct BboxCovering
    {
        String xmin_column, ymin_column;
        String xmax_column, ymax_column;
    };
    std::optional<BboxCovering> covering_bbox;
};

#if USE_ARROW
const std::string * extractGeoMetadata(std::shared_ptr<const arrow::KeyValueMetadata> metadata);
#endif

UnorderedMapWithMemoryTracking<String, GeoColumnMetadata> parseGeoMetadataEncoding(const std::string * geo_json_str);

DataTypePtr getGeoDataType(GeoType type);

/// `col` must match getGeoDataType(type). Create it using getGeoDataType(type)->createColumn().
void appendObjectToGeoColumn(const GeometricObject & object, GeoType type, IColumn & col);

GeometricObject parseWKTFormat(ReadBuffer & in_buffer, bool precise_float_parsing);

}
