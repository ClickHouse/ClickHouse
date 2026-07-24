#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Common/WKB.h>

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
    WKT
};

enum class GeoType : uint8_t
{
    Point,
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
    GeoEncoding encoding;
    GeoType type;
};

#if USE_ARROW
const std::string * extractGeoMetadata(std::shared_ptr<const arrow::KeyValueMetadata> metadata);
#endif

std::unordered_map<String, GeoColumnMetadata> parseGeoMetadataEncoding(const std::string * geo_json_str);

DataTypePtr getGeoDataType(GeoType type);

/// `col` must match getGeoDataType(type). Create it using getGeoDataType(type)->createColumn().
void appendObjectToGeoColumn(const GeometricObject & object, GeoType type, IColumn & col);

GeometricObject parseWKTFormat(ReadBuffer & in_buffer);

}
