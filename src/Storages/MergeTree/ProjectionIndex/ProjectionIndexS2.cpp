#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexS2.h>

#include "config.h"

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ASTProjectionSelectQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/KeyDescription.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <cmath>
#include <numeric>
#include <optional>
#include <string>
#include <unordered_set>
#include <variant>

#if USE_S2_GEOMETRY
#include <s2/s2cell_id.h>
#include <s2/s2cell_union.h>
#include <s2/s2earth.h>
#include <s2/s2error.h>
#include <s2/s2latlng.h>
#include <s2/s2latlng_rect.h>
#include <s2/s2polyline.h>
#include <s2/s2region_coverer.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
}

namespace
{

using S2Params = ProjectionIndexS2::Params;

/// ── Parameter parsing helpers ────────────────────────────────────────────────
/// Parse TYPE arguments from SQL: TYPE s2(max_cells=8, min_level=15, max_level=15)
/// Supports both named (key=value) and positional syntax.

UInt64 parseUInt64Literal(const IAST * ast, String name)
{
    const auto * literal = ast ? ast->as<ASTLiteral>() : nullptr;
    if (!literal)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Parameter '{}' must be a literal", name);

    try
    {
        return applyVisitor(FieldVisitorConvertToNumber<UInt64>(), literal->value);
    }
    catch (...)
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Parameter '{}' must be UInt64", name);
    }
}

bool parseBoolLiteral(const IAST * ast, String name)
{
    return parseUInt64Literal(ast, std::move(name)) != 0;
}

void assignNamedParam(S2Params & params, const String & key, const IAST * value)
{
    if (key == "max_cells")
        params.max_cells = parseUInt64Literal(value, key);
    else if (key == "min_level")
        params.min_level = parseUInt64Literal(value, key);
    else if (key == "max_level")
        params.max_level = parseUInt64Literal(value, key);
    else if (key == "strict_decode")
        params.strict_decode = parseBoolLiteral(value, key);
    else
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown ProjectionIndexS2 parameter '{}'", key);
}

void parseTypeArguments(S2Params & params, const ASTFunction * type_func)
{
    if (!type_func || !type_func->arguments)
        return;

    size_t positional_idx = 0;

    for (const auto & arg : type_func->arguments->children)
    {
        const auto * func = arg->as<ASTFunction>();
        if (func && func->name == "equals" && func->arguments && func->arguments->children.size() == 2)
        {
            const auto * name_id = func->arguments->children[0]->as<ASTIdentifier>();
            if (!name_id || !name_id->isShort())
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Named parameter must be identifier in ProjectionIndexS2 TYPE arguments");

            assignNamedParam(params, name_id->shortName(), func->arguments->children[1].get());
            continue;
        }

        /// Positional fallback: (max_cells, min_level, max_level)
        if (positional_idx == 0)
            params.max_cells = parseUInt64Literal(arg.get(), "max_cells");
        else if (positional_idx == 1)
            params.min_level = parseUInt64Literal(arg.get(), "min_level");
        else if (positional_idx == 2)
            params.max_level = parseUInt64Literal(arg.get(), "max_level");
        else
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Too many positional TYPE arguments for ProjectionIndexS2");

        ++positional_idx;
    }
}

/// Validate that S2 parameters are within acceptable ranges.
void validateParams(const S2Params & params)
{
    if (params.source_column.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "ProjectionIndexS2 source column is empty");

    if (params.max_cells == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ProjectionIndexS2 parameter 'max_cells' must be > 0");

    if (params.max_level > 30)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ProjectionIndexS2 parameter 'max_level' must be <= 30");

    if (params.min_level > params.max_level)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "ProjectionIndexS2 parameters invalid: min_level ({}) must be <= max_level ({})",
            params.min_level,
            params.max_level);
}

/// ── Parent offset computation ────────────────────────────────────────────────
/// Build the _parent_part_offset array that maps each source row to its position
/// in the written part. When a sort permutation is applied (perm_ptr != nullptr),
/// the mapping accounts for row reordering so that projection rows correctly
/// reference their parent rows after sorting.

PaddedPODArray<UInt64> buildParentOffsets(size_t rows, UInt64 starting_offset, const IColumnPermutation * perm_ptr)
{
    PaddedPODArray<UInt64> res;
    res.resize(rows);

    if (!perm_ptr)
    {
        for (size_t row = 0; row < rows; ++row)
            res[row] = starting_offset + row;
        return res;
    }

    /// Keep semantics consistent with ProjectionDescription::calculateByQuery insertion path:
    /// offset[perm[i]] = i, i.e. perm maps written position -> source row.
    for (size_t written_pos = 0; written_pos < rows; ++written_pos)
    {
        size_t source_row = (*perm_ptr)[written_pos];
        res[source_row] = starting_offset + written_pos;
    }

    return res;
}

/// ── Projection block sorting ─────────────────────────────────────────────────
/// Sort the output block by (cell_id, _parent_part_offset) so that the projection
/// data is stored in primary key order. This enables efficient index pruning when
/// the rewritten s2CellsIntersect predicate is evaluated against the projection.

void sortProjectionIndexS2Block(Block & out)
{
    if (out.rows() == 0)
        return;

    if (!out.has("cell_id") || !out.has("_parent_part_offset"))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "ProjectionIndexS2 sort requires columns 'cell_id' and '_parent_part_offset'");

    auto cell_col_ptr = out.getByName("cell_id").column->convertToFullColumnIfConst();
    auto parent_col_ptr = out.getByName("_parent_part_offset").column->convertToFullColumnIfConst();

    const auto * cell_u64 = typeid_cast<const ColumnUInt64 *>(cell_col_ptr.get());
    const auto * parent_u64 = typeid_cast<const ColumnUInt64 *>(parent_col_ptr.get());

    if (!cell_u64 || !parent_u64)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "ProjectionIndexS2 sort key columns must be UInt64");

    const auto & cell_data = cell_u64->getData();
    const auto & parent_data = parent_u64->getData();

    if (cell_data.size() != parent_data.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mismatched key column sizes in ProjectionIndexS2 sort");

    const size_t size = cell_data.size();

    bool already_sorted = true;
    for (size_t i = 1; i < size; ++i)
    {
        if (cell_data[i - 1] > cell_data[i]
            || (cell_data[i - 1] == cell_data[i] && parent_data[i - 1] > parent_data[i]))
        {
            already_sorted = false;
            break;
        }
    }

    if (already_sorted)
        return;

    IColumnPermutation permutation(size);
    std::iota(permutation.begin(), permutation.end(), 0);

    std::stable_sort(permutation.begin(), permutation.end(), [&](size_t lhs, size_t rhs)
    {
        const UInt64 lhs_cell = cell_data[lhs];
        const UInt64 rhs_cell = cell_data[rhs];
        if (lhs_cell != rhs_cell)
            return lhs_cell < rhs_cell;

        const UInt64 lhs_offset = parent_data[lhs];
        const UInt64 rhs_offset = parent_data[rhs];
        return lhs_offset < rhs_offset;
    });

    Columns reordered;
    reordered.reserve(out.columns());
    for (size_t i = 0; i < out.columns(); ++i)
        reordered.emplace_back(out.getByPosition(i).column->permute(permutation, 0));

    for (size_t i = 0; i < out.columns(); ++i)
        out.getByPosition(i).column = std::move(reordered[i]);
}

/// ── Geometry decode helpers ──────────────────────────────────────────────────
/// These helpers decode geometry values from ClickHouse columns into
/// boost::geometry types suitable for S2 bounding-box computation.

struct DecodedGeometry
{
    enum class Kind
    {
        Point,
        Ring,
        LineString,
        MultiLineString,
        Polygon,
        MultiPolygon
    };

    Kind kind;
    std::variant<SphericalPoint, SphericalRing, SphericalLineString, SphericalMultiLineString, SphericalPolygon, SphericalMultiPolygon> value;
};

struct GeometryDecodeCache
{
    enum class Kind
    {
        Point,
        Ring,
        LineString,
        MultiLineString,
        Polygon,
        MultiPolygon,
        Geometry
    };

    Kind kind;
    size_t rows = 0;

    /// For single-type columns (Point, Ring, LineString, MultiLineString, Polygon, MultiPolygon):
    std::variant<
        std::vector<SphericalPoint>,
        std::vector<SphericalRing>,
        std::vector<SphericalLineString>,
        std::vector<SphericalMultiLineString>,
        std::vector<SphericalPolygon>,
        std::vector<SphericalMultiPolygon>> decoded;

    /// For Geometry (Variant) columns: each nested column decoded separately.
    /// Discriminator mapping (sorted alphabetically by type name):
    ///   0 = LineString, 1 = MultiLineString, 2 = MultiPolygon,
    ///   3 = Point, 4 = Polygon, 5 = Ring, 255 = NULL
    struct GeometryVariantData
    {
        const ColumnVariant * column_variant = nullptr;
        std::vector<SphericalLineString> linestrings;         /// discriminator 0
        std::vector<SphericalMultiLineString> multilinestrings; /// discriminator 1
        std::vector<SphericalMultiPolygon> multipolygons;     /// discriminator 2
        std::vector<SphericalPoint> points;                   /// discriminator 3
        std::vector<SphericalPolygon> polygons;               /// discriminator 4
        std::vector<SphericalRing> rings;                     /// discriminator 5
    };
    std::optional<GeometryVariantData> geometry_data;

    /// Non-null when the source column is Nullable; points into the ColumnNullable's null map.
    const NullMap * null_map = nullptr;
};

/// Bulk-decode an entire column of geometry values into a cache.
/// Supports Point, Ring, LineString, MultiLineString, Polygon, MultiPolygon, and Geometry (Variant).
/// Returns nullopt if the column type is not a supported geometry type.
std::optional<GeometryDecodeCache> prepareGeometryDecodeCache(const ColumnPtr & column, const DataTypePtr & type, bool strict_mode)
{
    const auto & factory = DataTypeFactory::instance();
    ColumnPtr full_column = column->convertToFullColumnIfConst();

    /// Unwrap Nullable: strip the Nullable wrapper from both column and type,
    /// and remember the null map so tryDecodeGeometry() can skip NULL rows.
    const NullMap * null_map = nullptr;
    DataTypePtr inner_type = type;
    if (const auto * col_nullable = typeid_cast<const ColumnNullable *>(full_column.get()))
    {
        null_map = &col_nullable->getNullMapData();
        full_column = col_nullable->getNestedColumnPtr();
        inner_type = removeNullable(type);
    }

    try
    {
        /// Geometry (Variant) type: decode each nested column separately.
        if (inner_type->getCustomName() && inner_type->getCustomName()->getName() == "Geometry")
        {
            const auto * column_variant = typeid_cast<const ColumnVariant *>(full_column.get());
            if (!column_variant)
                return std::nullopt;

            GeometryDecodeCache cache;
            cache.kind = GeometryDecodeCache::Kind::Geometry;
            cache.rows = column_variant->size();
            cache.geometry_data.emplace();
            cache.geometry_data->column_variant = column_variant;

            /// Global discriminator order (DataTypeVariant sorts by name alphabetically):
            ///   0 = LineString, 1 = MultiLineString, 2 = MultiPolygon,
            ///   3 = Point, 4 = Polygon, 5 = Ring
            /// Use getVariantByGlobalDiscriminator() to avoid dependence on local ordering.
            auto get_variant = [&](size_t global_discr) -> const IColumn *
            {
                auto local_discr = column_variant->localDiscriminatorByGlobal(static_cast<ColumnVariant::Discriminator>(global_discr));
                if (local_discr == ColumnVariant::NULL_DISCRIMINATOR)
                    return nullptr;
                const auto & col = column_variant->getVariantByLocalDiscriminator(local_discr);
                return col.size() > 0 ? &col : nullptr;
            };

            if (const auto * col = get_variant(0))
                cache.geometry_data->linestrings = ColumnToLineStringsConverter<SphericalPoint>::convert(col->getPtr());
            if (const auto * col = get_variant(1))
                cache.geometry_data->multilinestrings = ColumnToMultiLineStringsConverter<SphericalPoint>::convert(col->getPtr());
            if (const auto * col = get_variant(2))
                cache.geometry_data->multipolygons = ColumnToMultiPolygonsConverter<SphericalPoint>::convert(col->getPtr());
            if (const auto * col = get_variant(3))
                cache.geometry_data->points = ColumnToPointsConverter<SphericalPoint>::convert(col->getPtr());
            if (const auto * col = get_variant(4))
                cache.geometry_data->polygons = ColumnToPolygonsConverter<SphericalPoint>::convert(col->getPtr());
            if (const auto * col = get_variant(5))
                cache.geometry_data->rings = ColumnToRingsConverter<SphericalPoint>::convert(col->getPtr());

            cache.null_map = null_map;
            return cache;
        }

        if (factory.get("Point")->equals(*inner_type))
        {
            auto decoded = ColumnToPointsConverter<SphericalPoint>::convert(full_column);
            GeometryDecodeCache cache;
            cache.kind = GeometryDecodeCache::Kind::Point;
            cache.rows = decoded.size();
            cache.decoded = std::move(decoded);
            cache.null_map = null_map;
            return cache;
        }

        if (factory.get("Ring")->equals(*inner_type))
        {
            auto decoded = ColumnToRingsConverter<SphericalPoint>::convert(full_column);
            GeometryDecodeCache cache;
            cache.kind = GeometryDecodeCache::Kind::Ring;
            cache.rows = decoded.size();
            cache.decoded = std::move(decoded);
            cache.null_map = null_map;
            return cache;
        }

        if (factory.get("LineString")->equals(*inner_type))
        {
            auto decoded = ColumnToLineStringsConverter<SphericalPoint>::convert(full_column);
            GeometryDecodeCache cache;
            cache.kind = GeometryDecodeCache::Kind::LineString;
            cache.rows = decoded.size();
            cache.decoded = std::move(decoded);
            cache.null_map = null_map;
            return cache;
        }

        if (factory.get("MultiLineString")->equals(*inner_type))
        {
            auto decoded = ColumnToMultiLineStringsConverter<SphericalPoint>::convert(full_column);
            GeometryDecodeCache cache;
            cache.kind = GeometryDecodeCache::Kind::MultiLineString;
            cache.rows = decoded.size();
            cache.decoded = std::move(decoded);
            cache.null_map = null_map;
            return cache;
        }

        if (factory.get("Polygon")->equals(*inner_type))
        {
            auto decoded = ColumnToPolygonsConverter<SphericalPoint>::convert(full_column);
            GeometryDecodeCache cache;
            cache.kind = GeometryDecodeCache::Kind::Polygon;
            cache.rows = decoded.size();
            cache.decoded = std::move(decoded);
            cache.null_map = null_map;
            return cache;
        }

        if (factory.get("MultiPolygon")->equals(*inner_type))
        {
            auto decoded = ColumnToMultiPolygonsConverter<SphericalPoint>::convert(full_column);
            GeometryDecodeCache cache;
            cache.kind = GeometryDecodeCache::Kind::MultiPolygon;
            cache.rows = decoded.size();
            cache.decoded = std::move(decoded);
            cache.null_map = null_map;
            return cache;
        }
    }
    catch (...)
    {
        if (strict_mode)
            throw;
        return std::nullopt;
    }

    if (strict_mode)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "ProjectionIndexS2 supports only Point/Ring/LineString/MultiLineString/Polygon/MultiPolygon/Geometry (and their Nullable variants), got {}", type->getName());

    return std::nullopt;
}

/// Extract a single decoded geometry from the cache by row index.
std::optional<DecodedGeometry> tryDecodeGeometry(const GeometryDecodeCache & cache, size_t row, bool strict_mode)
{
    if (row >= cache.rows)
    {
        if (strict_mode)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ProjectionIndexS2 row {} out of decode cache range {}", row, cache.rows);
        return std::nullopt;
    }

    /// NULL rows in Nullable geometry columns — return nullopt so calculate()
    /// emits face cells (safe: no false negatives, just no pruning for this row).
    if (cache.null_map && (*cache.null_map)[row])
        return std::nullopt;

    DecodedGeometry out;

    if (cache.kind == GeometryDecodeCache::Kind::Point)
    {
        out.kind = DecodedGeometry::Kind::Point;
        out.value = std::get<std::vector<SphericalPoint>>(cache.decoded)[row];
        return out;
    }

    if (cache.kind == GeometryDecodeCache::Kind::Ring)
    {
        out.kind = DecodedGeometry::Kind::Ring;
        out.value = std::get<std::vector<SphericalRing>>(cache.decoded)[row];
        return out;
    }

    if (cache.kind == GeometryDecodeCache::Kind::LineString)
    {
        out.kind = DecodedGeometry::Kind::LineString;
        out.value = std::get<std::vector<SphericalLineString>>(cache.decoded)[row];
        return out;
    }

    if (cache.kind == GeometryDecodeCache::Kind::MultiLineString)
    {
        out.kind = DecodedGeometry::Kind::MultiLineString;
        out.value = std::get<std::vector<SphericalMultiLineString>>(cache.decoded)[row];
        return out;
    }

    if (cache.kind == GeometryDecodeCache::Kind::Polygon)
    {
        out.kind = DecodedGeometry::Kind::Polygon;
        out.value = std::get<std::vector<SphericalPolygon>>(cache.decoded)[row];
        return out;
    }

    if (cache.kind == GeometryDecodeCache::Kind::MultiPolygon)
    {
        out.kind = DecodedGeometry::Kind::MultiPolygon;
        out.value = std::get<std::vector<SphericalMultiPolygon>>(cache.decoded)[row];
        return out;
    }

    /// Geometry (Variant) type: use global discriminator to determine the type of each row.
    if (cache.kind == GeometryDecodeCache::Kind::Geometry)
    {
        const auto & gd = *cache.geometry_data;
        const auto * cv = gd.column_variant;

        auto global_discr = cv->globalDiscriminatorAt(row);
        if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
            return std::nullopt;

        auto offset = cv->offsetAt(row);

        switch (global_discr)
        {
            case 0: /// LineString
                out.kind = DecodedGeometry::Kind::LineString;
                out.value = gd.linestrings[offset];
                return out;
            case 1: /// MultiLineString
                out.kind = DecodedGeometry::Kind::MultiLineString;
                out.value = gd.multilinestrings[offset];
                return out;
            case 2: /// MultiPolygon
                out.kind = DecodedGeometry::Kind::MultiPolygon;
                out.value = gd.multipolygons[offset];
                return out;
            case 3: /// Point
                out.kind = DecodedGeometry::Kind::Point;
                out.value = gd.points[offset];
                return out;
            case 4: /// Polygon
                out.kind = DecodedGeometry::Kind::Polygon;
                out.value = gd.polygons[offset];
                return out;
            case 5: /// Ring
                out.kind = DecodedGeometry::Kind::Ring;
                out.value = gd.rings[offset];
                return out;
            default:
                return std::nullopt;
        }
    }

    return std::nullopt;
}

#if USE_S2_GEOMETRY

/// ── S2 covering computation ──────────────────────────────────────────────────
/// Build an S2 cell covering for a geometry. For linestrings, we convert to
/// native S2Polyline for tight coverings. For polygons, rings, and multi-polygons,
/// we compute an S2LatLngRect bounding box from all vertices and cover that
/// rectangle. The bounding box is a conservative approximation (no false
/// negatives, only potential false positives). We cannot use S2Polygon due to
/// linker issues with S2ValidationQueryBase in ClickHouse's S2 library build,
/// and S2Loop has subtle winding-order semantics that can cause coverings to
/// represent the complement of the intended region.

struct S2CoveringBuildOptions
{
    UInt64 max_cells = 8;
    UInt64 min_level = 16;
    UInt64 max_level = 20;
    bool normalize_covering = true;
};

/// Add a single (lon, lat) point to the S2LatLngRect bounding box.
bool addPointToRect(const SphericalPoint & point, S2LatLngRect & rect, bool & initialized)
{
    const double lon = boost::geometry::get<0>(point);
    const double lat = boost::geometry::get<1>(point);

    if (!std::isfinite(lon) || !std::isfinite(lat))
        return false;

    S2LatLng lat_lng = S2LatLng::FromDegrees(lat, lon);
    if (!lat_lng.is_valid())
        return false;

    if (!initialized)
    {
        rect = S2LatLngRect(lat_lng, lat_lng);
        initialized = true;
    }
    else
    {
        rect.AddPoint(lat_lng.ToPoint());
    }

    return true;
}

/// Expand the bounding box to include all vertices of a ring (closed loop of points).
bool addRingToRect(const SphericalRing & ring, S2LatLngRect & rect, bool & initialized)
{
    for (const auto & point : ring)
    {
        if (!addPointToRect(point, rect, initialized))
            return false;
    }
    return true;
}

/// Expand the bounding box to include all vertices of a linestring.
bool addLineStringToRect(const SphericalLineString & linestring, S2LatLngRect & rect, bool & initialized)
{
    for (const auto & point : linestring)
    {
        if (!addPointToRect(point, rect, initialized))
            return false;
    }
    return true;
}

/// Expand the bounding box to include all vertices (outer ring + inner rings) of a polygon.
bool addPolygonToRect(const SphericalPolygon & polygon, S2LatLngRect & rect, bool & initialized)
{
    SphericalPolygon corrected = polygon;
    boost::geometry::correct(corrected);

    auto add_ring = [&](const SphericalRing & ring) -> bool
    {
        for (const auto & point : ring)
        {
            if (!addPointToRect(point, rect, initialized))
                return false;
        }

        return true;
    };

    if (!add_ring(corrected.outer()))
        return false;

    for (const auto & inner : corrected.inners())
    {
        if (!add_ring(inner))
            return false;
    }

    return true;
}

/// ── Native S2 type conversion helpers ─────────────────────────────────────────
/// Convert boost::geometry linestring types to native S2Polyline for tight S2
/// coverings. Used by `buildS2Covering` for LineString/MultiLineString.

/// Convert a boost (lon, lat) point in degrees to an S2Point on the unit sphere.
std::optional<S2Point> boostPointToS2Point(const SphericalPoint & point)
{
    const double lon = boost::geometry::get<0>(point);
    const double lat = boost::geometry::get<1>(point);

    if (!std::isfinite(lon) || !std::isfinite(lat))
        return std::nullopt;

    S2LatLng lat_lng = S2LatLng::FromDegrees(lat, lon);
    if (!lat_lng.is_valid())
        return std::nullopt;

    return lat_lng.ToPoint();
}

/// Convert a boost linestring to a vector of S2Points (no closing vertex removal).
std::optional<std::vector<S2Point>> convertBoostLineStringToS2Points(const SphericalLineString & linestring)
{
    if (linestring.empty())
        return std::nullopt;

    std::vector<S2Point> points;
    points.reserve(linestring.size());
    for (const auto & vertex : linestring)
    {
        auto s2point = boostPointToS2Point(vertex);
        if (!s2point)
            return std::nullopt;
        points.push_back(*s2point);
    }

    return points;
}

/// Try to convert a boost linestring to an S2Polyline. Returns nullptr on failure.
std::unique_ptr<S2Polyline> tryConvertLineStringToS2Polyline(const SphericalLineString & linestring)
{
    auto points_opt = convertBoostLineStringToS2Points(linestring);
    if (!points_opt || points_opt->size() < 2)
        return nullptr;

    auto polyline = std::make_unique<S2Polyline>();
    polyline->set_s2debug_override(S2Debug::DISABLE);
    polyline->Init(*points_opt);

    S2Error error;
    if (polyline->FindValidationError(&error))
        return nullptr;

    return polyline;
}

/// Build an S2CellUnion covering for a decoded geometry.
///
/// Strategy per geometry kind:
///   Point           → single S2CellId at max_level (no coverer needed)
///   LineString      → native S2Polyline covering (tight); bounding-box fallback
///   MultiLineString → native S2Polyline per segment, union; bounding-box fallback
///   Ring/Polygon/MultiPolygon → S2LatLngRect bounding-box covering
///
/// LineStrings prefer native S2Polyline because S2RegionCoverer can produce
/// a tight covering along the line. Polygons use bounding-box because S2Polygon
/// cannot be linked (missing symbols) and S2Loop has winding-order ambiguity.
std::optional<S2CellUnion> buildS2Covering(
    const DecodedGeometry & geometry,
    const S2CoveringBuildOptions & options)
{
    /// Point: map directly to a single S2CellId at max_level.
    if (geometry.kind == DecodedGeometry::Kind::Point)
    {
        const auto & point = std::get<SphericalPoint>(geometry.value);
        const double lon = boost::geometry::get<0>(point);
        const double lat = boost::geometry::get<1>(point);

        if (!std::isfinite(lon) || !std::isfinite(lat))
            return std::nullopt;

        S2LatLng lat_lng = S2LatLng::FromDegrees(lat, lon);
        if (!lat_lng.is_valid())
            return std::nullopt;

        S2CellId cell_id(lat_lng.ToPoint());
        if (!cell_id.is_valid())
            return std::nullopt;

        /// Use max_level for the finest granularity of point cells.
        cell_id = cell_id.parent(static_cast<int>(options.max_level));

        S2CellUnion result;
        result.Init({cell_id});
        return result;
    }

    /// LineString: try native S2Polyline for a tight covering; fall through
    /// to bounding-box if conversion fails.
    if (geometry.kind == DecodedGeometry::Kind::LineString)
    {
        auto polyline = tryConvertLineStringToS2Polyline(std::get<SphericalLineString>(geometry.value));
        if (polyline)
        {
            S2RegionCoverer::Options coverer_options;
            coverer_options.set_max_cells(static_cast<int>(options.max_cells));
            coverer_options.set_min_level(static_cast<int>(options.min_level));
            coverer_options.set_max_level(static_cast<int>(options.max_level));
            S2RegionCoverer coverer(coverer_options);

            S2CellUnion result = coverer.GetCovering(*polyline);
            if (options.normalize_covering)
                result.Normalize();
            if (!result.empty())
                return result;
        }
        /// Fall through to bounding-box path below.
    }

    /// MultiLineString: try native S2Polyline per segment, union results;
    /// fall through to bounding-box if any segment conversion fails.
    if (geometry.kind == DecodedGeometry::Kind::MultiLineString)
    {
        const auto & multi = std::get<SphericalMultiLineString>(geometry.value);
        bool all_converted = true;
        S2CellUnion native_result;

        S2RegionCoverer::Options coverer_options;
        coverer_options.set_max_cells(static_cast<int>(options.max_cells));
        coverer_options.set_min_level(static_cast<int>(options.min_level));
        coverer_options.set_max_level(static_cast<int>(options.max_level));
        S2RegionCoverer coverer(coverer_options);

        for (const auto & linestring : multi)
        {
            auto polyline = tryConvertLineStringToS2Polyline(linestring);
            if (!polyline)
            {
                all_converted = false;
                break;
            }
            S2CellUnion sub = coverer.GetCovering(*polyline);
            if (native_result.empty())
                native_result = std::move(sub);
            else
                native_result = native_result.Union(sub);
        }

        if (all_converted)
        {
            if (options.normalize_covering)
                native_result.Normalize();
            if (!native_result.empty())
                return native_result;
        }
        /// Fall through to bounding-box path below.
    }

    /// Bounding-box path for Ring, Polygon, MultiPolygon, and as fallback
    /// for LineString/MultiLineString when native conversion fails.
    S2LatLngRect rect;
    bool initialized = false;

    if (geometry.kind == DecodedGeometry::Kind::Ring)
    {
        if (!addRingToRect(std::get<SphericalRing>(geometry.value), rect, initialized))
            return std::nullopt;
    }
    else if (geometry.kind == DecodedGeometry::Kind::LineString)
    {
        if (!addLineStringToRect(std::get<SphericalLineString>(geometry.value), rect, initialized))
            return std::nullopt;
    }
    else if (geometry.kind == DecodedGeometry::Kind::MultiLineString)
    {
        const auto & multi = std::get<SphericalMultiLineString>(geometry.value);
        for (const auto & linestring : multi)
        {
            if (!addLineStringToRect(linestring, rect, initialized))
                return std::nullopt;
        }
    }
    else if (geometry.kind == DecodedGeometry::Kind::Polygon)
    {
        if (!addPolygonToRect(std::get<SphericalPolygon>(geometry.value), rect, initialized))
            return std::nullopt;
    }
    else
    {
        const auto & multipolygon = std::get<SphericalMultiPolygon>(geometry.value);
        for (const auto & polygon : multipolygon)
        {
            if (!addPolygonToRect(polygon, rect, initialized))
                return std::nullopt;
        }
    }

    if (!initialized || !rect.is_valid())
        return std::nullopt;

    S2RegionCoverer::Options coverer_options;
    coverer_options.set_max_cells(static_cast<int>(options.max_cells));
    coverer_options.set_min_level(static_cast<int>(options.min_level));
    coverer_options.set_max_level(static_cast<int>(options.max_level));

    S2RegionCoverer coverer(coverer_options);
    S2CellUnion result = coverer.GetCovering(rect);

    if (options.normalize_covering)
        result.Normalize();

    if (result.empty())
        return std::nullopt;

    return result;
}

/// Insert all 6 face-level cells (level 0) for a row that cannot be properly indexed.
/// This ensures the row is never pruned by the S2 projection index, avoiding false negatives.
/// Each S2 face cell covers approximately 1/6 of Earth's surface.
void insertFaceCellsForRow(
    ColumnUInt64 & cell_id_column,
    ColumnUInt64 & parent_offset_column,
    UInt64 parent_offset)
{
    /// S2 has 6 face cells (cube faces) numbered 0-5.
    /// S2CellId::FromFace(face) returns the face cell at level 0.
    for (int face = 0; face < 6; ++face)
    {
        cell_id_column.insertValue(S2CellId::FromFace(face).id());
        parent_offset_column.insertValue(parent_offset);
    }
}

/// ── Filter DAG inspection helpers ────────────────────────────────────────────
/// These helpers inspect the query's ActionsDAG to find and decompose
/// polygonsIntersectSpherical(column, const_polygon) calls.

/// Check if actual_name equals expected_short_name, or is a qualified version
/// (e.g., "db.table.column" ends with ".column").
bool isSameOrQualifiedName(const String & actual_name, const String & expected_short_name)
{
    if (actual_name == expected_short_name)
        return true;

    return actual_name.size() > expected_short_name.size()
        && actual_name.ends_with(expected_short_name)
        && actual_name[actual_name.size() - expected_short_name.size() - 1] == '.';
}

/// Follow ALIAS chains to get the underlying node.
const ActionsDAG::Node * unwrapAlias(const ActionsDAG::Node * node)
{
    while (node && node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
        node = node->children.front();

    return node;
}

/// Check if a DAG node is an INPUT referencing the given source column name.
bool isSourceColumnNode(const ActionsDAG::Node * node, const String & source_column)
{
    node = unwrapAlias(node);
    if (!node || node->type != ActionsDAG::ActionType::INPUT)
        return false;

    return isSameOrQualifiedName(node->result_name, source_column);
}

/// Extract a constant Field value and its DataType from a DAG node.
bool tryGetConstantField(const ActionsDAG::Node * node, Field & field, DataTypePtr & type)
{
    node = unwrapAlias(node);
    if (!node || !node->column || !isColumnConst(*node->column))
        return false;

    const auto & const_column = assert_cast<const ColumnConst &>(*node->column);
    field = const_column.getField();
    type = node->result_type;
    return true;
}

/// Try to extract a constant Float64 value from a DAG node.
bool tryGetConstantFloat64(const ActionsDAG::Node * node, Float64 & result)
{
    Field field;
    DataTypePtr type;
    if (!tryGetConstantField(node, field, type))
        return false;
    try
    {
        result = applyVisitor(FieldVisitorConvertToNumber<Float64>(), field);
        return std::isfinite(result);
    }
    catch (...)
    {
        return false;
    }
}

/// Build a rectangular polygon from bounding box coordinates for S2 covering.
std::optional<DecodedGeometry> buildBoxGeometry(Float64 xmin, Float64 ymin, Float64 xmax, Float64 ymax)
{
    SphericalPolygon polygon;
    polygon.outer().emplace_back(xmin, ymin);
    polygon.outer().emplace_back(xmax, ymin);
    polygon.outer().emplace_back(xmax, ymax);
    polygon.outer().emplace_back(xmin, ymax);
    polygon.outer().emplace_back(xmin, ymin);
    boost::geometry::correct(polygon);

    DecodedGeometry geometry;
    geometry.kind = DecodedGeometry::Kind::Polygon;
    geometry.value = std::move(polygon);
    return geometry;
}

/// Decode a geometry constant from a Field (used for the query's constant geometry argument).
std::optional<DecodedGeometry> decodeGeometryFromField(const Field & field, const DataTypePtr & type)
{
    const auto & factory = DataTypeFactory::instance();
    auto one_row = type->createColumnConst(1, field)->convertToFullColumnIfConst();

    if (factory.get("Point")->equals(*type))
    {
        auto points = ColumnToPointsConverter<SphericalPoint>::convert(one_row);
        if (points.empty())
            return std::nullopt;

        DecodedGeometry geometry;
        geometry.kind = DecodedGeometry::Kind::Point;
        geometry.value = points.front();
        return geometry;
    }

    if (factory.get("Ring")->equals(*type))
    {
        auto rings = ColumnToRingsConverter<SphericalPoint>::convert(one_row);
        if (rings.empty())
            return std::nullopt;

        DecodedGeometry geometry;
        geometry.kind = DecodedGeometry::Kind::Ring;
        geometry.value = rings.front();
        return geometry;
    }

    if (factory.get("LineString")->equals(*type))
    {
        auto linestrings = ColumnToLineStringsConverter<SphericalPoint>::convert(one_row);
        if (linestrings.empty())
            return std::nullopt;

        DecodedGeometry geometry;
        geometry.kind = DecodedGeometry::Kind::LineString;
        geometry.value = linestrings.front();
        return geometry;
    }

    if (factory.get("MultiLineString")->equals(*type))
    {
        auto multilinestrings = ColumnToMultiLineStringsConverter<SphericalPoint>::convert(one_row);
        if (multilinestrings.empty())
            return std::nullopt;

        DecodedGeometry geometry;
        geometry.kind = DecodedGeometry::Kind::MultiLineString;
        geometry.value = multilinestrings.front();
        return geometry;
    }

    if (factory.get("Polygon")->equals(*type))
    {
        auto polygons = ColumnToPolygonsConverter<SphericalPoint>::convert(one_row);
        if (polygons.empty())
            return std::nullopt;

        DecodedGeometry geometry;
        geometry.kind = DecodedGeometry::Kind::Polygon;
        geometry.value = polygons.front();
        return geometry;
    }

    if (factory.get("MultiPolygon")->equals(*type))
    {
        auto multipolygons = ColumnToMultiPolygonsConverter<SphericalPoint>::convert(one_row);
        if (multipolygons.empty())
            return std::nullopt;

        DecodedGeometry geometry;
        geometry.kind = DecodedGeometry::Kind::MultiPolygon;
        geometry.value = multipolygons.front();
        return geometry;
    }

    return std::nullopt;
}

#endif

}

/// ── Public API implementations ───────────────────────────────────────────────

/// Parse the projection declaration and create a ProjectionIndexS2 instance.
/// Expected syntax: PROJECTION <name> INDEX <column> TYPE s2(...)
ProjectionIndexPtr ProjectionIndexS2::create(const ASTProjectionDeclaration & proj)
{
    if (!proj.index)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "ProjectionIndexS2 requires INDEX expression");

    const auto * index_identifier = proj.index->as<ASTIdentifier>();
    if (!index_identifier || !index_identifier->isShort())
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "ProjectionIndexS2 INDEX expression must be a plain column identifier");

    Params params;
    params.source_column = index_identifier->shortName();

    parseTypeArguments(params, proj.type);
    validateParams(params);

    return std::make_shared<ProjectionIndexS2>(std::move(params));
}

/// Fill the ProjectionDescription with metadata for this S2 projection.
/// The projection has two columns:
///   - cell_id (UInt64)        : S2 cell ID, used as the primary/sorting key
///   - _parent_part_offset (UInt64) : row offset in the original part, used to
///                                    map projection matches back to source rows
void ProjectionIndexS2::fillProjectionDescription(
    ProjectionDescription & result,
    const IAST * index_expr,
    const ColumnsDescription & columns,
    ContextPtr query_context) const
{
    if (columns.has("_part_index") || columns.has("_part_offset") || columns.has("_parent_part_offset"))
    {
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "ProjectionIndexS2 cannot be used when table contains columns named `_part_index`, `_part_offset`, or `_parent_part_offset`");
    }

    /// Validate that the INDEX column has a supported geometry type.
    {
        auto col = columns.getPhysical(params.source_column);
        auto col_type = removeNullable(col.type);
        const auto & factory = DataTypeFactory::instance();

        bool is_supported = (col_type->getCustomName() && col_type->getCustomName()->getName() == "Geometry")
            || factory.get("Point")->equals(*col_type)
            || factory.get("Ring")->equals(*col_type)
            || factory.get("LineString")->equals(*col_type)
            || factory.get("MultiLineString")->equals(*col_type)
            || factory.get("Polygon")->equals(*col_type)
            || factory.get("MultiPolygon")->equals(*col_type);

        if (!is_supported)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "ProjectionIndexS2 INDEX column '{}' has unsupported type '{}'. "
                "Supported types: Point, Ring, LineString, MultiLineString, Polygon, MultiPolygon, Geometry (and their Nullable variants)",
                params.source_column,
                col.type->getName());
    }

    result.type = ProjectionDescription::Type::Normal;
    result.required_columns = {params.source_column};
    result.with_parent_part_offset = true;
    result.key_size = 1;

    auto uint64_type = std::make_shared<DataTypeUInt64>();

    result.sample_block = Block{};
    result.sample_block.insert({ColumnUInt64::create(), uint64_type, "cell_id"});
    result.sample_block.insert({ColumnUInt64::create(), uint64_type, "_parent_part_offset"});

    result.sample_block_for_keys = Block{};
    result.sample_block_for_keys.insert({nullptr, uint64_type, "cell_id"});

    /// Keep query AST expression-safe for auxiliary analysis paths.
    auto select_query = make_intrusive<ASTProjectionSelectQuery>();
    auto select_expr_list = make_intrusive<ASTExpressionList>();
    select_expr_list->children.push_back(make_intrusive<ASTIdentifier>("_part_offset"));
    select_query->setExpression(ASTProjectionSelectQuery::Expression::SELECT, std::move(select_expr_list));
    select_query->setExpression(ASTProjectionSelectQuery::Expression::ORDER_BY, index_expr->clone());
    result.query_ast = select_query->cloneToASTSelect();

    StorageInMemoryMetadata metadata;
    metadata.partition_key = KeyDescription::buildEmptyKey();

    ColumnsDescription projection_columns(result.sample_block.getNamesAndTypesList());
    ASTPtr order_ast = make_intrusive<ASTIdentifier>("cell_id");

    metadata.sorting_key = KeyDescription::getSortingKeyFromAST(order_ast, projection_columns, query_context, {});
    metadata.primary_key = KeyDescription::getKeyFromAST(order_ast, projection_columns, query_context);
    metadata.primary_key.definition_ast = nullptr;

    NamesAndTypesList metadata_columns;
    metadata_columns.emplace_back("cell_id", uint64_type);
    metadata_columns.emplace_back("_parent_part_offset", uint64_type);
    metadata.setColumns(ColumnsDescription(metadata_columns));

    result.metadata = std::make_shared<StorageInMemoryMetadata>(metadata);
}

/// Attempt to rewrite a query filter for projection-based index pruning.
///
/// Scans the filter's conjunction atoms for supported spatial predicates:
///   1. Two-argument form: f(source_column, const_polygon) where f is one of
///      polygonsIntersectSpherical, polygonsWithinSpherical, ST_Intersects, etc.
///   2. Box form: ST_IntersectsBox(source_column, xmin, ymin, xmax, ymax)
///   3. Distance form: ST_DWithin(source_column, const_geom, distance_meters)
///
/// If found, builds a multi-cell S2 covering for the constant geometry (or box)
/// and produces a rewritten ActionsDAG:
///   __s2CoveringIntersects(cell_id, [c1, c2, ..., cN])
///
/// The full covering is passed as an Array(UInt64) constant, preserving tight
/// spatial resolution. KeyCondition recognizes `__s2CoveringIntersects` and
/// uses `coveringIntersectsRange` to binary-search the sorted S2CellUnion.
std::optional<ActionsDAG> ProjectionIndexS2::tryRewriteFilterForQuery(const ActionsDAG::Node * filter_node, ContextPtr context) const
{
#if !USE_S2_GEOMETRY
    (void)filter_node;
    (void)context;
    return std::nullopt;
#else
    LOG_DEBUG(log, "Rewrite begin");

    if (!filter_node)
        return std::nullopt;

    /// All spherical spatial predicates can be pruned with S2 covering:
    /// if the predicate is true, the geometries must have spatial overlap,
    /// so S2 cell intersection is a necessary condition (no false negatives).
    static const std::unordered_set<std::string> supported_two_arg_functions = {
        "polygonsIntersectSpherical",
        "polygonsWithinSpherical",
        "ST_Contains",
        "ST_CoveredBy",
        "ST_Covers",
        "ST_Equals",
        "ST_Intersects",
        "ST_Touches",
        "ST_Within",
    };

    const auto atoms = ActionsDAG::extractConjunctionAtoms(filter_node);
    for (const auto * atom : atoms)
    {
        const auto * fn = unwrapAlias(atom);
        if (!fn || fn->type != ActionsDAG::ActionType::FUNCTION)
            continue;

        const auto & func_name = fn->function_base->getName();
        std::optional<DecodedGeometry> geometry;
        double distance_buffer_meters = 0;

        /// Path 1: Two-argument spatial predicates — f(source_column, const_geometry)
        if (supported_two_arg_functions.contains(func_name) && fn->children.size() == 2)
        {
            const ActionsDAG::Node * geometry_const = nullptr;
            if (isSourceColumnNode(fn->children[0], params.source_column))
                geometry_const = fn->children[1];
            else if (isSourceColumnNode(fn->children[1], params.source_column))
                geometry_const = fn->children[0];
            else
                continue;

            Field geometry_field;
            DataTypePtr geometry_type;
            if (!tryGetConstantField(geometry_const, geometry_field, geometry_type))
                continue;

            geometry = decodeGeometryFromField(geometry_field, geometry_type);
        }
        /// Path 2: ST_IntersectsBox(source_column, xmin, ymin, xmax, ymax)
        else if (func_name == "ST_IntersectsBox" && fn->children.size() == 5)
        {
            if (!isSourceColumnNode(fn->children[0], params.source_column))
                continue;

            Float64 xmin, ymin, xmax, ymax;
            if (!tryGetConstantFloat64(fn->children[1], xmin)
                || !tryGetConstantFloat64(fn->children[2], ymin)
                || !tryGetConstantFloat64(fn->children[3], xmax)
                || !tryGetConstantFloat64(fn->children[4], ymax))
                continue;

            geometry = buildBoxGeometry(xmin, ymin, xmax, ymax);
        }
        /// Path 3: ST_DWithin(source_column, const_geometry, distance_meters)
        ///          or ST_DWithin(const_geometry, source_column, distance_meters)
        /// Build an S2 covering for const_geometry, then expand it by the distance
        /// buffer using S2CellUnion::Expand.
        else if (func_name == "ST_DWithin" && fn->children.size() == 3)
        {
            const ActionsDAG::Node * geometry_const = nullptr;
            if (isSourceColumnNode(fn->children[0], params.source_column))
                geometry_const = fn->children[1];
            else if (isSourceColumnNode(fn->children[1], params.source_column))
                geometry_const = fn->children[0];
            else
                continue;

            Float64 dist;
            if (!tryGetConstantFloat64(fn->children[2], dist) || dist < 0)
                continue;

            Field geometry_field;
            DataTypePtr geometry_type;
            if (!tryGetConstantField(geometry_const, geometry_field, geometry_type))
                continue;

            geometry = decodeGeometryFromField(geometry_field, geometry_type);
            distance_buffer_meters = dist;
        }
        else
        {
            continue;
        }

        if (!geometry)
            continue;

        /// Build S2 covering for the query geometry. The full multi-cell covering
        /// is passed to `__s2CoveringIntersects` so that `coveringIntersectsRange`
        /// can binary-search the sorted covering — preserving the tight spatial
        /// resolution instead of diluting it to a single ancestor cell.
        ///
        /// IMPORTANT: We do NOT use the projection's min_level/max_level here.
        /// Those params constrain how data cells are stored in the projection,
        /// but the query covering must cover the region at ALL levels. Using
        /// unconstrained levels lets S2RegionCoverer choose optimal cell sizes
        /// that correctly cover the region without gaps in Hilbert space.
        S2CoveringBuildOptions covering_options;
        covering_options.max_cells = params.max_cells;
        covering_options.min_level = 0;
        covering_options.max_level = 30;
        covering_options.normalize_covering = true;

        auto covering = buildS2Covering(*geometry, covering_options);
        if (!covering || covering->empty())
            continue;

        /// For ST_DWithin, expand the covering by the distance buffer.
        /// S2CellUnion::Expand adds neighboring cells at each level to cover
        /// the specified radius around every cell in the covering.
        if (distance_buffer_meters > 0)
        {
            S1Angle radius = S1Angle::Radians(
                distance_buffer_meters / S2Earth::RadiusMeters());
            covering->Expand(radius, /*max_level_diff=*/4);
        }

        LOG_DEBUG(log, "Query covering cells: {}", covering->size());

        /// Build Array(UInt64) constant with all covering cell IDs.
        Array cell_ids;
        cell_ids.reserve(covering->size());
        for (const auto & cell : *covering)
            cell_ids.push_back(cell.id());

        ActionsDAG rewritten;
        auto uint64_type = std::make_shared<DataTypeUInt64>();
        const auto * cell_id_input = &rewritten.addInput("cell_id", uint64_type);

        auto arr_type = std::make_shared<DataTypeArray>(uint64_type);
        ColumnWithTypeAndName arr_col;
        arr_col.name = "s2_covering";
        arr_col.type = arr_type;
        arr_col.column = arr_type->createColumnConst(1, cell_ids);
        const auto * arr_node = &rewritten.addColumn(std::move(arr_col));

        auto s2_covering_fn = FunctionFactory::instance().get("__s2CoveringIntersects", context);
        const auto * predicate = &rewritten.addFunction(s2_covering_fn, {cell_id_input, arr_node}, {});
        rewritten.getOutputs() = {predicate};

        LOG_DEBUG(log, "Rewrite success");
        return rewritten;
    }

    return std::nullopt;
#endif
}

/// Compute the projection block from source data. This is a 1:N expansion:
/// each source row (containing a geometry) is converted to one or more
/// (cell_id, _parent_part_offset) rows — one per S2 cell in the covering.
/// For Point columns, each row produces exactly one cell.
///
/// Steps:
///   1. Decode geometries from the source column
///   2. For each geometry, build an S2 covering (or single cell for Point)
///   3. Emit (cell_id, parent_offset) for each cell in the covering
///   4. Sort the output by (cell_id, _parent_part_offset)
Block ProjectionIndexS2::calculate(
    const ProjectionDescription & /* projection_desc */,
    const Block & block,
    UInt64 starting_offset,
    ContextPtr /* context */,
    const IColumnPermutation * perm_ptr) const
{
#if !USE_S2_GEOMETRY
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "ProjectionIndexS2 requires S2 support (USE_S2_GEOMETRY)");
#else
    if (!block.has(params.source_column))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "ProjectionIndexS2 source column '{}' not found in block",
            params.source_column);
    }

    const auto & source = block.getByName(params.source_column);

    auto decode_cache = prepareGeometryDecodeCache(source.column, source.type, params.strict_decode);
    if (!decode_cache)
    {
        Block out;
        out.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "cell_id"});
        out.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "_parent_part_offset"});
        return out;
    }

    const size_t rows = block.rows();
    auto parent_offsets = buildParentOffsets(rows, starting_offset, perm_ptr);

    auto cell_id_column = ColumnUInt64::create();
    auto parent_offset_column = ColumnUInt64::create();

    const size_t reserve_rows = rows * std::max<UInt64>(1, params.max_cells);
    cell_id_column->reserve(reserve_rows);
    parent_offset_column->reserve(reserve_rows);

    S2CoveringBuildOptions covering_options;
    covering_options.max_cells = params.max_cells;
    covering_options.min_level = params.min_level;
    covering_options.max_level = params.max_level;

    for (size_t row = 0; row < rows; ++row)
    {
        const UInt64 parent_offset = parent_offsets[row];

        auto geometry = tryDecodeGeometry(*decode_cache, row, params.strict_decode);
        if (!geometry)
        {
            /// Cannot decode geometry — emit face cells to ensure row is never pruned.
            /// This prevents false negatives for rows with malformed geometry data.
            insertFaceCellsForRow(*cell_id_column, *parent_offset_column, parent_offset);
            continue;
        }

        auto covering = buildS2Covering(*geometry, covering_options);
        if (!covering)
        {
            if (params.strict_decode)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to build S2 covering for row {}", row);
            /// Cannot build covering (e.g., degenerate geometry) — emit face cells.
            insertFaceCellsForRow(*cell_id_column, *parent_offset_column, parent_offset);
            continue;
        }

        for (const auto & cell : *covering)
        {
            cell_id_column->insertValue(cell.id());
            parent_offset_column->insertValue(parent_offset);
        }
    }

    Block out;
    out.insert({std::move(cell_id_column), std::make_shared<DataTypeUInt64>(), "cell_id"});
    out.insert({std::move(parent_offset_column), std::make_shared<DataTypeUInt64>(), "_parent_part_offset"});

    sortProjectionIndexS2Block(out);
    return out;
#endif
}

}
