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
#include <Functions/s2CoveringBuilder.h>
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
#include <type_traits>
#include <unordered_set>
#include <variant>

#if USE_S2_GEOMETRY
#include <s2/s1angle.h>
#include <s2/s2cell_id.h>
#include <s2/s2cell_union.h>
#include <s2/s2earth.h>
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
/// the rewritten __s2CoveringIntersects predicate is evaluated against the projection.

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

/// Helper: check if inner_type matches a named geometry type and, if so,
/// decode the column using the given Converter and populate the cache.
/// Uses the custom type name for matching to distinguish types with identical
/// underlying structure (e.g. Polygon vs MultiLineString).
template <typename Converter, GeometryDecodeCache::Kind CacheKind>
bool tryDecodeTypedColumn(
    const DataTypePtr & inner_type,
    const ColumnPtr & full_column,
    const NullMap * null_map,
    const String & type_name,
    std::optional<GeometryDecodeCache> & result)
{
    const auto * custom_name = inner_type->getCustomName();
    if (!custom_name || custom_name->getName() != type_name)
        return false;

    auto decoded = Converter::convert(full_column);
    GeometryDecodeCache cache;
    cache.kind = CacheKind;
    cache.rows = decoded.size();
    cache.decoded = std::move(decoded);
    cache.null_map = null_map;
    result = std::move(cache);
    return true;
}

/// Bulk-decode an entire column of geometry values into a cache.
/// Supports Point, Ring, LineString, MultiLineString, Polygon, MultiPolygon, and Geometry (Variant).
/// Returns nullopt if the column type is not a supported geometry type.
std::optional<GeometryDecodeCache> prepareGeometryDecodeCache(const ColumnPtr & column, const DataTypePtr & type, bool strict_mode)
{
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
                return !col.empty() ? &col : nullptr;
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

        std::optional<GeometryDecodeCache> result;
        if (tryDecodeTypedColumn<ColumnToPointsConverter<SphericalPoint>, GeometryDecodeCache::Kind::Point>(
                inner_type, full_column, null_map, "Point", result)
            || tryDecodeTypedColumn<ColumnToRingsConverter<SphericalPoint>, GeometryDecodeCache::Kind::Ring>(
                inner_type, full_column, null_map, "Ring", result)
            || tryDecodeTypedColumn<ColumnToLineStringsConverter<SphericalPoint>, GeometryDecodeCache::Kind::LineString>(
                inner_type, full_column, null_map, "LineString", result)
            || tryDecodeTypedColumn<ColumnToMultiLineStringsConverter<SphericalPoint>, GeometryDecodeCache::Kind::MultiLineString>(
                inner_type, full_column, null_map, "MultiLineString", result)
            || tryDecodeTypedColumn<ColumnToPolygonsConverter<SphericalPoint>, GeometryDecodeCache::Kind::Polygon>(
                inner_type, full_column, null_map, "Polygon", result)
            || tryDecodeTypedColumn<ColumnToMultiPolygonsConverter<SphericalPoint>, GeometryDecodeCache::Kind::MultiPolygon>(
                inner_type, full_column, null_map, "MultiPolygon", result))
            return result;
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

    /// NULL rows in Nullable geometry columns — return nullopt so `calculate`
    /// emits face cells (safe: no false negatives, just no pruning for this row).
    if (cache.null_map && (*cache.null_map)[row])
        return std::nullopt;

    /// Single-type columns: extract the row from the decoded vector.
    if (cache.kind != GeometryDecodeCache::Kind::Geometry)
    {
        return std::visit([row](const auto & vec) -> std::optional<DecodedGeometry>
        {
            using VecT = std::decay_t<decltype(vec)>;
            using ElementT = typename VecT::value_type;

            DecodedGeometry out;
            if constexpr (std::is_same_v<ElementT, SphericalPoint>)
                out.kind = DecodedGeometry::Kind::Point;
            else if constexpr (std::is_same_v<ElementT, SphericalRing>)
                out.kind = DecodedGeometry::Kind::Ring;
            else if constexpr (std::is_same_v<ElementT, SphericalLineString>)
                out.kind = DecodedGeometry::Kind::LineString;
            else if constexpr (std::is_same_v<ElementT, SphericalMultiLineString>)
                out.kind = DecodedGeometry::Kind::MultiLineString;
            else if constexpr (std::is_same_v<ElementT, SphericalPolygon>)
                out.kind = DecodedGeometry::Kind::Polygon;
            else if constexpr (std::is_same_v<ElementT, SphericalMultiPolygon>)
                out.kind = DecodedGeometry::Kind::MultiPolygon;

            out.value = vec[row];
            return out;
        }, cache.decoded);
    }

    /// Geometry (Variant) type: use global discriminator to determine the type of each row.
    const auto & gd = *cache.geometry_data;
    const auto * cv = gd.column_variant;

    auto global_discr = cv->globalDiscriminatorAt(row);
    if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
        return std::nullopt;

    auto offset = cv->offsetAt(row);
    DecodedGeometry out;

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

#if USE_S2_GEOMETRY

/// ── S2 covering computation ──────────────────────────────────────────────────
/// Build an S2 cell covering for a geometry using shared builders from
/// s2CoveringBuilder.h. The bounding-box fallback is handled inside each
/// shared builder function.

/// Build an S2CellUnion covering for a decoded geometry.
/// Delegates to the shared per-type builders from s2CoveringBuilder.h.
std::optional<S2CellUnion> buildS2Covering(
    const DecodedGeometry & geometry,
    const S2CoveringOptions & options)
{
    return std::visit([&](const auto & value) -> std::optional<S2CellUnion>
    {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, SphericalPoint>)
            return buildPointCovering(value, options);
        else if constexpr (std::is_same_v<T, SphericalLineString>)
            return buildLineStringCovering(value, options);
        else if constexpr (std::is_same_v<T, SphericalMultiLineString>)
            return buildMultiLineStringCovering(value, options);
        else if constexpr (std::is_same_v<T, SphericalRing>)
            return buildRingCovering(value, options);
        else if constexpr (std::is_same_v<T, SphericalPolygon>)
            return buildPolygonCovering(value, options);
        else if constexpr (std::is_same_v<T, SphericalMultiPolygon>)
            return buildMultiPolygonCovering(value, options);
        else
            return std::nullopt;
    }, geometry.value);
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
        /// Ok: conversion failure means this is not a usable constant number.
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

/// Helper: try to decode a single geometry from a one-row column of a specific type.
/// Uses the custom type name (e.g. "Polygon", "MultiLineString") for matching,
/// not just the underlying data structure, because some geometry types share
/// the same structure (e.g. Polygon = Array(Array(Point)) = MultiLineString).
template <typename Converter, DecodedGeometry::Kind GeoKind>
bool tryDecodeFieldAsType(
    const DataTypePtr & type,
    const ColumnPtr & one_row,
    const String & type_name,
    std::optional<DecodedGeometry> & result)
{
    /// Check custom name first — this distinguishes types with identical
    /// underlying structure (Polygon vs MultiLineString, Ring vs LineString).
    const auto * custom_name = type->getCustomName();
    if (!custom_name || custom_name->getName() != type_name)
        return false;

    auto decoded = Converter::convert(one_row);
    if (decoded.empty())
        return false;

    DecodedGeometry geometry;
    geometry.kind = GeoKind;
    geometry.value = std::move(decoded.front());
    result = std::move(geometry);
    return true;
}

/// Decode a geometry constant from a Field (used for the query's constant geometry argument).
std::optional<DecodedGeometry> decodeGeometryFromField(const Field & field, const DataTypePtr & type)
{
    auto one_row = type->createColumnConst(1, field)->convertToFullColumnIfConst();

    std::optional<DecodedGeometry> result;
    if (tryDecodeFieldAsType<ColumnToPointsConverter<SphericalPoint>, DecodedGeometry::Kind::Point>(
            type, one_row, "Point", result)
        || tryDecodeFieldAsType<ColumnToRingsConverter<SphericalPoint>, DecodedGeometry::Kind::Ring>(
            type, one_row, "Ring", result)
        || tryDecodeFieldAsType<ColumnToLineStringsConverter<SphericalPoint>, DecodedGeometry::Kind::LineString>(
            type, one_row, "LineString", result)
        || tryDecodeFieldAsType<ColumnToMultiLineStringsConverter<SphericalPoint>, DecodedGeometry::Kind::MultiLineString>(
            type, one_row, "MultiLineString", result)
        || tryDecodeFieldAsType<ColumnToPolygonsConverter<SphericalPoint>, DecodedGeometry::Kind::Polygon>(
            type, one_row, "Polygon", result)
        || tryDecodeFieldAsType<ColumnToMultiPolygonsConverter<SphericalPoint>, DecodedGeometry::Kind::MultiPolygon>(
            type, one_row, "MultiPolygon", result))
        return result;

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

    const IAST * index_ast = proj.index;

    if (const auto * index_list = index_ast->as<ASTExpressionList>())
    {
        if (index_list->children.size() != 1)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "ProjectionIndexS2 requires exactly one INDEX column");
        index_ast = index_list->children[0].get();
    }

    if (const auto * tuple_func = index_ast->as<ASTFunction>(); tuple_func && tuple_func->name == "tuple" && tuple_func->arguments)
    {
        if (tuple_func->arguments->children.size() != 1)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "ProjectionIndexS2 requires exactly one INDEX column");
        index_ast = tuple_func->arguments->children[0].get();
    }

    const auto * index_identifier = index_ast->as<ASTIdentifier>();
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
    const KeyDescription * partition_key,
    const ContextPtr & query_context) const
{
    (void) partition_key;

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

    metadata.sorting_key = KeyDescription::getKeyFromAST(order_ast, projection_columns, {}, query_context, {});
    metadata.primary_key = KeyDescription::getKeyFromAST(order_ast, projection_columns, {}, query_context);
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
        "polygonsIntersectCartesian",
        "polygonsIntersectSpherical",
        "polygonsWithinCartesian",
        "polygonsWithinSpherical",
        "geoContainsCartesian",
        "geoContainsSpherical",
        "geoCoveredByCartesian",
        "geoCoveredBySpherical",
        "geoCoversCartesian",
        "geoCoversSpherical",
        "geoEqualsCartesian",
        "geoEqualsSpherical",
        "geoIntersectsCartesian",
        "geoIntersectsSpherical",
        "geoTouchesCartesian",
        "geoTouchesSpherical",
        "geoWithinCartesian",
        "geoWithinSpherical",
        // "ST_Contains",
        // "ST_CoveredBy",
        // "ST_Covers",
        // "ST_Equals",
        // "ST_Intersects",
        // "ST_Touches",
        // "ST_Within",
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
        else if ((func_name == "geoIntersectsBoxCartesian" || func_name == "geoIntersectsBoxSpherical") && fn->children.size() == 5)
        {
            if (!isSourceColumnNode(fn->children[0], params.source_column))
                continue;

            Float64 xmin;
            Float64 ymin;
            Float64 xmax;
            Float64 ymax;
            if (!tryGetConstantFloat64(fn->children[1], xmin)
                || !tryGetConstantFloat64(fn->children[2], ymin)
                || !tryGetConstantFloat64(fn->children[3], xmax)
                || !tryGetConstantFloat64(fn->children[4], ymax))
                continue;

            geometry = buildBoxGeometry(xmin, ymin, xmax, ymax);
        }
        /// Path 3: spherical ST_DWithin(source_column, const_geometry, distance_meters)
        ///          or spherical ST_DWithin(const_geometry, source_column, distance_meters)
        /// Build an S2 covering for const_geometry, then expand it by the distance
        /// buffer (meters) using S2CellUnion::Expand.
        ///
        /// Note: `geoDWithinCartesian` distance is in cartesian coordinate units,
        /// not meters, so it cannot be safely converted to an S2 angular buffer.
        /// Skip cartesian variant to preserve conservative pruning.
        else if (func_name == "geoDWithinSpherical" && fn->children.size() == 3)
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
        /// We use the projection's min_level to match the data-side cell levels,
        /// and max_level=30 to allow the coverer full flexibility upward.
        S2CoveringOptions covering_options;
        covering_options.max_cells = static_cast<int>(params.max_cells);
        covering_options.min_level = static_cast<int>(params.min_level);
        covering_options.max_level = 30;

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

        LOG_DEBUG(log, "Query geometry: {}, has {} covering cells: {}", geometry->kind, covering->size(), covering->ToString());

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
    (void)block;
    (void)starting_offset;
    (void)perm_ptr;
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

    S2CoveringOptions covering_options;
    covering_options.max_cells = static_cast<int>(params.max_cells);
    covering_options.min_level = static_cast<int>(params.min_level);
    covering_options.max_level = static_cast<int>(params.max_level);

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
            if (geometry->kind != DecodedGeometry::Kind::Point)
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
