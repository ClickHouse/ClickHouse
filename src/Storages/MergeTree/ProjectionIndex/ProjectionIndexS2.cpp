#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexS2.h>

#include "config.h"

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeFactory.h>
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
#include <variant>

#if USE_S2_GEOMETRY
#include <s2/s2cell_id.h>
#include <s2/s2cell_union.h>
#include <s2/s2latlng.h>
#include <s2/s2latlng_rect.h>
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
/// These helpers decode Polygon/MultiPolygon values from ClickHouse columns into
/// boost::geometry types suitable for S2 bounding-box computation.

struct DecodedGeometry
{
    enum class Kind
    {
        Polygon,
        MultiPolygon
    };

    Kind kind;
    std::variant<SphericalPolygon, SphericalMultiPolygon> value;
};

struct GeometryDecodeCache
{
    enum class Kind
    {
        Polygon,
        MultiPolygon
    };

    Kind kind;
    size_t rows = 0;
    std::variant<std::vector<SphericalPolygon>, std::vector<SphericalMultiPolygon>> decoded;
};

/// Bulk-decode an entire column of Polygon or MultiPolygon values into a cache.
/// Returns nullopt if the column type is not a supported geometry type.
std::optional<GeometryDecodeCache> prepareGeometryDecodeCache(const ColumnPtr & column, const DataTypePtr & type, bool strict_mode)
{
    const auto & factory = DataTypeFactory::instance();
    ColumnPtr full_column = column->convertToFullColumnIfConst();

    try
    {
        if (factory.get("Polygon")->equals(*type))
        {
            auto decoded = ColumnToPolygonsConverter<SphericalPoint>::convert(full_column);
            GeometryDecodeCache cache;
            cache.kind = GeometryDecodeCache::Kind::Polygon;
            cache.rows = decoded.size();
            cache.decoded = std::move(decoded);
            return cache;
        }

        if (factory.get("MultiPolygon")->equals(*type))
        {
            auto decoded = ColumnToMultiPolygonsConverter<SphericalPoint>::convert(full_column);
            GeometryDecodeCache cache;
            cache.kind = GeometryDecodeCache::Kind::MultiPolygon;
            cache.rows = decoded.size();
            cache.decoded = std::move(decoded);
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
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "ProjectionIndexS2 supports only Polygon/MultiPolygon, got {}", type->getName());

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

    DecodedGeometry out;

    if (cache.kind == GeometryDecodeCache::Kind::Polygon)
    {
        out.kind = DecodedGeometry::Kind::Polygon;
        out.value = std::get<std::vector<SphericalPolygon>>(cache.decoded)[row];
        return out;
    }

    out.kind = DecodedGeometry::Kind::MultiPolygon;
    out.value = std::get<std::vector<SphericalMultiPolygon>>(cache.decoded)[row];
    return out;
}

#if USE_S2_GEOMETRY

/// ── S2 covering computation ──────────────────────────────────────────────────
/// Build an S2 cell covering for a geometry. Instead of using S2Polygon (which
/// has linker issues with S2ValidationQueryBase), we compute an S2LatLngRect
/// bounding box from all polygon vertices and then cover that rectangle.
/// This is a conservative approximation (may include more area than the actual
/// polygon), but it is correct: no false negatives, only potential false positives.

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

/// Build an S2CellUnion covering for the bounding box of a geometry.
/// Uses S2RegionCoverer to generate a set of S2 cells that cover the bounding rectangle.
std::optional<S2CellUnion> buildS2CoveringForGeometry(
    const DecodedGeometry & geometry,
    const S2CoveringBuildOptions & options)
{
    S2LatLngRect rect;
    bool initialized = false;

    if (geometry.kind == DecodedGeometry::Kind::Polygon)
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

/// Decode a geometry constant from a Field (used for the query's constant polygon argument).
std::optional<DecodedGeometry> decodeGeometryFromField(const Field & field, const DataTypePtr & type)
{
    const auto & factory = DataTypeFactory::instance();
    auto one_row = type->createColumnConst(1, field)->convertToFullColumnIfConst();

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

/// ── Single ancestor cell strategy ────────────────────────────────────────────
/// Given a covering (set of S2 cells), find the smallest single S2 cell that
/// contains ALL cells in the covering. This "ancestor cell" is used as the
/// constant argument in the rewritten s2CellsIntersect(cell_id, ancestor) predicate.
/// The ancestor is found by walking up from the first cell's level toward level 0
/// and checking containment. This simplifies the rewrite to a single-cell comparison.
std::optional<UInt64> buildSingleAncestorCellId(const S2CellUnion & covering)
{
    if (covering.empty())
        return std::nullopt;

    const S2CellId first = *covering.begin();
    for (int level = first.level(); level >= 0; --level)
    {
        S2CellId candidate = first.parent(level);
        bool contains_all = true;
        for (const auto & cell : covering)
        {
            if (!candidate.contains(cell))
            {
                contains_all = false;
                break;
            }
        }

        if (contains_all)
            return candidate.id();
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
/// Scans the filter's conjunction atoms for:
///   polygonsIntersectSpherical(source_column, const_polygon)
/// If found, builds an S2 covering for const_polygon, compresses it into a
/// single ancestor cell, and produces a new ActionsDAG:
///   s2CellsIntersect(cell_id, <ancestor_cell_id>)
///
/// This rewritten DAG is then used by the generic projection index framework
/// (in projectionsCommon.cpp) to prune marks via the projection's primary key.
std::optional<ActionsDAG> ProjectionIndexS2::tryRewriteFilterForQuery(const ActionsDAG::Node * filter_node, ContextPtr context) const
{
#if !USE_S2_GEOMETRY
    (void)filter_node;
    (void)context;
    return std::nullopt;
#else
    LOG_DEBUG(getLogger("ProjectionIndexS2"), "Rewrite begin");

    if (!filter_node)
        return std::nullopt;

    const auto atoms = ActionsDAG::extractConjunctionAtoms(filter_node);
    for (const auto * atom : atoms)
    {
        const auto * fn = unwrapAlias(atom);
        if (!fn || fn->type != ActionsDAG::ActionType::FUNCTION)
            continue;

        /// Both polygonsIntersectSpherical and polygonsWithinSpherical can be pruned:
        /// if A is within B then A necessarily intersects B, so the same S2 covering
        /// strategy is sound for both (no false negatives).
        const auto & func_name = fn->function_base->getName();
        if (func_name != "polygonsIntersectSpherical" && func_name != "polygonsWithinSpherical")
            continue;

        if (fn->children.size() != 2)
            continue;

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

        auto geometry = decodeGeometryFromField(geometry_field, geometry_type);
        if (!geometry)
            continue;

        S2CoveringBuildOptions covering_options;
        covering_options.max_cells = params.max_cells;
        covering_options.min_level = params.min_level;
        covering_options.max_level = params.max_level;

        auto covering = buildS2CoveringForGeometry(*geometry, covering_options);
        if (!covering)
            continue;

        auto ancestor = buildSingleAncestorCellId(*covering);
        if (!ancestor)
            continue;

        ActionsDAG rewritten;
        auto uint64_type = std::make_shared<DataTypeUInt64>();
        const auto * cell_id_input = &rewritten.addInput("cell_id", uint64_type);

        ColumnWithTypeAndName ancestor_col;
        ancestor_col.name = std::to_string(*ancestor);
        ancestor_col.type = uint64_type;
        ancestor_col.column = uint64_type->createColumnConst(1, *ancestor);
        const auto * ancestor_node = &rewritten.addColumn(std::move(ancestor_col));

        auto s2_intersect = FunctionFactory::instance().get("s2CellsIntersect", context);
        const auto * predicate = &rewritten.addFunction(s2_intersect, {cell_id_input, ancestor_node}, {});
        rewritten.getOutputs() = {predicate};

        LOG_DEBUG(getLogger("ProjectionIndexS2"), "Rewrite success");
        return rewritten;
    }

    return std::nullopt;
#endif
}

/// Compute the projection block from source data. This is a 1:N expansion:
/// each source row (containing a Polygon/MultiPolygon) is converted to multiple
/// (cell_id, _parent_part_offset) rows — one for each S2 cell in the covering.
///
/// Steps:
///   1. Decode geometries from the source column
///   2. For each geometry, build an S2 bounding-box covering
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
        auto geometry = tryDecodeGeometry(*decode_cache, row, params.strict_decode);
        if (!geometry)
            continue;

        auto covering = buildS2CoveringForGeometry(*geometry, covering_options);
        if (!covering)
        {
            if (params.strict_decode)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to build S2 covering for row {}", row);
            continue;
        }

        const UInt64 parent_offset = parent_offsets[row];
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
