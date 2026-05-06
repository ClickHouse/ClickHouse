#include "config.h"

#if USE_S2_GEOMETRY

#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>
#include <Functions/s2CoveringBuilder.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>

#include <boost/geometry.hpp>

#include <s2/s2cell_union.h>

#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// ── Ramer-Douglas-Peucker simplification helpers ────────────────────────────
///
/// Reduce the number of vertices in a geometry before S2 conversion.
/// The epsilon (in degrees) is chosen to be 1/10 of the S2 cell edge length at
/// max_level, ensuring that simplification errors are invisible at the target
/// S2 resolution. For max_level=30 this is ~8e-9°; for max_level=16 ~5e-4°.
///
/// boost::geometry::simplify implements RDP natively for all ring / polygon
/// types in spherical_equatorial<degree> coordinates.  The distance measure is
/// Euclidean in (lon, lat) space, which is a valid approximation for the small
/// deviations introduced by RDP at geographic scales.
///
/// All helpers fall back to the original geometry when simplification would
/// produce a degenerate result (< 3 unique vertices for a ring).

/// Compute the RDP epsilon in degrees for the given S2 max_level.
double rdpEpsilonForLevel(int max_level)
{
    /// S2 cell edge ≈ 360° / (4 * 2^max_level).  Use 10% of that as epsilon.
    return 360.0 / (4.0 * static_cast<double>(1LL << max_level)) * 0.1;
}

/// Simplify a closed boost Ring with RDP.
/// Boost rings are Closed=true so they carry a duplicate closing vertex;
/// the minimum valid simplified ring has 4 entries (3 unique + 1 closing).
SphericalRing simplifyRing(const SphericalRing & ring, double epsilon)
{
    SphericalRing result;
    boost::geometry::simplify(ring, result, epsilon);
    if (result.size() < 4)
        return ring; /// fallback: keep original
    return result;
}

/// Simplify all rings of a Polygon with RDP.
SphericalPolygon simplifyPolygon(const SphericalPolygon & polygon, double epsilon)
{
    SphericalPolygon result;
    boost::geometry::simplify(polygon.outer(), result.outer(), epsilon);

    if (result.outer().size() < 4)
        return polygon; /// fallback: keep original

    result.inners().resize(polygon.inners().size());
    for (size_t k = 0; k < polygon.inners().size(); ++k)
    {
        boost::geometry::simplify(polygon.inners()[k], result.inners()[k], epsilon);

        if (result.inners()[k].size() < 4)
            result.inners()[k] = polygon.inners()[k]; /// fallback for this hole
    }
    return result;
}

/// Simplify every polygon in a MultiPolygon with RDP.
SphericalMultiPolygon simplifyMultiPolygon(const SphericalMultiPolygon & multi, double epsilon)
{
    SphericalMultiPolygon result;
    result.reserve(multi.size());
    for (const auto & poly : multi)
        result.push_back(simplifyPolygon(poly, epsilon));
    return result;
}

/// Extract cell IDs from an S2CellUnion into a vector.
std::vector<UInt64> extractCellIds(const S2CellUnion & covering)
{
    std::vector<UInt64> ids;
    ids.reserve(covering.size());
    for (const auto & cell : covering)
        ids.push_back(cell.id());
    return ids;
}

/// geoToS2Cells(geometry, max_cells, min_level, max_level)
///
/// Converts a geometry to an array of S2 cell IDs that cover it.
/// This mirrors the S2 covering logic from `buildS2Covering`
/// in ProjectionIndexS2.
///
/// For Point: returns an array with one cell at max_level.
/// For Ring/Polygon: converts to native S2Polygon for a tight covering.
/// For LineString: converts to native S2Polyline for a tight covering.
/// Falls back to bounding-box covering if native conversion fails.
///
/// Returns an empty array for invalid or degenerate geometries.
class FunctionGeoToS2Cells : public IFunction
{
public:
    static constexpr auto name = "geoToS2Cells";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGeoToS2Cells>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 4 || arguments.size() > 5)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 4 or 5 arguments, got {}",
                getName(), arguments.size());

        for (size_t i = 1; i < 4; ++i)
        {
            if (!isNativeInteger(arguments[i]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument {} of function {}. Must be an integer type",
                    arguments[i]->getName(), i + 1, getName());
        }

        if (arguments.size() == 5 && !isNativeInteger(arguments[4]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument 5 of function {}. Must be an integer type (0 = false, non-zero = true)",
                arguments[4]->getName(), getName());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto col_data = ColumnUInt64::create();
        auto & data_vec = col_data->getData();

        auto col_offsets = ColumnArray::ColumnOffsets::create();
        auto & offsets_vec = col_offsets->getData();
        offsets_vec.resize(input_rows_count);

        /// Extract integer parameters (max_cells, min_level, max_level).
        auto get_int_column = [&](size_t arg_idx) -> ColumnPtr
        {
            return arguments[arg_idx].column->convertToFullColumnIfConst();
        };

        auto col_max_cells = get_int_column(1);
        auto col_min_level = get_int_column(2);
        auto col_max_level = get_int_column(3);

        /// Read optional using_rdp argument (arg index 4, default = false).
        const bool has_rdp_arg = (arguments.size() == 5);
        ColumnPtr col_using_rdp;
        if (has_rdp_arg)
            col_using_rdp = arguments[4].column->convertToFullColumnIfConst();

        callOnGeometryDataType<SphericalPoint>(
            arguments[0].type,
            [&](const auto & converter_type)
            {
                using ConverterType = std::decay_t<decltype(converter_type)>;
                using Converter = typename ConverterType::Type;

                constexpr bool is_point = std::is_same_v<ColumnToPointsConverter<SphericalPoint>, Converter>;
                constexpr bool is_ring = std::is_same_v<ColumnToRingsConverter<SphericalPoint>, Converter>;
                constexpr bool is_linestring = std::is_same_v<ColumnToLineStringsConverter<SphericalPoint>, Converter>;
                constexpr bool is_multilinestring = std::is_same_v<ColumnToMultiLineStringsConverter<SphericalPoint>, Converter>;
                constexpr bool is_polygon = std::is_same_v<ColumnToPolygonsConverter<SphericalPoint>, Converter>;
                // else: MultiPolygon

                auto geometries = Converter::convert(arguments[0].column->convertToFullColumnIfConst());
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    const auto max_cells_val = col_max_cells->getUInt(i);
                    const auto min_level_val = col_min_level->getUInt(i);
                    const auto max_level_val = col_max_level->getUInt(i);

                    if (min_level_val > 30 || max_level_val > 30 || min_level_val > max_level_val || max_cells_val < 1)
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Invalid S2 covering parameters in function {}: max_cells must be >= 1, "
                            "min_level and max_level must be in [0, 30], and min_level <= max_level",
                            getName());

                    const auto max_cells = static_cast<int>(max_cells_val);
                    const auto min_level = static_cast<int>(min_level_val);
                    const auto max_level = static_cast<int>(max_level_val);

                    const bool using_rdp = has_rdp_arg && (col_using_rdp->getUInt(i) != 0);
                    const double rdp_epsilon = using_rdp ? rdpEpsilonForLevel(max_level) : 0.0;

                    std::optional<S2CellUnion> covering;

                    if constexpr (is_point)
                    {
                        covering = buildPointCovering(geometries[i], max_level);
                    }
                    else if constexpr (is_linestring)
                    {
                        covering = buildLineStringCovering(geometries[i], max_cells, min_level, max_level);
                    }
                    else if constexpr (is_multilinestring)
                    {
                        covering = buildMultiLineStringCovering(geometries[i], max_cells, min_level, max_level);
                    }
                    else if constexpr (is_ring)
                    {
                        if (using_rdp)
                        {
                            SphericalRing simplified = simplifyRing(geometries[i], rdp_epsilon);
                            covering = buildRingCovering(simplified, max_cells, min_level, max_level);
                        }
                        else
                        {
                            covering = buildRingCovering(geometries[i], max_cells, min_level, max_level);
                        }
                    }
                    else if constexpr (is_polygon)
                    {
                        if (using_rdp)
                        {
                            SphericalPolygon simplified = simplifyPolygon(geometries[i], rdp_epsilon);
                            covering = buildPolygonCovering(simplified, max_cells, min_level, max_level);
                        }
                        else
                        {
                            covering = buildPolygonCovering(geometries[i], max_cells, min_level, max_level);
                        }
                    }
                    else
                    {
                        /// MultiPolygon
                        if (using_rdp)
                        {
                            SphericalMultiPolygon simplified = simplifyMultiPolygon(geometries[i], rdp_epsilon);
                            covering = buildMultiPolygonCovering(simplified, max_cells, min_level, max_level);
                        }
                        else
                        {
                            covering = buildMultiPolygonCovering(geometries[i], max_cells, min_level, max_level);
                        }
                    }

                    if (covering)
                    {
                        auto ids = extractCellIds(*covering);
                        for (const auto & id : ids)
                            data_vec.push_back(id);
                        offsets_vec[i] = data_vec.size();
                    }
                    else
                    {
                        /// Invalid geometry — return empty array.
                        offsets_vec[i] = data_vec.size();
                    }
                }
            });

        return ColumnArray::create(std::move(col_data), std::move(col_offsets));
    }
};

}

REGISTER_FUNCTION(GeoToS2Cells)
{
    factory.registerFunction<FunctionGeoToS2Cells>(FunctionDocumentation{
        .description = R"(
Returns an array of S2 cell IDs that cover the given geometry.

This function mirrors the S2 covering logic used by the S2 projection index
(`TYPE s2`) for spatial indexing. For polygon-like geometries (Ring, Polygon,
MultiPolygon), it converts them to native S2 types (S2Loop/S2Polygon) to
produce tight coverings that follow the actual shape boundary. For linestrings,
it uses native S2Polyline coverings. Falls back to a bounding-box covering
only when native S2 type conversion fails (e.g., degenerate geometry).

For a `Point`: returns an array with a single S2 cell ID at `max_level`.
For `Ring`, `Polygon`, `MultiPolygon`: converts to native S2Polygon and
computes a tight covering with the specified parameters.
For `LineString`, `MultiLineString`: converts to native S2Polyline and
computes a tight covering with the specified parameters.

When `using_rdp` is set to a non-zero value, the Ramer-Douglas-Peucker (RDP)
algorithm is applied to Ring, Polygon, and MultiPolygon geometries before S2
conversion. This reduces the vertex count, which can significantly speed up S2
polygon construction and covering computation for complex geometries with many
vertices. The RDP epsilon is automatically derived from `max_level` (1/10 of
the S2 cell edge length at that level), ensuring that the simplification error
is invisible at the target S2 resolution. Degenerate simplification results
(fewer than 3 unique vertices) fall back to the original geometry.

Returns an empty array for invalid or degenerate geometries (NaN coordinates, empty geometries).
    )",
        .syntax = "geoToS2Cells(geometry, max_cells, min_level, max_level[, using_rdp])",
        .arguments
        = {{"geometry",
            "A value of type [`Point`](/sql-reference/data-types/geo#point), "
            "[`Ring`](/sql-reference/data-types/geo#ring), "
            "[`LineString`](/sql-reference/data-types/geo#linestring), "
            "[`MultiLineString`](/sql-reference/data-types/geo#multilinestring), "
            "[`Polygon`](/sql-reference/data-types/geo#polygon), or "
            "[`MultiPolygon`](/sql-reference/data-types/geo#multipolygon)."},
           {"max_cells", "Maximum number of S2 cells in the covering. [`UInt64`](/sql-reference/data-types/int-uint)."},
           {"min_level", "Minimum S2 cell level (0 = coarsest, 30 = finest). [`UInt64`](/sql-reference/data-types/int-uint)."},
           {"max_level", "Maximum S2 cell level (0 = coarsest, 30 = finest). [`UInt64`](/sql-reference/data-types/int-uint)."},
           {"using_rdp",
            "Optional. When non-zero, applies the Ramer-Douglas-Peucker algorithm to simplify Ring, Polygon, and "
            "MultiPolygon geometries before S2 conversion. Reduces computation time for complex polygons. "
            "Defaults to 0 (disabled). [`UInt8`](/sql-reference/data-types/int-uint)."}},
        .returned_value
        = {"An array of S2 cell IDs (UInt64) covering the geometry, or an empty array for invalid input. "
           "[`Array(UInt64)`](/sql-reference/data-types/array)."},
        .examples
        = {{"Point",
            "SELECT geoToS2Cells(CAST((37.79506683, 55.71290588), 'Point'), 8, 16, 20)",
            "[4704772434919038976]"},
           {"Polygon",
            R"(
                SELECT geoToS2Cells(
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'),
                    8, 1, 10)
            )",
            "[1153277837422952448,1155173329567391744,1155454804544102400,...]"}},
        .introduced_in = {25, 8},
        .category = FunctionDocumentation::Category::Geo});
}

}

#endif
