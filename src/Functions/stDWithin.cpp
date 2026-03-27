#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>

#include <cmath>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// Mean Earth radius in meters (same as ClickHouse's geoDistance).
constexpr double EARTH_RADIUS_METERS = 6371007.180918475;

/// Haversine distance between two spherical points (lon/lat in degrees).
/// Returns distance in meters.
double haversineDistance(const SphericalPoint & a, const SphericalPoint & b)
{
    double lon1 = boost::geometry::get<0>(a) * M_PI / 180.0;
    double lat1 = boost::geometry::get<1>(a) * M_PI / 180.0;
    double lon2 = boost::geometry::get<0>(b) * M_PI / 180.0;
    double lat2 = boost::geometry::get<1>(b) * M_PI / 180.0;

    double dlat = lat2 - lat1;
    double dlon = lon2 - lon1;

    double h = std::sin(dlat / 2) * std::sin(dlat / 2)
             + std::cos(lat1) * std::cos(lat2) * std::sin(dlon / 2) * std::sin(dlon / 2);

    /// Clamp h to [0, 1] to avoid NaN from asin(sqrt(h)) when h > 1 due to
    /// floating-point rounding errors (can happen for nearly-antipodal points).
    h = std::min(1.0, std::max(0.0, h));

    return 2.0 * EARTH_RADIUS_METERS * std::asin(std::sqrt(h));
}

/// ST_DWithin(geometry1, geometry2, distance_meters)
///
/// Returns true if the minimum distance between two geometries is <= distance_meters.
/// Distance is measured on a sphere in meters. Modeled after BigQuery's ST_DWITHIN.
///
/// For Point-Point: uses haversine formula.
/// For other combinations: uses boost::geometry::distance (returns radians on sphere)
/// multiplied by Earth radius to get meters.
class FunctionSTDWithin : public IFunction
{
public:
    static inline const char * name = "ST_DWithin";

    explicit FunctionSTDWithin() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSTDWithin>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 3; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isNativeNumber(*arguments[2]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Third argument of function {} (distance) must be a numeric type, got {}",
                getName(),
                arguments[2]->getName());
        return std::make_shared<DataTypeUInt8>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override { return std::make_shared<DataTypeUInt8>(); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto res_column = ColumnUInt8::create();
        auto & res_data = res_column->getData();
        res_data.reserve(input_rows_count);

        auto col_distance = arguments[2].column->convertToFullColumnIfConst();

        callOnTwoGeometryDataTypes<SphericalPoint>(
            arguments[0].type,
            arguments[1].type,
            [&](const auto & left_type, const auto & right_type)
            {
                using LeftConverterType = std::decay_t<decltype(left_type)>;
                using RightConverterType = std::decay_t<decltype(right_type)>;

                using LeftConverter = typename LeftConverterType::Type;
                using RightConverter = typename RightConverterType::Type;

                if constexpr (
                    std::is_same_v<ColumnToLineStringsConverter<SphericalPoint>, LeftConverter>
                    || std::is_same_v<ColumnToLineStringsConverter<SphericalPoint>, RightConverter>)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Any argument of function {} must not be LineString", getName());
                else if constexpr (
                    std::is_same_v<ColumnToMultiLineStringsConverter<SphericalPoint>, LeftConverter>
                    || std::is_same_v<ColumnToMultiLineStringsConverter<SphericalPoint>, RightConverter>)
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Any argument of function {} must not be MultiLineString", getName());
                else
                {
                    constexpr bool left_is_point = std::is_same_v<ColumnToPointsConverter<SphericalPoint>, LeftConverter>;
                    constexpr bool right_is_point = std::is_same_v<ColumnToPointsConverter<SphericalPoint>, RightConverter>;

                    auto first = LeftConverter::convert(arguments[0].column->convertToFullColumnIfConst());
                    auto second = RightConverter::convert(arguments[1].column->convertToFullColumnIfConst());

                    for (size_t i = 0; i < input_rows_count; ++i)
                    {
                        Float64 max_dist_meters = col_distance->getFloat64(i);

                        if constexpr (!left_is_point)
                            boost::geometry::correct(first[i]);
                        if constexpr (!right_is_point)
                            boost::geometry::correct(second[i]);

                        Float64 dist_meters;
                        if constexpr (left_is_point && right_is_point)
                        {
                            /// Point-Point: use haversine (boost::geometry::distance
                            /// for spherical points returns radians, but haversine is
                            /// more direct and well-tested).
                            dist_meters = haversineDistance(first[i], second[i]);
                        }
                        else
                        {
                            /// boost::geometry::distance returns radians for spherical
                            /// coordinate systems. Multiply by Earth radius for meters.
                            double dist_radians = boost::geometry::distance(first[i], second[i]);
                            dist_meters = dist_radians * EARTH_RADIUS_METERS;
                        }

                        res_data.emplace_back(dist_meters <= max_dist_meters);
                    }
                }
            });

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
};

}

REGISTER_FUNCTION(STDWithin)
{
    factory.registerFunction<FunctionSTDWithin>(FunctionDocumentation{
        .description = R"(
Returns true if the minimum distance between two geometries is less than or equal
to the specified distance in meters, measured on the surface of a sphere.

Similar to BigQuery's `ST_DWITHIN`. Supports Point, Ring, Polygon, and MultiPolygon
types in any combination. Operates in spherical coordinates (longitude, latitude in degrees).

Note: function name is case-insensitive, so `st_dwithin`, `ST_DWITHIN`, etc. all work.
    )",
        .syntax = "ST_DWithin(geometry1, geometry2, distance_meters)",
        .arguments
        = {{"geometry1",
            "A value of type [`Point`](/sql-reference/data-types/geo#point), "
            "[`Ring`](/sql-reference/data-types/geo#ring), "
            "[`Polygon`](/sql-reference/data-types/geo#polygon), or "
            "[`MultiPolygon`](/sql-reference/data-types/geo#multipolygon)."},
           {"geometry2",
            "A value of type [`Point`](/sql-reference/data-types/geo#point), "
            "[`Ring`](/sql-reference/data-types/geo#ring), "
            "[`Polygon`](/sql-reference/data-types/geo#polygon), or "
            "[`MultiPolygon`](/sql-reference/data-types/geo#multipolygon)."},
           {"distance_meters", "Maximum distance threshold in meters (Float64)."}},
        .returned_value
        = {"Returns 1 if the geometries are within the specified distance, 0 otherwise. [`UInt8`](/sql-reference/data-types/int-uint)."},
        .examples
        = {{"Two points within 1km",
            R"(
                SELECT ST_DWithin(
                    CAST((0.0, 0.0), 'Point'),
                    CAST((0.001, 0.0), 'Point'),
                    1000)
            )",
            R"(
                ┌─ST_DWithin()─┐
                │            1 │
                └──────────────┘
            )"},
           {"Two points farther than 1km",
            R"(
                SELECT ST_DWithin(
                    CAST((0.0, 0.0), 'Point'),
                    CAST((1.0, 0.0), 'Point'),
                    1000)
            )",
            R"(
                ┌─ST_DWithin()─┐
                │            0 │
                └──────────────┘
            )"}},
        .introduced_in = {25, 8},
        .category = FunctionDocumentation::Category::Geo});
}

}
