#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>
#include <Functions/geometryConstOptimization.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

#include <memory>

namespace DB
{

namespace Setting
{
    extern const SettingsBool st_function_use_spherical;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// Equality function: returns true if two geometries are topologically equal,
/// i.e., each one covers the other. Modeled after BigQuery's ST_EQUALS.
///
/// Implemented as: covered_by(A, B) AND covered_by(B, A)
/// This avoids potential boost::geometry::equals compilation issues with certain
/// type combinations while providing correct topological equality semantics.
/// Templated on Point to support both Spherical and Cartesian coordinate systems.
template <typename Point>
class FunctionGeoEquals : public IFunction
{
public:
    static inline const char * name;

    explicit FunctionGeoEquals() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGeoEquals>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeUInt8>(); }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override { return std::make_shared<DataTypeUInt8>(); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto res_column = ColumnUInt8::create();
        auto & res_data = res_column->getData();
        res_data.reserve(input_rows_count);

        bool left_is_const = isColumnConst(*arguments[0].column);
        bool right_is_const = isColumnConst(*arguments[1].column);

        callOnTwoGeometryDataTypes<Point>(
            arguments[0].type,
            arguments[1].type,
            [&](const auto & left_type, const auto & right_type)
            {
                using LeftConverterType = std::decay_t<decltype(left_type)>;
                using RightConverterType = std::decay_t<decltype(right_type)>;

                using LeftConverter = typename LeftConverterType::Type;
                using RightConverter = typename RightConverterType::Type;

                if constexpr (
                    std::is_same_v<ColumnToLineStringsConverter<Point>, LeftConverter>
                    || std::is_same_v<ColumnToLineStringsConverter<Point>, RightConverter>)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Any argument of function {} must not be LineString", getName());
                else if constexpr (
                    std::is_same_v<ColumnToMultiLineStringsConverter<Point>, LeftConverter>
                    || std::is_same_v<ColumnToMultiLineStringsConverter<Point>, RightConverter>)
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Any argument of function {} must not be MultiLineString", getName());
                else
                {
                    constexpr bool left_is_point = std::is_same_v<ColumnToPointsConverter<Point>, LeftConverter>;
                    constexpr bool right_is_point = std::is_same_v<ColumnToPointsConverter<Point>, RightConverter>;

                    /// Different dimensionality types can never be equal:
                    /// A Point cannot equal a Polygon/Ring/MultiPolygon, and vice versa.
                    /// covered_by(Polygon, Point) is not implemented in boost and is logically false.
                    if constexpr (left_is_point != right_is_point)
                    {
                        res_data.resize_fill(input_rows_count, 0);
                    }
                    else
                    {
                        /// geoEquals(A, B) = covered_by(A, B) AND covered_by(B, A)
                        executeGeometryPredicate<Point, LeftConverter, RightConverter, left_is_point, right_is_point>(
                            arguments, res_data, input_rows_count, left_is_const, right_is_const,
                            [](const auto & a, const auto & b)
                            {
                                return boost::geometry::covered_by(a, b)
                                    && boost::geometry::covered_by(b, a);
                            });
                    }
                }
            });

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override { return false; }
};

template <>
const char * FunctionGeoEquals<CartesianPoint>::name = "geoEqualsCartesian";

template <>
const char * FunctionGeoEquals<SphericalPoint>::name = "geoEqualsSpherical";

}

REGISTER_FUNCTION(GeoEquals)
{
    factory.registerFunction<FunctionGeoEquals<CartesianPoint>>(FunctionDocumentation{
        .description = R"(
Returns true if two geometries are topologically equal using Cartesian (flat/planar)
coordinates, meaning each geometry covers the other. Two geometries are equal if they
represent the same spatial region, even if their vertex orderings differ.

Equivalent to `geoCoversCartesian(geometry1, geometry2) AND geoCoversCartesian(geometry2, geometry1)`.

Supports Point, Ring, Polygon, and MultiPolygon types in any combination.

    )",
        .syntax = "geoEqualsCartesian(geometry1, geometry2)",
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
            "[`MultiPolygon`](/sql-reference/data-types/geo#multipolygon)."}},
        .returned_value
        = {"Returns 1 if the two geometries are equal, 0 otherwise. [`UInt8`](/sql-reference/data-types/int-uint)."},
        .examples
        = {{"Same polygon",
            R"(
                SELECT geoEqualsCartesian(
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'),
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─geoEqualsCartesian()─┐
                │                    1 │
                └──────────────────────┘
            )"},
           {"Different polygons",
            R"(
                SELECT geoEqualsCartesian(
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'),
                    CAST([[(0.0, 0.0), (5.0, 0.0), (5.0, 5.0), (0.0, 5.0), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─geoEqualsCartesian()─┐
                │                    0 │
                └──────────────────────┘
            )"}},
        .introduced_in = {25, 9},
        .category = FunctionDocumentation::Category::Geo});

    /// ST_Equals dispatches to geoEqualsSpherical or geoEqualsCartesian
    /// based on the `st_function_use_spherical` setting.
    factory.registerFunction("ST_Equals", [](ContextPtr context) -> FunctionPtr
    {
        if (context->getSettingsRef()[Setting::st_function_use_spherical])
            return FunctionGeoEquals<SphericalPoint>::create(context);
        else
            return FunctionGeoEquals<CartesianPoint>::create(context);
    }, FunctionDocumentation{
        .description = R"(
Returns true if two geometries are topologically equal, meaning each geometry covers the other.
Two geometries are equal if they represent the same spatial region, even if their vertex orderings differ.

By default operates in spherical coordinates (longitude, latitude in degrees), consistent
with BigQuery's `ST_EQUALS`. Set `st_function_use_spherical = false` to use Cartesian coordinates.

Supports Point, Ring, Polygon, and MultiPolygon types in any combination.
    )",
        .syntax = "ST_Equals(geometry1, geometry2)",
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
            "[`MultiPolygon`](/sql-reference/data-types/geo#multipolygon)."}},
        .returned_value
        = {"Returns 1 if the two geometries are equal, 0 otherwise. [`UInt8`](/sql-reference/data-types/int-uint)."},
        .examples
        = {{"Same polygon",
            R"(
                SELECT ST_Equals(
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'),
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─ST_Equals()─┐
                │           1 │
                └─────────────┘
            )"}},
        .introduced_in = {25, 8},
        .category = FunctionDocumentation::Category::Geo},
    FunctionFactory::Case::Insensitive);

    factory.registerFunction<FunctionGeoEquals<SphericalPoint>>(FunctionDocumentation{
        .description = R"(
Returns true if two geometries are topologically equal on the sphere, meaning each
geometry covers the other. Two geometries are equal if they represent the same spatial
region, even if their vertex orderings differ.

Equivalent to `geoCoversSpherical(geometry1, geometry2) AND geoCoversSpherical(geometry2, geometry1)`.

Similar to BigQuery's `ST_EQUALS`. Supports Point, Ring, Polygon, and MultiPolygon
types in any combination. Operates in spherical coordinates (longitude, latitude in degrees).
    )",
        .syntax = "geoEqualsSpherical(geometry1, geometry2)",
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
            "[`MultiPolygon`](/sql-reference/data-types/geo#multipolygon)."}},
        .returned_value
        = {"Returns 1 if the two geometries are equal, 0 otherwise. [`UInt8`](/sql-reference/data-types/int-uint)."},
        .examples
        = {{"Same polygon (spherical)",
            R"(
                SELECT geoEqualsSpherical(
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'),
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─geoEqualsSpherical()─┐
                │                    1 │
                └──────────────────────┘
            )"},
           {"Different polygons (spherical)",
            R"(
                SELECT geoEqualsSpherical(
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'),
                    CAST([[(0.0, 0.0), (5.0, 0.0), (5.0, 5.0), (0.0, 5.0), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─geoEqualsSpherical()─┐
                │                    0 │
                └──────────────────────┘
            )"}},
        .introduced_in = {25, 9},
        .category = FunctionDocumentation::Category::Geo});
}

}
