#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/box.hpp>
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
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// IntersectsBox function: returns true if the geometry intersects the bounding box
/// defined by (xmin, ymin, xmax, ymax).
///
/// Modeled after BigQuery's ST_INTERSECTSBOX.
/// Templated on Point to support both Spherical and Cartesian coordinate systems.
template <typename Point>
class FunctionGeoIntersectsBox : public IFunction
{
    using Box = boost::geometry::model::box<Point>;

public:
    static inline const char * name;

    explicit FunctionGeoIntersectsBox() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGeoIntersectsBox>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 5; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (size_t i = 1; i < 5; ++i)
        {
            if (!isNativeNumber(*arguments[i]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument {} of function {} must be a numeric type, got {}",
                    i + 1,
                    getName(),
                    arguments[i]->getName());
        }
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

        /// Extract the four bounding box coordinate columns.
        auto col_xmin = arguments[1].column->convertToFullColumnIfConst();
        auto col_ymin = arguments[2].column->convertToFullColumnIfConst();
        auto col_xmax = arguments[3].column->convertToFullColumnIfConst();
        auto col_ymax = arguments[4].column->convertToFullColumnIfConst();

        callOnGeometryDataType<Point>(
            arguments[0].type,
            [&](const auto & converter_type)
            {
                using ConverterType = std::decay_t<decltype(converter_type)>;
                using Converter = typename ConverterType::Type;

                if constexpr (
                    std::is_same_v<ColumnToLineStringsConverter<Point>, Converter>
                    || std::is_same_v<ColumnToMultiLineStringsConverter<Point>, Converter>)
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "First argument of function {} must not be LineString or MultiLineString",
                        getName());
                else
                {
                    constexpr bool is_point = std::is_same_v<ColumnToPointsConverter<Point>, Converter>;

                    auto geometries = Converter::convert(arguments[0].column->convertToFullColumnIfConst());

                    for (size_t i = 0; i < input_rows_count; ++i)
                    {
                        Float64 xmin = col_xmin->getFloat64(i);
                        Float64 ymin = col_ymin->getFloat64(i);
                        Float64 xmax = col_xmax->getFloat64(i);
                        Float64 ymax = col_ymax->getFloat64(i);

                        if (xmin > xmax || ymin > ymax)
                            throw Exception(
                                ErrorCodes::BAD_ARGUMENTS,
                                "Invalid bounding box in function {}: "
                                "xmin ({}) must be <= xmax ({}) and ymin ({}) must be <= ymax ({})",
                                getName(), xmin, xmax, ymin, ymax);

                        /// Construct bounding box: min_corner(xmin, ymin), max_corner(xmax, ymax)
                        Point min_corner;
                        boost::geometry::set<0>(min_corner, xmin);
                        boost::geometry::set<1>(min_corner, ymin);

                        Point max_corner;
                        boost::geometry::set<0>(max_corner, xmax);
                        boost::geometry::set<1>(max_corner, ymax);

                        Box box(min_corner, max_corner);

                        if constexpr (!is_point)
                            boost::geometry::correct(geometries[i]);

                        res_data.emplace_back(boost::geometry::intersects(geometries[i], box));
                    }
                }
            });

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
};

template <>
const char * FunctionGeoIntersectsBox<CartesianPoint>::name = "geoIntersectsBoxCartesian";

template <>
const char * FunctionGeoIntersectsBox<SphericalPoint>::name = "geoIntersectsBoxSpherical";

}

REGISTER_FUNCTION(GeoIntersectsBox)
{
    factory.registerFunction<FunctionGeoIntersectsBox<CartesianPoint>>(FunctionDocumentation{
        .description = R"(
Returns true if the geometry intersects the rectangle defined by the given bounding
box coordinates using Cartesian (flat/planar) coordinates.

Supports Point, Ring, Polygon, and MultiPolygon types.
    )",
        .syntax = "geoIntersectsBoxCartesian(geometry, xmin, ymin, xmax, ymax)",
        .arguments
        = {{"geometry",
            "A value of type [`Point`](/sql-reference/data-types/geo#point), "
            "[`Ring`](/sql-reference/data-types/geo#ring), "
            "[`Polygon`](/sql-reference/data-types/geo#polygon), or "
            "[`MultiPolygon`](/sql-reference/data-types/geo#multipolygon)."},
           {"xmin", "Minimum x coordinate (Float64)."},
           {"ymin", "Minimum y coordinate (Float64)."},
           {"xmax", "Maximum x coordinate (Float64)."},
           {"ymax", "Maximum y coordinate (Float64)."}},
        .returned_value
        = {"Returns 1 if the geometry intersects the box, 0 otherwise. [`UInt8`](/sql-reference/data-types/int-uint)."},
        .examples
        = {{"Point inside box",
            R"(
                SELECT geoIntersectsBoxCartesian(
                    CAST((5.0, 5.0), 'Point'),
                    0.0, 0.0, 10.0, 10.0)
            )",
            R"(
                ┌─geoIntersectsBoxCartesian()─┐
                │                           1 │
                └─────────────────────────────┘
            )"},
           {"Point outside box",
            R"(
                SELECT geoIntersectsBoxCartesian(
                    CAST((50.0, 50.0), 'Point'),
                    0.0, 0.0, 10.0, 10.0)
            )",
            R"(
                ┌─geoIntersectsBoxCartesian()─┐
                │                           0 │
                └─────────────────────────────┘
            )"}},
        .introduced_in = {25, 9},
        .category = FunctionDocumentation::Category::Geo});

    /// ST_IntersectsBox dispatches to geoIntersectsBoxSpherical or geoIntersectsBoxCartesian
    /// based on the `st_function_use_spherical` setting.
    factory.registerFunction("ST_IntersectsBox", [](ContextPtr context) -> FunctionPtr
    {
        if (context->getSettingsRef()[Setting::st_function_use_spherical])
            return FunctionGeoIntersectsBox<SphericalPoint>::create(context);
        else
            return FunctionGeoIntersectsBox<CartesianPoint>::create(context);
    }, FunctionDocumentation{
        .description = R"(
Returns true if the geometry intersects the rectangle defined by the given bounding
box coordinates.

By default operates in spherical coordinates (longitude, latitude in degrees), consistent
with BigQuery's `ST_INTERSECTSBOX`. Set `st_function_use_spherical = false` to use
Cartesian coordinates.

Supports Point, Ring, Polygon, and MultiPolygon types.
    )",
        .syntax = "ST_IntersectsBox(geometry, xmin, ymin, xmax, ymax)",
        .arguments
        = {{"geometry",
            "A value of type [`Point`](/sql-reference/data-types/geo#point), "
            "[`Ring`](/sql-reference/data-types/geo#ring), "
            "[`Polygon`](/sql-reference/data-types/geo#polygon), or "
            "[`MultiPolygon`](/sql-reference/data-types/geo#multipolygon)."},
           {"xmin", "Westmost longitude / minimum x (Float64)."},
           {"ymin", "Southmost latitude / minimum y (Float64)."},
           {"xmax", "Eastmost longitude / maximum x (Float64)."},
           {"ymax", "Northmost latitude / maximum y (Float64)."}},
        .returned_value
        = {"Returns 1 if the geometry intersects the box, 0 otherwise. [`UInt8`](/sql-reference/data-types/int-uint)."},
        .examples
        = {{"Point inside box",
            R"(
                SELECT ST_IntersectsBox(
                    CAST((5.0, 5.0), 'Point'),
                    0.0, 0.0, 10.0, 10.0)
            )",
            R"(
                ┌─ST_IntersectsBox()─┐
                │                  1 │
                └────────────────────┘
            )"},
           {"Point outside box",
            R"(
                SELECT ST_IntersectsBox(
                    CAST((50.0, 50.0), 'Point'),
                    0.0, 0.0, 10.0, 10.0)
            )",
            R"(
                ┌─ST_IntersectsBox()─┐
                │                  0 │
                └────────────────────┘
            )"}},
        .introduced_in = {25, 8},
        .category = FunctionDocumentation::Category::Geo},
    FunctionFactory::Case::Insensitive);

    factory.registerFunction<FunctionGeoIntersectsBox<SphericalPoint>>(FunctionDocumentation{
        .description = R"(
Returns true if the geometry intersects the rectangle defined by the given bounding
box coordinates (longitude/latitude min/max). Operates in spherical coordinates.

Similar to BigQuery's `ST_INTERSECTSBOX`.

Supports Point, Ring, Polygon, and MultiPolygon types.
    )",
        .syntax = "geoIntersectsBoxSpherical(geometry, xmin, ymin, xmax, ymax)",
        .arguments
        = {{"geometry",
            "A value of type [`Point`](/sql-reference/data-types/geo#point), "
            "[`Ring`](/sql-reference/data-types/geo#ring), "
            "[`Polygon`](/sql-reference/data-types/geo#polygon), or "
            "[`MultiPolygon`](/sql-reference/data-types/geo#multipolygon)."},
           {"xmin", "Westmost longitude (Float64)."},
           {"ymin", "Southmost latitude (Float64)."},
           {"xmax", "Eastmost longitude (Float64)."},
           {"ymax", "Northmost latitude (Float64)."}},
        .returned_value
        = {"Returns 1 if the geometry intersects the box, 0 otherwise. [`UInt8`](/sql-reference/data-types/int-uint)."},
        .examples
        = {{"Point inside box (spherical)",
            R"(
                SELECT geoIntersectsBoxSpherical(
                    CAST((5.0, 5.0), 'Point'),
                    0.0, 0.0, 10.0, 10.0)
            )",
            R"(
                ┌─geoIntersectsBoxSpherical()─┐
                │                           1 │
                └─────────────────────────────┘
            )"},
           {"Point outside box (spherical)",
            R"(
                SELECT geoIntersectsBoxSpherical(
                    CAST((50.0, 50.0), 'Point'),
                    0.0, 0.0, 10.0, 10.0)
            )",
            R"(
                ┌─geoIntersectsBoxSpherical()─┐
                │                           0 │
                └─────────────────────────────┘
            )"}},
        .introduced_in = {25, 9},
        .category = FunctionDocumentation::Category::Geo});
}

}
