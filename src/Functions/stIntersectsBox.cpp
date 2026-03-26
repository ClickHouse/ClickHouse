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

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

using SphericalBox = boost::geometry::model::box<SphericalPoint>;

/// ST_IntersectsBox(geometry, xmin, ymin, xmax, ymax)
///
/// Returns true if the geometry intersects the bounding box defined by
/// longitude/latitude min/max values. Modeled after BigQuery's ST_INTERSECTSBOX.
///
/// Parameters:
///   geometry — Point, Ring, Polygon, or MultiPolygon in spherical coordinates
///   xmin     — westmost longitude (Float64)
///   ymin     — southmost latitude (Float64)
///   xmax     — eastmost longitude (Float64)
///   ymax     — northmost latitude (Float64)
class FunctionSTIntersectsBox : public IFunction
{
public:
    static inline const char * name = "ST_IntersectsBox";

    explicit FunctionSTIntersectsBox() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSTIntersectsBox>(); }

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

        callOnGeometryDataType<SphericalPoint>(
            arguments[0].type,
            [&](const auto & converter_type)
            {
                using ConverterType = std::decay_t<decltype(converter_type)>;
                using Converter = typename ConverterType::Type;

                if constexpr (
                    std::is_same_v<ColumnToLineStringsConverter<SphericalPoint>, Converter>
                    || std::is_same_v<ColumnToMultiLineStringsConverter<SphericalPoint>, Converter>)
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "First argument of function {} must not be LineString or MultiLineString",
                        getName());
                else
                {
                    constexpr bool is_point = std::is_same_v<ColumnToPointsConverter<SphericalPoint>, Converter>;

                    auto geometries = Converter::convert(arguments[0].column->convertToFullColumnIfConst());

                    for (size_t i = 0; i < input_rows_count; ++i)
                    {
                        Float64 xmin = col_xmin->getFloat64(i);
                        Float64 ymin = col_ymin->getFloat64(i);
                        Float64 xmax = col_xmax->getFloat64(i);
                        Float64 ymax = col_ymax->getFloat64(i);

                        /// Construct bounding box: min_corner(lon_min, lat_min), max_corner(lon_max, lat_max)
                        SphericalPoint min_corner;
                        boost::geometry::set<0>(min_corner, xmin);
                        boost::geometry::set<1>(min_corner, ymin);

                        SphericalPoint max_corner;
                        boost::geometry::set<0>(max_corner, xmax);
                        boost::geometry::set<1>(max_corner, ymax);

                        SphericalBox box(min_corner, max_corner);

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

}

REGISTER_FUNCTION(STIntersectsBox)
{
    factory.registerFunction<FunctionSTIntersectsBox>(FunctionDocumentation{
        .description = R"(
Returns true if the geometry intersects the rectangle defined by the given bounding
box coordinates (longitude/latitude min/max). Operates in spherical coordinates.

Similar to BigQuery's `ST_INTERSECTSBOX`.

Note: function name is case-insensitive, so `st_intersectsbox`, `ST_INTERSECTSBOX`, etc. all work.
    )",
        .syntax = "ST_IntersectsBox(geometry, xmin, ymin, xmax, ymax)",
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
        .category = FunctionDocumentation::Category::Geo});
}

}
