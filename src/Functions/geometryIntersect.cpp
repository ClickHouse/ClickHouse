#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
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
}

namespace
{

/// `callOnGeometryDataType` distinguishes `LineString` from `Ring`, and `MultiLineString` from
/// `Polygon`, only by the custom type name: each pair shares the same structural type
/// (`Array(Tuple(Float64, Float64))` and `Array(Array(Tuple(Float64, Float64)))` respectively).
/// When the custom name is lost during expression analysis (see `getGeometryColumnTypeFromDataType`),
/// the dispatch falls back to the areal interpretation (`Ring` / `Polygon`), so a linear geometry
/// would be silently reinterpreted as areal and the result would change. Since these functions
/// accept any geometry type, reject such ambiguous unnamed forms with a clear error instead of
/// guessing; the caller can cast to an explicit geometry type to disambiguate.
void checkGeometryIntersectArgument(const DataTypePtr & type, const String & function_name)
{
    const auto & factory = DataTypeFactory::instance();

    /// Named geometry types are unambiguous: the dispatch uses the name.
    if (const auto * custom_name = type->getCustomName())
    {
        const auto & name = custom_name->getName();
        if (name == "Point" || name == "Ring" || name == "LineString"
            || name == "MultiLineString" || name == "Polygon" || name == "MultiPolygon")
            return;
    }

    /// `Point` and `MultiPolygon` are unambiguous even without a name: no other geometry type
    /// shares their structure (`Tuple(Float64, Float64)` and `Array(Array(Array(Tuple(Float64, Float64))))`).
    if (factory.get("Point")->equals(*type) || factory.get("MultiPolygon")->equals(*type))
        return;

    if (factory.get("Ring")->equals(*type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument of function {} has type {} which is ambiguous between Ring and LineString. "
            "Cast it to an explicit geometry type, for example `x::Ring` or `x::LineString`.",
            function_name, type->getName());

    if (factory.get("Polygon")->equals(*type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument of function {} has type {} which is ambiguous between Polygon and MultiLineString. "
            "Cast it to an explicit geometry type, for example `x::Polygon` or `x::MultiLineString`.",
            function_name, type->getName());

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "Argument of function {} has type {} which is not a geometry type.",
        function_name, type->getName());
}

/// Similar to polygonsIntersect{Cartesian,Spherical}, but works for any geometry data type
/// (Point, LineString, MultiLineString, Ring, Polygon, MultiPolygon), not only polygons.
/// The Geometry data type (a Variant of the geometry types above) is supported automatically
/// via useDefaultImplementationForVariant(): the Variant adaptor decomposes it into its concrete
/// alternatives and calls this function on each of them.
template <typename Point>
class FunctionGeometryIntersect final : public IFunction
{
public:
    static inline const char * name;

    explicit FunctionGeometryIntersect() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGeometryIntersect>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        checkGeometryIntersectArgument(arguments[0], getName());
        checkGeometryIntersectArgument(arguments[1], getName());
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

        callOnTwoGeometryDataTypes<Point>(
            arguments[0].type,
            arguments[1].type,
            [&](const auto & left_type, const auto & right_type)
            {
                using LeftConverterType = std::decay_t<decltype(left_type)>;
                using RightConverterType = std::decay_t<decltype(right_type)>;

                using LeftConverter = typename LeftConverterType::Type;
                using RightConverter = typename RightConverterType::Type;

                auto first = LeftConverter::convert(arguments[0].column->convertToFullColumnIfConst());
                auto second = RightConverter::convert(arguments[1].column->convertToFullColumnIfConst());

                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    boost::geometry::correct(first[i]);
                    boost::geometry::correct(second[i]);

                    res_data.emplace_back(boost::geometry::intersects(first[i], second[i]));
                }
            });

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
};


template <>
const char * FunctionGeometryIntersect<CartesianPoint>::name = "geometryIntersectCartesian";

template <>
const char * FunctionGeometryIntersect<SphericalPoint>::name = "geometryIntersectSpherical";

}

REGISTER_FUNCTION(geometryIntersect)
{
    factory.registerFunction<FunctionGeometryIntersect<CartesianPoint>>(FunctionDocumentation{
        .description = R"(
        Returns true if two geometries intersect (share any common point, line or area).
        Unlike [`polygonsIntersectCartesian`](#polygonsIntersectCartesian), it accepts any geometry data type
        ([`Point`](/sql-reference/data-types/geo#point), [`LineString`](/sql-reference/data-types/geo#linestring),
        [`MultiLineString`](/sql-reference/data-types/geo#multilinestring), [`Ring`](/sql-reference/data-types/geo#ring),
        [`Polygon`](/sql-reference/data-types/geo#polygon), [`MultiPolygon`](/sql-reference/data-types/geo#multipolygon)),
        including the common [`Geometry`](/sql-reference/data-types/geo#geometry) type, and the two arguments may be of different types.
        Coordinates are interpreted in the Cartesian plane.
    )",
        .syntax = "geometryIntersectCartesian(geometry1, geometry2)",
        .arguments
        = {{"geometry1", "A value of any geometry data type or [`Geometry`](/sql-reference/data-types/geo#geometry)."},
           {"geometry2", "A value of any geometry data type or [`Geometry`](/sql-reference/data-types/geo#geometry)."}},
        .returned_value = {"Returns true (1) if the two geometries intersect. [`Bool`](/sql-reference/data-types/boolean)."},
        .examples
        = {{"Usage example",
            R"(
                SELECT geometryIntersectCartesian([(2., 2.), (2., 3.), (3., 3.), (3., 2.)]::Ring, [(1., 1.), (1., 4.), (4., 4.), (4., 1.)]::Ring)
        )",
            R"(
                ┌─geometryIntersectCartesian(⋯)─┐
                │                             1 │
                └───────────────────────────────┘
        )"}},
        .introduced_in = {26, 7},
        .category = FunctionDocumentation::Category::Geo});

    factory.registerFunction<FunctionGeometryIntersect<SphericalPoint>>(FunctionDocumentation{
        .description = R"(
        Returns true if two geometries intersect (share any common point, line or area).
        Unlike [`polygonsIntersectSpherical`](#polygonsIntersectSpherical), it accepts any geometry data type
        ([`Point`](/sql-reference/data-types/geo#point), [`LineString`](/sql-reference/data-types/geo#linestring),
        [`MultiLineString`](/sql-reference/data-types/geo#multilinestring), [`Ring`](/sql-reference/data-types/geo#ring),
        [`Polygon`](/sql-reference/data-types/geo#polygon), [`MultiPolygon`](/sql-reference/data-types/geo#multipolygon)),
        including the common [`Geometry`](/sql-reference/data-types/geo#geometry) type, and the two arguments may be of different types.
        Coordinates are interpreted as being on an ideal sphere.
    )",
        .syntax = "geometryIntersectSpherical(geometry1, geometry2)",
        .arguments
        = {{"geometry1", "A value of any geometry data type or [`Geometry`](/sql-reference/data-types/geo#geometry)."},
           {"geometry2", "A value of any geometry data type or [`Geometry`](/sql-reference/data-types/geo#geometry)."}},
        .returned_value = {"Returns true (1) if the two geometries intersect. [`Bool`](/sql-reference/data-types/boolean)."},
        .examples
        = {{"Usage example",
            R"(
                SELECT geometryIntersectSpherical([[[(4.3613577, 50.8651821), (4.349556, 50.8535879), (4.3602419, 50.8435626), (4.3830299, 50.8428851), (4.3904543, 50.8564867), (4.3613148, 50.8651279)]]], (4.36, 50.85))
        )",
            R"(
                ┌─geometryIntersectSpherical(⋯)─┐
                │                             1 │
                └───────────────────────────────┘
        )"}},
        .introduced_in = {26, 7},
        .category = FunctionDocumentation::Category::Geo});
}

}
