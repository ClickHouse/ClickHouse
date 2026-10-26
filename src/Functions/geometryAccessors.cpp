#include <Columns/ColumnArray.h>
#include <Columns/ColumnVariant.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/geometry.h>

#include <limits>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// The position of the `n`-th of `size` elements: `n` counts from 1, and a negative `n` counts from the end, as in `arrayElement`.
std::optional<size_t> elementPosition(Int64 n, size_t size)
{
    if (n > 0 && static_cast<UInt64>(n) <= size)
        return n - 1;
    /// Unlike `-n`, `-(n + 1)` does not overflow for the minimum `Int64`.
    if (n < 0 && static_cast<UInt64>(-(n + 1)) < size)
        return size - 1 - static_cast<UInt64>(-(n + 1));
    return {};
}

/// A polygon is an array of rings and a line is an array of points, so an accessor takes an element of the array.
/// For a `Geometry` argument the result is `Geometry`, which is NULL for a row of another type or without such an element.
/// For other arguments, a row without such an element gives the default value of the result type, as in `arrayElement`.
template <typename Impl>
class FunctionGeometryAccessor final : public IFunction
{
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGeometryAccessor>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return Impl::has_index ? 2 : 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    /// Handle `Geometry` here, so that the result is `Geometry` too.
    bool useDefaultImplementationForVariantWithCustomName(const DataTypePtr & type) const override
    {
        return type->getName() != "Geometry";
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (Impl::has_index && !isNativeInteger(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The second argument of function {} must be an integer, got {}", getName(), arguments[1]->getName());

        if (arguments[0]->getName() == "Geometry")
            return arguments[0];

        const auto type = getGeometryColumnTypeFromDataType(arguments[0]);
        if (!type || !Impl::isContainer(*type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The first argument of function {} must be {} or Geometry, got {}", getName(), Impl::containers, arguments[0]->getName());

        return DataTypeFactory::instance().get(Impl::element);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ColumnPtr column = arguments[0].column->convertToFullColumnIfConst();

        /// The position of the element to take from the array of `size` elements in the row.
        const bool index_is_uint64 = Impl::has_index && isUInt64(arguments[1].type);
        auto position = [&](size_t row, size_t size)
        {
            Int64 n = 0;
            if constexpr (Impl::has_index)
                n = index_is_uint64
                    ? static_cast<Int64>(std::min<UInt64>(arguments[1].column->getUInt(row), std::numeric_limits<Int64>::max()))
                    : arguments[1].column->getInt(row);
            return Impl::position(n, size);
        };

        if (const auto * geometries = typeid_cast<const ColumnVariant *>(column.get()))
        {
            auto result = result_type->createColumn();
            auto & result_geometries = assert_cast<ColumnVariant &>(*result);
            result_geometries.reserve(input_rows_count);

            for (size_t row = 0; row < input_rows_count; ++row)
            {
                /// The global discriminators of `Geometry` are the values of `GeometryColumnType`.
                const auto discriminator = geometries->globalDiscriminatorAt(row);
                if (Impl::isContainer(static_cast<GeometryColumnType>(discriminator)))
                {
                    const auto & arrays = assert_cast<const ColumnArray &>(geometries->getVariantByGlobalDiscriminator(discriminator));
                    const auto & offsets = arrays.getOffsets();
                    const size_t offset = geometries->offsetAt(row);
                    if (auto pos = position(row, offsets[offset] - offsets[offset - 1]))
                    {
                        result_geometries.insertIntoVariantFrom(
                            static_cast<ColumnVariant::Discriminator>(Impl::element_type), arrays.getData(), offsets[offset - 1] + *pos);
                        continue;
                    }
                }
                result_geometries.insertDefault();
            }

            return result;
        }

        const auto & arrays = assert_cast<const ColumnArray &>(*column);
        const auto & offsets = arrays.getOffsets();
        auto result = arrays.getData().cloneEmpty();
        result->reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            if (auto pos = position(row, offsets[row] - offsets[row - 1]))
                result->insertFrom(arrays.getData(), offsets[row - 1] + *pos);
            else
                result->insertDefault();
        }

        return result;
    }
};

struct ExteriorRing
{
    static constexpr auto name = "exteriorRing";
    static constexpr bool has_index = false;
    static constexpr auto containers = "Polygon";
    static constexpr auto element = "Ring";
    static constexpr auto element_type = GeometryColumnType::Ring;

    static bool isContainer(GeometryColumnType type) { return type == GeometryColumnType::Polygon; }

    /// The first ring of a polygon is its exterior ring.
    static std::optional<size_t> position(Int64, size_t size) { return elementPosition(1, size); }
};

struct InteriorRingN
{
    static constexpr auto name = "interiorRingN";
    static constexpr bool has_index = true;
    static constexpr auto containers = "Polygon";
    static constexpr auto element = "Ring";
    static constexpr auto element_type = GeometryColumnType::Ring;

    static bool isContainer(GeometryColumnType type) { return type == GeometryColumnType::Polygon; }

    /// The interior rings (holes) follow the exterior ring.
    static std::optional<size_t> position(Int64 n, size_t size)
    {
        if (size == 0)
            return {};
        auto pos = elementPosition(n, size - 1);
        if (!pos)
            return {};
        return *pos + 1;
    }
};

struct PointN
{
    static constexpr auto name = "pointN";
    static constexpr bool has_index = true;
    static constexpr auto containers = "LineString, Ring";
    static constexpr auto element = "Point";
    static constexpr auto element_type = GeometryColumnType::Point;

    static bool isContainer(GeometryColumnType type) { return type == GeometryColumnType::Linestring || type == GeometryColumnType::Ring; }

    static std::optional<size_t> position(Int64 n, size_t size) { return elementPosition(n, size); }
};

}

REGISTER_FUNCTION(GeometryAccessors)
{
    const String geometry_result = R"(
For a `Geometry` argument the result is `Geometry`, which is NULL if the row has another type or there is no such element, as in PostGIS.
For other arguments, there is no NULL, so the result is the default value of its type, as in `arrayElement`.
)";
    FunctionDocumentation::IntroducedIn introduced_in = {26, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;

    {
        FunctionDocumentation::Description description = R"(
Returns the exterior ring of a polygon, which is the first ring of a `Polygon`, like `ST_ExteriorRing`.
)" + geometry_result + "So the result of a `Polygon` without rings is an empty `Ring`.\n";
        FunctionDocumentation::Syntax syntax = "exteriorRing(polygon)";
        FunctionDocumentation::Arguments arguments = {{"polygon", "A polygon.", {"Polygon", "Geometry"}}};
        FunctionDocumentation::ReturnedValue returned_value = {"The exterior ring.", {"Ring", "Geometry"}};
        FunctionDocumentation::Examples examples = {
            {
                "Exterior ring of a polygon with a hole",
                "SELECT exteriorRing([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(2., 2.), (4., 2.), (4., 4.), (2., 2.)]]::Polygon)",
                "[(0,0),(0,10),(10,10),(10,0),(0,0)]"
            },
            {
                "A Geometry that is not a polygon",
                "SELECT exteriorRing(readWKT('POINT (1 2)'))",
                "\\N"
            }
        };
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionGeometryAccessor<ExteriorRing>>(documentation);
    }
    {
        FunctionDocumentation::Description description = R"(
Returns the `n`-th interior ring (hole) of a polygon, like `ST_InteriorRingN`.
`n` counts from 1, and a negative `n` counts from the last hole.
)" + geometry_result + "So the result for a missing hole is an empty `Ring`.\n";
        FunctionDocumentation::Syntax syntax = "interiorRingN(polygon, n)";
        FunctionDocumentation::Arguments arguments = {
            {"polygon", "A polygon.", {"Polygon", "Geometry"}},
            {"n", "The number of the hole.", {"(U)Int*"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"The interior ring.", {"Ring", "Geometry"}};
        FunctionDocumentation::Examples examples = {
            {
                "Hole of a polygon",
                "SELECT interiorRingN([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(2., 2.), (4., 2.), (4., 4.), (2., 2.)]]::Polygon, 1)",
                "[(2,2),(4,2),(4,4),(2,2)]"
            }
        };
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionGeometryAccessor<InteriorRingN>>(documentation);
    }
    {
        FunctionDocumentation::Description description = R"(
Returns the `n`-th point of a line or a ring, like `ST_PointN`.
`n` counts from 1, and a negative `n` counts from the last point.
)" + geometry_result + "So the result for a missing point is `(0, 0)`.\n";
        FunctionDocumentation::Syntax syntax = "pointN(line, n)";
        FunctionDocumentation::Arguments arguments = {
            {"line", "A line or a ring.", {"LineString", "Ring", "Geometry"}},
            {"n", "The number of the point.", {"(U)Int*"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"The point.", {"Point", "Geometry"}};
        FunctionDocumentation::Examples examples = {
            {
                "First and last points of a line",
                "WITH [(1., 2.), (3., 4.), (5., 6.)]::LineString AS line SELECT (pointN(line, 1), pointN(line, -1))",
                "((1,2),(5,6))"
            }
        };
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionGeometryAccessor<PointN>>(documentation);
    }

    factory.registerAlias("ST_ExteriorRing", FunctionGeometryAccessor<ExteriorRing>::name);
    factory.registerAlias("ST_InteriorRingN", FunctionGeometryAccessor<InteriorRingN>::name);
    factory.registerAlias("ST_PointN", FunctionGeometryAccessor<PointN>::name);
}

}
