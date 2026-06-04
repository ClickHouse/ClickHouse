#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGeoUtils.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <DataTypes/DataTypeFactory.h>

#include <boost/geometry.hpp>

#include <vector>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int INCORRECT_DATA;
}

namespace
{

constexpr size_t CONVEX_HULL_COMPRESSION_THRESHOLD = 10000;

struct GroupConvexHullData
{
    std::vector<CartesianPoint> points; // STYLE_CHECK_ALLOW_STD_CONTAINERS

    void addMany(const std::vector<CartesianPoint> & new_points, const char * function_name) // STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        points.insert(points.end(), new_points.begin(), new_points.end());

        /// Compress (recompute the hull) before enforcing the budget, mirroring `merge`: many
        /// input points can collapse to a small hull, so a valid result must not be rejected
        /// based on how the points were batched across rows. If the compressed hull genuinely
        /// exceeds the cap, reject it.
        if (points.size() > MAX_POINTS_IN_CONVEX_HULL_STATE)
        {
            compress();
            if (points.size() > MAX_POINTS_IN_CONVEX_HULL_STATE)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Aggregate function {} state has too many points after compression: {} (limit {})",
                    function_name,
                    points.size(),
                    MAX_POINTS_IN_CONVEX_HULL_STATE);
        }
        else
            maybeCompress();
    }

    void merge(const GroupConvexHullData & other, const char * function_name)
    {
        points.insert(points.end(), other.points.begin(), other.points.end());

        /// Compress (recompute the hull) before enforcing the budget. Two partial states can
        /// each be below the cap yet share the same large hull, so the combined point count
        /// can exceed the cap only because the hulls have not been merged yet. Computing the
        /// canonical hull first keeps a valid result from being rejected based on the
        /// merge-tree shape; if the merged hull genuinely exceeds the cap, reject it.
        if (points.size() > MAX_POINTS_IN_CONVEX_HULL_STATE)
        {
            compress();
            if (points.size() > MAX_POINTS_IN_CONVEX_HULL_STATE)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Aggregate function {} state has too many points after compression: {} (limit {})",
                    function_name,
                    points.size(),
                    MAX_POINTS_IN_CONVEX_HULL_STATE);
        }
        else
            maybeCompress();
    }

    void compress()
    {
        if (points.size() <= 1)
            return;

        boost::geometry::model::multi_point<CartesianPoint> mp(points.begin(), points.end());

        CartesianPolygon hull;
        boost::geometry::convex_hull(mp, hull);

        points.clear();
        points.assign(hull.outer().begin(), hull.outer().end());

        /// boost::geometry::convex_hull returns a closed ring (the first point is duplicated
        /// at the end). The stored points are only an accumulator for recomputing the hull,
        /// so the closing duplicate is redundant. Dropping it keeps the stored point count
        /// from exceeding the state budget by one, which would otherwise let a self-produced
        /// state at the limit serialize but fail to deserialize (INCORRECT_DATA).
        if (points.size() >= 2
            && points.front().get<0>() == points.back().get<0>()
            && points.front().get<1>() == points.back().get<1>())
            points.pop_back();
    }

    void maybeCompress()
    {
        if (points.size() > CONVEX_HULL_COMPRESSION_THRESHOLD)
            compress();
    }

    CartesianRing getResult() const
    {
        if (points.empty())
            return {};

        boost::geometry::model::multi_point<CartesianPoint> mp(points.begin(), points.end());

        CartesianPolygon hull;
        boost::geometry::convex_hull(mp, hull);

        CartesianRing result;
        result.assign(hull.outer().begin(), hull.outer().end());
        return result;
    }
};


void extractPointsFromField(
    const Field & field,
    GeometryColumnType geo_type,
    std::vector<CartesianPoint> & out) // STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    auto validate = [](const CartesianPoint & p)
    {
        Float64 x = p.get<0>();
        Float64 y = p.get<1>();
        if (std::isnan(x) || std::isnan(y))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Point coordinates must not be NaN");
        if (std::isinf(x) || std::isinf(y))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Point coordinates must not be infinite");
    };

    switch (geo_type)
    {
        case GeometryColumnType::Point: {
            auto pt = getPointFromField<CartesianPoint>(field);
            validate(pt);
            out.push_back(pt);
            break;
        }
        case GeometryColumnType::Ring: {
            auto ring = getRingFromField<CartesianPoint>(field);
            for (const auto & pt : ring)
            {
                validate(pt);
                out.push_back(pt);
            }
            break;
        }
        case GeometryColumnType::Linestring: {
            auto ls = getLineStringFromField<CartesianPoint>(field);
            for (const auto & pt : ls)
            {
                validate(pt);
                out.push_back(pt);
            }
            break;
        }
        case GeometryColumnType::MultiLinestring: {
            auto mls = getMultiLineStringFromField<CartesianPoint>(field);
            for (const auto & ls : mls)
                for (const auto & pt : ls)
                {
                    validate(pt);
                    out.push_back(pt);
                }
            break;
        }
        case GeometryColumnType::Polygon: {
            auto poly = getPolygonFromField<CartesianPoint>(field);
            for (const auto & pt : poly.outer())
            {
                validate(pt);
                out.push_back(pt);
            }
            break;
        }
        case GeometryColumnType::MultiPolygon: {
            auto mpoly = getMultiPolygonFromField<CartesianPoint>(field);
            for (const auto & poly : mpoly)
                for (const auto & pt : poly.outer())
                {
                    validate(pt);
                    out.push_back(pt);
                }
            break;
        }
        case GeometryColumnType::Null:
            break;
    }
}


class AggregateFunctionGroupConvexHull final : public IAggregateFunctionDataHelper<GroupConvexHullData, AggregateFunctionGroupConvexHull>
{
private:
    GeometryColumnType geo_type;
    bool is_variant = false;
    VariantTypeMap variant_type_map;

public:
    AggregateFunctionGroupConvexHull(
        const DataTypePtr & argument_type, const Array & parameters_, GeometryColumnType geo_type_, bool is_variant_)
        : IAggregateFunctionDataHelper<GroupConvexHullData, AggregateFunctionGroupConvexHull>(
              {argument_type}, parameters_, DataTypeFactory::instance().get("Ring"))
        , geo_type(geo_type_)
        , is_variant(is_variant_)
    {
        if (is_variant)
            variant_type_map = buildVariantTypeMap(argument_type);
    }

    String getName() const override { return "groupConvexHull"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        Field field;
        columns[0]->get(row_num, field);

        GeometryColumnType current_type = geo_type;
        if (is_variant)
            current_type = resolveGeometryVariantType(columns[0], row_num, variant_type_map);

        if (current_type == GeometryColumnType::Null)
            return;

        std::vector<CartesianPoint> new_points; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        extractPointsFromField(field, current_type, new_points);
        AggregateFunctionGroupConvexHull::data(place).addMany(new_points, getName().c_str());
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        AggregateFunctionGroupConvexHull::data(place).merge(
            AggregateFunctionGroupConvexHull::data(rhs), getName().c_str());
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinaryLittleEndian(GEO_SERDE_VERSION, buf);

        const auto & points = AggregateFunctionGroupConvexHull::data(place).points;
        writeVarUInt(points.size(), buf);
        for (const auto & pt : points)
        {
            writeBinaryLittleEndian(pt.get<0>(), buf);
            writeBinaryLittleEndian(pt.get<1>(), buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        UInt8 version = 0;
        readBinaryLittleEndian(version, buf);
        if (version != GEO_SERDE_VERSION)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Unsupported serialization version {} for aggregate function {} (expected {})",
                static_cast<int>(version),
                getName(),
                static_cast<int>(GEO_SERDE_VERSION));

        auto & points = AggregateFunctionGroupConvexHull::data(place).points;
        UInt64 size = 0;
        readVarUInt(size, buf);
        if (size > MAX_POINTS_IN_CONVEX_HULL_STATE)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Corrupted state of aggregate function {}: {} points (limit {})",
                getName(),
                size,
                MAX_POINTS_IN_CONVEX_HULL_STATE);

        points.resize(size);
        for (UInt64 i = 0; i < size; ++i)
        {
            Float64 x = 0;
            Float64 y = 0;
            readBinaryLittleEndian(x, buf);
            readBinaryLittleEndian(y, buf);
            if (!std::isfinite(x) || !std::isfinite(y))
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Corrupted state of aggregate function {}: non-finite coordinate ({}, {})",
                    getName(),
                    x,
                    y);
            points[i] = CartesianPoint(x, y);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto result = AggregateFunctionGroupConvexHull::data(place).getResult();

        auto & arr_to = assert_cast<ColumnArray &>(to);
        auto & arr_offsets = arr_to.getOffsets();
        auto & tuple_col = assert_cast<ColumnTuple &>(arr_to.getData());
        auto & x_col = assert_cast<ColumnFloat64 &>(tuple_col.getColumn(0));
        auto & y_col = assert_cast<ColumnFloat64 &>(tuple_col.getColumn(1));

        for (const auto & pt : result)
        {
            x_col.getData().push_back(pt.get<0>());
            y_col.getData().push_back(pt.get<1>());
        }

        arr_offsets.push_back(x_col.getData().size());
    }
};


AggregateFunctionPtr createAggregateFunctionGroupConvexHull(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertUnary(name, argument_types);
    assertNoParameters(name, parameters);

    const auto & arg_type = argument_types[0];

    if (arg_type->getName() == "Geometry")
        return std::make_shared<AggregateFunctionGroupConvexHull>(
            arg_type, parameters, GeometryColumnType::Point /* unused for variant */, true);

    auto geo_type = getGeometryColumnTypeFromDataType(arg_type);
    if (!geo_type)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument of function {} must be a Geo type (Point, Ring, LineString, Polygon, MultiPolygon, etc.), got {}",
            name,
            arg_type->getName());

    return std::make_shared<AggregateFunctionGroupConvexHull>(arg_type, parameters, *geo_type, false);
}

}

void registerAggregateFunctionGroupConvexHull(AggregateFunctionFactory & factory);
void registerAggregateFunctionGroupConvexHull(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Computes the convex hull of all geometries in the group.

Coordinates are interpreted as Cartesian (planar), not spherical, regardless of whether the input values represent longitude/latitude. There is no spherical variant of this function.

For `Point` arguments, each point is added directly.
For `Ring`, `LineString`, and `MultiLineString`, all points are used.
For `Polygon`, only the outer ring points are used.
For `MultiPolygon`, only the outer ring points of each polygon are used.
`Geometry` arguments are also supported according to the active value type.

The result is a `Ring` representing the convex hull.
    )";
    FunctionDocumentation::Syntax syntax = "groupConvexHull(geometry)";
    FunctionDocumentation::Arguments arguments
        = {{"geometry",
            "A geometry value (`Point`, `Ring`, `LineString`, `MultiLineString`, `Polygon`, `MultiPolygon`) or a `Geometry` value "
            "containing one of these types.",
            {"Point", "Ring", "LineString", "MultiLineString", "Polygon", "MultiPolygon", "Geometry"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the convex hull as a Ring.", {"Ring"}};
    FunctionDocumentation::Examples examples
        = {{"Convex hull of points",
            R"(
SELECT wkt(groupConvexHull(pt)) FROM (
    SELECT arrayJoin([
        readWKTPoint('POINT (0 0)'),
        readWKTPoint('POINT (1 0)'),
        readWKTPoint('POINT (1 1)'),
        readWKTPoint('POINT (0 1)')
    ]) AS pt
)
            )",
            R"(
POLYGON((0 0,0 1,1 1,1 0,0 0))
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = false};
    factory.registerFunction("groupConvexHull", {createAggregateFunctionGroupConvexHull, documentation, properties});
}

}
