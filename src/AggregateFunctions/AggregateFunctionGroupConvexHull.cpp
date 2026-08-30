#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGeoUtils.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Common/AllocatorWithMemoryTracking.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypeFactory.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

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
/// `maybeCompress` triggers after the threshold is exceeded, so a full ingestion
/// batch includes one extra point and is compressed before the next batch.
constexpr size_t CONVEX_HULL_INGESTION_BATCH_SIZE = CONVEX_HULL_COMPRESSION_THRESHOLD + 1;
constexpr UInt8 GROUP_CONVEX_HULL_SERDE_VERSION = 2;

using CartesianMultiPoint = boost::geometry::model::multi_point<CartesianPoint, std::vector, AllocatorWithMemoryTracking>;

struct GroupConvexHullData
{
    CartesianMultiPoint points;

    /// Stored point count right after the most recent `compress`. The compression trigger looks
    /// at growth since then, not the total size, so a large valid hull (many points in convex
    /// position, e.g. on a circle) whose vertex count alone exceeds
    /// `CONVEX_HULL_COMPRESSION_THRESHOLD` does not force a full hull recomputation on every
    /// subsequent row.
    size_t size_after_compression = 0;

    void finishAdd(const char * function_name)
    {
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
        /// Preserve the compression watermark when a serialized partial state is the first state
        /// merged into a fresh accumulator. Treating its already-compressed hull points as newly
        /// appended growth would discard the amortization restored by `deserialize`.
        if (points.empty())
        {
            points = other.points;
            size_after_compression = other.size_after_compression;
            return;
        }

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
        {
            size_after_compression = points.size();
            return;
        }

        CartesianPolygon hull;
        boost::geometry::convex_hull(points, hull);

        CartesianMultiPoint compressed_points(hull.outer().begin(), hull.outer().end());

        /// boost::geometry::convex_hull returns a closed ring (the first point is duplicated
        /// at the end). The stored points are only an accumulator for recomputing the hull,
        /// so the closing duplicate is redundant. Dropping it keeps the stored point count
        /// from exceeding the state budget by one, which would otherwise let a self-produced
        /// state at the limit serialize but fail to deserialize (INCORRECT_DATA).
        if (compressed_points.size() >= 2 && compressed_points.front().get<0>() == compressed_points.back().get<0>()
            && compressed_points.front().get<1>() == compressed_points.back().get<1>())
            compressed_points.pop_back();

        points.swap(compressed_points);

        size_after_compression = points.size();
    }

    void maybeCompress()
    {
        /// Trigger on points accumulated since the last compression, not the total size: a hull
        /// already larger than the threshold must not recompress on every single appended point.
        if (points.size() - size_after_compression > CONVEX_HULL_COMPRESSION_THRESHOLD)
            compress();
    }

    CartesianRing getResult() const
    {
        if (points.empty())
            return {};

        CartesianPolygon hull;
        boost::geometry::convex_hull(points, hull);

        CartesianRing result;
        result.assign(hull.outer().begin(), hull.outer().end());
        return result;
    }
};


class AggregateFunctionGroupConvexHull final : public IAggregateFunctionDataHelper<GroupConvexHullData, AggregateFunctionGroupConvexHull>
{
private:
    static constexpr auto name = "groupConvexHull";
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

    String getName() const override { return name; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto value = getGeometryColumnValue(columns[0], row_num, geo_type, is_variant, variant_type_map);
        if (value.type == GeometryColumnType::Null)
            return;

        auto & data = AggregateFunctionGroupConvexHull::data(place);
        ConvexHullPointsCursor cursor;
        while (!cursor.finished)
        {
            const size_t appended = appendConvexHullPointsBatchFromColumn(
                *value.column, value.row_num, value.type, data.points, cursor, CONVEX_HULL_INGESTION_BATCH_SIZE, name);
            if (appended != 0)
                data.finishAdd(name);
        }
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        AggregateFunctionGroupConvexHull::data(place).merge(AggregateFunctionGroupConvexHull::data(rhs), name);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinaryLittleEndian(GROUP_CONVEX_HULL_SERDE_VERSION, buf);

        const auto & data = AggregateFunctionGroupConvexHull::data(place);
        writeVarUInt(data.points.size(), buf);
        writeVarUInt(data.size_after_compression, buf);
        for (const auto & pt : data.points)
        {
            writeBinaryLittleEndian(pt.get<0>(), buf);
            writeBinaryLittleEndian(pt.get<1>(), buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        UInt8 version = 0;
        readBinaryLittleEndian(version, buf);
        if (version != GROUP_CONVEX_HULL_SERDE_VERSION)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Unsupported serialization version {} for aggregate function {} (expected {})",
                static_cast<int>(version),
                getName(),
                static_cast<int>(GROUP_CONVEX_HULL_SERDE_VERSION));

        UInt64 size = 0;
        readVarUInt(size, buf);
        if (size > MAX_POINTS_IN_CONVEX_HULL_STATE)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Corrupted state of aggregate function {}: {} points (limit {})",
                getName(),
                size,
                MAX_POINTS_IN_CONVEX_HULL_STATE);

        UInt64 size_after_compression = 0;
        readVarUInt(size_after_compression, buf);
        if (size_after_compression > size)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Corrupted state of aggregate function {}: compression watermark {} exceeds point count {}",
                getName(),
                size_after_compression,
                size);

        /// `finishAdd` and `merge` compress immediately when growth since the previous compression
        /// exceeds this threshold. Therefore the writer can never emit a state whose watermark
        /// lags farther behind. Enforce the same invariant on untrusted serialized states so a
        /// crafted watermark cannot defer compression for a point set the writer would already
        /// have reduced.
        if (size - size_after_compression > CONVEX_HULL_COMPRESSION_THRESHOLD)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Corrupted state of aggregate function {}: {} points since compression exceed the threshold {}",
                getName(),
                size - size_after_compression,
                CONVEX_HULL_COMPRESSION_THRESHOLD);

        /// The prefix before the watermark is not recomputed to prove that it is already a convex
        /// hull. Such a check would add an O(n log n) operation to every state read, while accepting
        /// extra finite points does not change the final result or bypass either resource bound:
        /// `getResult` and the next `compress` compute the hull from all stored points.

        /// Do not pre-size the accumulator to the declared count. A corrupted state can advertise
        /// an in-range count (up to `MAX_POINTS_IN_CONVEX_HULL_STATE`) yet carry a truncated
        /// coordinate payload, so `points.resize(size)` would force an allocation for up to
        /// `MAX_POINTS_IN_CONVEX_HULL_STATE` `CartesianPoint` values before `readBinaryLittleEndian`
        /// hits EOF. Append points incrementally with a bounded initial reservation so memory grows
        /// only with the bytes actually present. Do not call `finishAdd`: it can compress the partial
        /// payload and make the restored state depend on the deserialization batch size.
        constexpr UInt64 deserialize_initial_capacity = 4096;
        auto & data = AggregateFunctionGroupConvexHull::data(place);
        data.points.reserve(size < deserialize_initial_capacity ? static_cast<size_t>(size) : deserialize_initial_capacity);

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
            data.points.push_back(CartesianPoint(x, y));
        }

        data.size_after_compression = static_cast<size_t>(size_after_compression);
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
    /// An unnamed `Array(Tuple(Float64, Float64))` can be either `Ring` or `LineString`, but
    /// `groupConvexHull` uses all points for both, so losing the custom name is harmless here.
    if (!geo_type || *geo_type != GeometryColumnType::Ring || arg_type->getName() == "Ring")
        geo_type = getUnambiguousGeometryColumnTypeFromDataType(arg_type, name);
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
For `MultiPoint`, `Ring`, `LineString`, and `MultiLineString`, all points are used.
For `Polygon`, only the outer ring points are used.
For `MultiPolygon`, only the outer ring points of each polygon are used.
`Geometry` arguments are also supported according to the active value type.

For polygonal inputs, every coordinate, including those of inner rings (holes), must be finite, and a polygon with an empty outer ring but non-empty inner rings is rejected. Inner-ring points are checked even though only outer-ring points contribute to the hull; other polygon topology constraints are not validated by this function.

Empty geometries contribute no points. The result is a `Ring` representing the convex hull, or an empty `Ring` when the group is empty or all inputs are empty.

`NULL` values inside a `Geometry` (Variant) column are skipped. For a `Nullable` argument (e.g. `Nullable(Point)`), `NULL` rows are skipped as well; because a `Ring` cannot be wrapped in `Nullable`, the result type stays `Ring` and a group with only `NULL` values yields an empty `Ring`, consistent with the empty-group result. A literal `NULL` argument yields `NULL`.
    )";
    FunctionDocumentation::Syntax syntax = "groupConvexHull(geometry)";
    FunctionDocumentation::Arguments arguments
        = {{"geometry",
            "A geometry value (`Point`, `MultiPoint`, `Ring`, `LineString`, `MultiLineString`, `Polygon`, `MultiPolygon`) or a "
            "`Geometry` value containing one of these types.",
            {"Point", "MultiPoint", "Ring", "LineString", "MultiLineString", "Polygon", "MultiPolygon", "Geometry"}}};
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
    FunctionDocumentation::IntroducedIn introduced_in = {26, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    /// `returns_default_when_only_null` must stay false. It would not change the `Nullable(Point)` behavior:
    /// `Ring` cannot be inside `Nullable`, so the `Null` combinator keeps the non-nullable result type and an
    /// all-NULL group returns an empty `Ring` either way, consistent with the empty-group result. But with true,
    /// a literal NULL argument would be replaced by `AggregateFunctionNothingUInt64`, i.e. `groupConvexHull(NULL)`
    /// would return `0 :: UInt64` instead of `NULL`.
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    factory.registerFunction("groupConvexHull", {createAggregateFunctionGroupConvexHull, documentation, properties});
}

}
