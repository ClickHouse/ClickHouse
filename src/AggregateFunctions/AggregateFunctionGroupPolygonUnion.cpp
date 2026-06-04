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

constexpr size_t UNION_REDUCTION_THRESHOLD = 16;


/// Pairwise reduction to keep intermediate complexity lower than a linear left-fold.
void reduceChunksPairwiseUnion(std::vector<CartesianMultiPolygon> & chunks) // STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    while (chunks.size() > 1)
    {
        size_t n = chunks.size();
        size_t out = 0;
        for (size_t i = 0; i + 1 < n; i += 2)
        {
            CartesianMultiPolygon tmp;
            boost::geometry::union_(chunks[i], chunks[i + 1], tmp);
            chunks[out++] = std::move(tmp);
        }
        if (n % 2 == 1)
            chunks[out++] = std::move(chunks[n - 1]);
        chunks.resize(out);
    }
}


struct GroupPolygonUnionData
{
    std::vector<CartesianMultiPolygon> chunks; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    size_t total_points = 0;

    void add(CartesianMultiPolygon && mp, const char * function_name)
    {
        if (mp.empty())
            return;
        total_points += countMultiPolygonPoints(mp);
        chunks.push_back(std::move(mp));

        /// The accumulated point count may exceed the budget only because identical or
        /// overlapping polygons have not been unioned yet. Reduce to the canonical state
        /// first and enforce the cap on the reduced representation, mirroring `merge`, so a
        /// valid result is not rejected based on the row/chunk shape of the input.
        if (total_points > MAX_POINTS_IN_POLYGONAL_STATE)
        {
            reduceChunksPairwiseUnion(chunks);
            recountPoints(function_name);
        }
        else
            maybeReduce(function_name);
    }

    void merge(const GroupPolygonUnionData & other, const char * function_name)
    {
        chunks.insert(chunks.end(), other.chunks.begin(), other.chunks.end());
        total_points += other.total_points;

        /// The combined point count may exceed the budget only because identical or
        /// overlapping polygons coming from two partial states have not been unioned yet.
        /// Reduce first, then enforce the budget on the canonical (reduced) state, so a
        /// valid result is not rejected merely because of the merge-tree shape.
        if (total_points > MAX_POINTS_IN_POLYGONAL_STATE)
        {
            reduceChunksPairwiseUnion(chunks);
            recountPoints(function_name);
        }
        else
            maybeReduce(function_name);
    }

    void maybeReduce(const char * function_name)
    {
        if (chunks.size() > UNION_REDUCTION_THRESHOLD)
        {
            reduceChunksPairwiseUnion(chunks);
            recountPoints(function_name);
        }
    }

    /// Boost union may add intersection vertices, so the post-reduction sum can grow.
    /// Re-enforce the budget here so post-reduce state never exceeds what deserialize accepts.
    void recountPoints(const char * function_name)
    {
        size_t recomputed = 0;
        for (const auto & chunk : chunks)
            recomputed += countMultiPolygonPoints(chunk);
        if (recomputed > MAX_POINTS_IN_POLYGONAL_STATE)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Aggregate function {} state has too many points after reduction: {} (limit {})",
                function_name,
                recomputed,
                MAX_POINTS_IN_POLYGONAL_STATE);
        total_points = recomputed;
    }

    CartesianMultiPolygon getResult(const char * function_name)
    {
        if (chunks.empty())
            return {};
        reduceChunksPairwiseUnion(chunks);
        /// `insertResultInto` may be followed by further `add`/`merge`/`insertResultInto`
        /// calls (for example in `runningAccumulate` or window execution), so keep
        /// `total_points` consistent with the reduced `chunks` instead of leaving a stale count.
        recountPoints(function_name);
        return chunks.empty() ? CartesianMultiPolygon{} : chunks[0];
    }
};


class AggregateFunctionGroupPolygonUnion final
    : public IAggregateFunctionDataHelper<GroupPolygonUnionData, AggregateFunctionGroupPolygonUnion>
{
private:
    GeometryColumnType geo_type;
    bool is_variant = false;
    VariantTypeMap variant_type_map;

public:
    AggregateFunctionGroupPolygonUnion(
        const DataTypePtr & argument_type, const Array & parameters_, GeometryColumnType geo_type_, bool is_variant_)
        : IAggregateFunctionDataHelper<GroupPolygonUnionData, AggregateFunctionGroupPolygonUnion>(
              {argument_type}, parameters_, DataTypeFactory::instance().get("MultiPolygon"))
        , geo_type(geo_type_)
        , is_variant(is_variant_)
    {
        if (is_variant)
            variant_type_map = buildVariantTypeMap(argument_type);
    }

    String getName() const override { return "groupPolygonUnion"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        Field field;
        columns[0]->get(row_num, field);

        GeometryColumnType current_type = geo_type;
        if (is_variant)
        {
            current_type = resolveGeometryVariantType(columns[0], row_num, variant_type_map);
            current_type = normalizePolygonalVariantType(current_type);
        }

        if (current_type == GeometryColumnType::Null)
            return;

        auto mp = fieldToMultiPolygon(field, current_type, getName().c_str());
        AggregateFunctionGroupPolygonUnion::data(place).add(std::move(mp), getName().c_str());
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        AggregateFunctionGroupPolygonUnion::data(place).merge(
            AggregateFunctionGroupPolygonUnion::data(rhs), getName().c_str());
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinaryLittleEndian(GEO_SERDE_VERSION, buf);

        const auto & chunks = AggregateFunctionGroupPolygonUnion::data(place).chunks;
        writeVarUInt(chunks.size(), buf);
        for (const auto & chunk : chunks)
            serializeGeoMultiPolygon(chunk, buf);
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

        auto & data = AggregateFunctionGroupPolygonUnion::data(place);
        auto & chunks = data.chunks;
        UInt64 chunk_count = 0;
        readVarUInt(chunk_count, buf);
        if (chunk_count > MAX_CHUNKS_PER_STATE)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Corrupted state of aggregate function {}: {} chunks (limit {})",
                getName(),
                chunk_count,
                MAX_CHUNKS_PER_STATE);

        chunks.resize(chunk_count);
        PolygonalStateBudget budget;
        for (UInt64 i = 0; i < chunk_count; ++i)
        {
            chunks[i] = deserializeGeoMultiPolygon(buf, getName().c_str(), budget);
            validateDeserializedMultiPolygon(chunks[i], getName().c_str());
        }
        /// `validateDeserializedMultiPolygon` runs `boost::geometry::correct`, which can append
        /// closing points that were not charged against `budget.points`. Recount from the
        /// normalized geometry and re-enforce the cap so an accepted state never exceeds the
        /// limit that this same reader applies to serialized bytes.
        data.total_points = recountPolygonalPointsAndCheck(chunks, getName().c_str());
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto result = AggregateFunctionGroupPolygonUnion::data(place).getResult(getName().c_str());
        insertMultiPolygonIntoColumn(result, to);
    }
};


AggregateFunctionPtr createAggregateFunctionGroupPolygonUnion(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertUnary(name, argument_types);
    assertNoParameters(name, parameters);

    const auto & arg_type = argument_types[0];

    if (arg_type->getName() == "Geometry")
        return std::make_shared<AggregateFunctionGroupPolygonUnion>(
            arg_type, parameters, GeometryColumnType::Polygon /* unused for variant */, true);

    auto geo_type = getGeometryColumnTypeFromDataType(arg_type);
    if (!geo_type)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument of function {} must be a polygonal Geo type (Ring, Polygon, MultiPolygon), got {}",
            name,
            arg_type->getName());

    switch (*geo_type)
    {
        case GeometryColumnType::Ring:
        case GeometryColumnType::Polygon:
        case GeometryColumnType::MultiPolygon:
            break;
        case GeometryColumnType::Point:
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument of function {} must not be Point", name);
        case GeometryColumnType::Linestring:
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument of function {} must not be LineString", name);
        case GeometryColumnType::MultiLinestring:
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument of function {} must not be MultiLineString", name);
        case GeometryColumnType::Null:
            break;
    }

    return std::make_shared<AggregateFunctionGroupPolygonUnion>(arg_type, parameters, *geo_type, false);
}

}

void registerAggregateFunctionGroupPolygonUnion(AggregateFunctionFactory & factory);
void registerAggregateFunctionGroupPolygonUnion(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Computes the union of all polygonal geometries in the group.

Coordinates are interpreted as Cartesian (planar), not spherical, regardless of whether the input values represent longitude/latitude. There is no spherical variant of this function.

For typed columns, accepts `Ring`, `Polygon`, and `MultiPolygon`. Rejects `Point`, `LineString`, and `MultiLineString`.

For `Geometry` (Variant) columns, accepts any active value that is structurally polygonal. Because `Ring` and `LineString` are structurally identical in `ColumnVariant` (both are `Array(Tuple(Float64, Float64))`), and similarly `Polygon` and `MultiLineString`, a `LineString` or `MultiLineString` stored in a `Geometry` column will be interpreted as `Ring` or `Polygon` respectively.

Empty geometry inputs are silently skipped (neutral element for union).

The result is a `MultiPolygon`. Returns an empty `MultiPolygon` for empty groups. Invalid polygonal input raises an exception.
    )";
    FunctionDocumentation::Syntax syntax = "groupPolygonUnion(polygon)";
    FunctionDocumentation::Arguments arguments
        = {{"polygon",
            "A polygonal geometry value (`Ring`, `Polygon`, `MultiPolygon`) or a `Geometry` value with a structurally polygonal "
            "active type.",
            {"Ring", "Polygon", "MultiPolygon", "Geometry"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the union as a MultiPolygon.", {"MultiPolygon"}};
    FunctionDocumentation::Examples examples
        = {{"Union of two overlapping polygons",
            R"(
SELECT wkt(groupPolygonUnion(p)) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'),
        readWKTPolygon('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))')
    ]) AS p
)
            )",
            R"(
MULTIPOLYGON(((1 2,1 3,3 3,3 1,2 1,2 0,0 0,0 2,1 2)))
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = false};
    factory.registerFunction("groupPolygonUnion", {createAggregateFunctionGroupPolygonUnion, documentation, properties});
}

}
