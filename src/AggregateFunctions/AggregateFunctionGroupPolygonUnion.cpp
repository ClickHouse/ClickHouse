#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGeoUtils.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <DataTypes/DataTypeFactory.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <bit>
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

constexpr size_t MAX_SERIALIZED_UNION_CHUNKS = 16;


/// A factor of four keeps the number of occupied tiers within the version-1 wire limit.
/// Cache point counts: recounting a large retained chunk on each input would itself be quadratic.
constexpr size_t unionChunkTier(size_t points)
{
    return points ? (std::bit_width(points) - 1) / 2 : 0;
}

static_assert(unionChunkTier(MAX_POINTS_IN_POLYGONAL_STATE) + 1 <= MAX_SERIALIZED_UNION_CHUNKS);


struct GroupPolygonUnionData
{
    struct Chunk
    {
        CartesianMultiPolygon geometry;
        size_t points;
    };

    std::vector<Chunk> chunks; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    size_t total_points = 0;

    static Chunk unite(
        const Chunk & lhs, const Chunk & rhs, const char * function_name, std::optional<size_t> max_result_points = {})
    {
        CartesianMultiPolygon result;
        unionPolygonalGeometries(lhs.geometry, rhs.geometry, result);
        if (result.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} produced an empty union from non-empty inputs", function_name);
        normalizeAndValidatePolygonalResult(result, function_name, max_result_points);
        size_t points = countMultiPolygonPoints(result);
        return {std::move(result), points};
    }

    void insertChunk(Chunk chunk, const char * function_name, bool final_chunk)
    {
        if (chunk.geometry.empty())
            return;

        /// Only combine comparable geometries during accumulation. A large prefix remains in
        /// its tier until enough new geometry has accumulated, instead of joining every batch.
        while (true)
        {
            const auto tier = unionChunkTier(chunk.points);
            const auto it = std::find_if(chunks.begin(), chunks.end(), [tier](const auto & existing)
            {
                return unionChunkTier(existing.points) == tier;
            });
            if (it == chunks.end())
            {
                total_points += chunk.points;
                chunks.push_back(std::move(chunk));
                return;
            }

            /// If this is the last pending chunk and the only retained tier, this overlay
            /// is the complete state. Enforce its budget before expensive validation.
            auto combined = unite(
                *it, chunk, function_name,
                final_chunk && chunks.size() == 1 ? std::optional{MAX_POINTS_IN_POLYGONAL_STATE} : std::nullopt);
            total_points -= it->points;
            chunks.erase(it);
            chunk = std::move(combined);
        }
    }

    void reduceAll(const char * function_name)
    {
        /// There are only logarithmically many chunks. Merge the smallest remaining pair,
        /// including when finalization is followed by more inputs in a window or running state.
        while (chunks.size() > 1)
        {
            std::sort(chunks.begin(), chunks.end(), [](const auto & lhs, const auto & rhs)
            {
                return lhs.points > rhs.points;
            });
            auto & lhs = chunks[chunks.size() - 2];
            const auto & rhs = chunks.back();
            /// Only the final pair has no remaining geometry that could reduce its point count.
            auto combined = unite(
                lhs, rhs, function_name, chunks.size() == 2 ? std::optional{MAX_POINTS_IN_POLYGONAL_STATE} : std::nullopt);
            total_points -= lhs.points + rhs.points;
            total_points += combined.points;
            lhs = std::move(combined);
            chunks.pop_back();
        }
    }

    void checkPointBudget(const char * function_name) const
    {
        if (total_points > MAX_POINTS_IN_POLYGONAL_STATE)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Aggregate function {} state has too many points after reduction: {} (limit {})",
                function_name,
                total_points,
                MAX_POINTS_IN_POLYGONAL_STATE);
    }

    void compactIfOverBudget(const char * function_name)
    {
        /// Overlap between retained tiers can make their sum larger than the actual union.
        /// Keep the existing policy of reducing before rejecting an over-budget state.
        if (total_points > MAX_POINTS_IN_POLYGONAL_STATE)
        {
            reduceAll(function_name);
            checkPointBudget(function_name);
        }
    }

    void add(CartesianMultiPolygon && mp, const char * function_name)
    {
        size_t points = countMultiPolygonPoints(mp);
        insertChunk({std::move(mp), points}, function_name, true);
        compactIfOverBudget(function_name);
    }

    void merge(const GroupPolygonUnionData & other, const char * function_name)
    {
        if (this == &other)
            return;
        /// Copy each right-hand chunk only when it is needed. The right state remains reusable.
        for (size_t i = 0; i < other.chunks.size(); ++i)
            insertChunk(other.chunks[i], function_name, i + 1 == other.chunks.size());
        compactIfOverBudget(function_name);
    }

    const CartesianMultiPolygon & getResult(const char * function_name)
    {
        static const CartesianMultiPolygon empty_result;

        if (chunks.empty())
            return empty_result;
        reduceAll(function_name);
        /// An overlay can add intersection vertices, including during finalization.
        checkPointBudget(function_name);
        return chunks[0].geometry;
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
        const auto value = getGeometryColumnValue(columns[0], row_num, geo_type, is_variant, variant_type_map);
        auto current_type = value.type;
        if (is_variant)
            current_type = normalizePolygonalVariantType(current_type);

        if (current_type == GeometryColumnType::Null)
            return;

        auto mp = columnToMultiPolygon(*value.column, value.row_num, current_type, getName().c_str());
        AggregateFunctionGroupPolygonUnion::data(place).add(std::move(mp), getName().c_str());
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        AggregateFunctionGroupPolygonUnion::data(place).merge(AggregateFunctionGroupPolygonUnion::data(rhs), getName().c_str());
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinaryLittleEndian(GEO_SERDE_VERSION, buf);

        const auto & chunks = AggregateFunctionGroupPolygonUnion::data(place).chunks;
        writeVarUInt(chunks.size(), buf);
        for (const auto & chunk : chunks)
            serializeGeoMultiPolygon(chunk.geometry, buf);
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
        UInt64 chunk_count = 0;
        readVarUInt(chunk_count, buf);
        /// Version 1 allowed up to 16 arbitrary chunks. The size tiers use fewer chunks without
        /// changing that format; retain the old bound when reading existing states.
        if (chunk_count > MAX_SERIALIZED_UNION_CHUNKS)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Corrupted state of aggregate function {}: {} chunks (limit {})",
                getName(),
                chunk_count,
                MAX_SERIALIZED_UNION_CHUNKS);

        std::vector<CartesianMultiPolygon> restored_chunks(chunk_count); // STYLE_CHECK_ALLOW_STD_CONTAINERS
        PolygonalStateBudget budget;
        for (UInt64 i = 0; i < chunk_count; ++i)
        {
            restored_chunks[i] = deserializeGeoMultiPolygon(buf, getName().c_str(), budget);
            validateDeserializedMultiPolygon(restored_chunks[i], getName().c_str());
        }
        /// `validateDeserializedMultiPolygon` runs `boost::geometry::correct`, which can append
        /// closing points that were not charged against `budget.points`. Recount from the
        /// normalized geometry and re-enforce the cap so an accepted state never exceeds the
        /// limit that this same reader applies to serialized bytes.
        recountPolygonalPointsAndCheck(restored_chunks, getName().c_str());

        data.chunks.clear();
        data.total_points = 0;
        std::erase_if(restored_chunks, [](const auto & chunk)
        {
            return chunk.empty();
        });
        for (size_t i = 0; i < restored_chunks.size(); ++i)
        {
            auto & chunk = restored_chunks[i];
            size_t points = countMultiPolygonPoints(chunk);
            data.insertChunk({std::move(chunk), points}, getName().c_str(), i + 1 == restored_chunks.size());
        }
        data.compactIfOverBudget(getName().c_str());
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & result = AggregateFunctionGroupPolygonUnion::data(place).getResult(getName().c_str());
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

    auto geo_type = getUnambiguousGeometryColumnTypeFromDataType(arg_type, name);
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
        case GeometryColumnType::MultiPolygon: break;
        case GeometryColumnType::Point:
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument of function {} must not be Point", name);
        case GeometryColumnType::MultiPoint:
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument of function {} must not be MultiPoint", name);
        case GeometryColumnType::Linestring:
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument of function {} must not be LineString", name);
        case GeometryColumnType::MultiLinestring:
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument of function {} must not be MultiLineString", name);
        case GeometryColumnType::Null: break;
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

For typed columns, accepts `Ring`, `Polygon`, and `MultiPolygon`. Rejects `Point`, `MultiPoint`, `LineString`, and `MultiLineString`.

For `Geometry` (Variant) columns, an active `LineString` is interpreted as a `Ring`, and an active `MultiLineString` as a `Polygon`, because their underlying array shapes match. Other non-polygonal active types are rejected.

Empty geometry inputs are silently skipped (neutral element for union).

The result is a `MultiPolygon`. Returns an empty `MultiPolygon` for empty groups. Invalid polygonal input raises an exception.

The geometric union is order-independent, but floating-point overlay operations are not exactly associative and the returned `MultiPolygon` is not canonicalized. Its raw array layout and floating-point coordinates can therefore depend on input and merge order.

`NULL` values inside a `Geometry` (Variant) column are skipped. The polygonal argument types are `Array`-based and cannot be wrapped in `Nullable`; a literal `NULL` argument follows the standard aggregate convention and yields `NULL` (like `sum` and unlike `count`).
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
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    /// `returns_default_when_only_null` must stay false: with true, a literal NULL argument is replaced by
    /// `AggregateFunctionNothingUInt64`, i.e. `groupPolygonUnion(NULL)` would return `0 :: UInt64` instead of `NULL`.
    /// The polygonal argument types are `Array`-based and cannot be wrapped in `Nullable`, so a literal NULL
    /// is the only way the `Null` combinator applies here.
    /// `is_order_dependent` must stay true: the result is geometrically order-invariant, but `boost::geometry`
    /// set operations on floating point are not exactly associative and the returned `MultiPolygon` is not
    /// canonicalized, so the observable array layout may differ across input orders.
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    factory.registerFunction("groupPolygonUnion", {createAggregateFunctionGroupPolygonUnion, documentation, properties});
}

}
