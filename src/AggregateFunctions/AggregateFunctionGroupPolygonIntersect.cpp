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

constexpr size_t INTERSECT_REDUCTION_THRESHOLD = 8;


enum class IntersectMode : UInt8
{
    Uninitialized = 0,
    NonEmpty = 1,
    Empty = 2
};


struct GroupPolygonIntersectData
{
    IntersectMode mode = IntersectMode::Uninitialized;
    std::vector<CartesianMultiPolygon> chunks; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    size_t total_points = 0;

    void add(CartesianMultiPolygon && mp, const char * function_name)
    {
        if (mode == IntersectMode::Empty)
            return;

        if (mp.empty())
        {
            mode = IntersectMode::Empty;
            chunks.clear();
            total_points = 0;
            return;
        }

        size_t added = countMultiPolygonPoints(mp);
        checkPolygonalStateBudget(total_points, added, function_name);
        total_points += added;

        if (mode == IntersectMode::Uninitialized)
        {
            mode = IntersectMode::NonEmpty;
            chunks.push_back(std::move(mp));
            return;
        }

        chunks.push_back(std::move(mp));
        maybeReduce(function_name);
    }

    void merge(const GroupPolygonIntersectData & other, const char * function_name)
    {
        if (mode == IntersectMode::Empty)
            return;

        if (other.mode == IntersectMode::Empty)
        {
            mode = IntersectMode::Empty;
            chunks.clear();
            total_points = 0;
            return;
        }

        if (other.mode == IntersectMode::Uninitialized)
            return;

        if (mode == IntersectMode::Uninitialized)
        {
            mode = other.mode;
            chunks = other.chunks;
            total_points = other.total_points;
            return;
        }

        checkPolygonalStateBudget(total_points, other.total_points, function_name);
        total_points += other.total_points;
        chunks.insert(chunks.end(), other.chunks.begin(), other.chunks.end());
        maybeReduce(function_name);
    }

    void maybeReduce(const char * function_name)
    {
        if (chunks.size() > INTERSECT_REDUCTION_THRESHOLD)
            reduce(function_name);
    }

    /// Balanced pairwise reduction with early-empty short-circuit.
    void reduce(const char * function_name)
    {
        if (chunks.size() <= 1)
        {
            recountPoints(function_name);
            return;
        }

        while (chunks.size() > 1)
        {
            size_t n = chunks.size();
            size_t out = 0;
            for (size_t i = 0; i + 1 < n; i += 2)
            {
                CartesianMultiPolygon tmp;
                boost::geometry::intersection(chunks[i], chunks[i + 1], tmp);
                if (tmp.empty())
                {
                    mode = IntersectMode::Empty;
                    chunks.clear();
                    total_points = 0;
                    return;
                }
                chunks[out++] = std::move(tmp);
            }
            if (n % 2 == 1)
                chunks[out++] = std::move(chunks[n - 1]);
            chunks.resize(out);
        }

        if (chunks.empty() || chunks[0].empty())
        {
            mode = IntersectMode::Empty;
            chunks.clear();
            total_points = 0;
            return;
        }

        recountPoints(function_name);
    }

    /// Boost intersection may add intersection vertices, so the post-reduction sum can grow.
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
        if (mode == IntersectMode::Uninitialized || mode == IntersectMode::Empty)
            return {};

        reduce(function_name);
        if (mode == IntersectMode::Empty || chunks.empty())
            return {};

        return chunks[0];
    }
};


class AggregateFunctionGroupPolygonIntersect final
    : public IAggregateFunctionDataHelper<GroupPolygonIntersectData, AggregateFunctionGroupPolygonIntersect>
{
private:
    GeometryColumnType geo_type;
    bool is_variant = false;
    VariantTypeMap variant_type_map;

public:
    AggregateFunctionGroupPolygonIntersect(
        const DataTypePtr & argument_type, const Array & parameters_, GeometryColumnType geo_type_, bool is_variant_)
        : IAggregateFunctionDataHelper<GroupPolygonIntersectData, AggregateFunctionGroupPolygonIntersect>(
              {argument_type}, parameters_, DataTypeFactory::instance().get("MultiPolygon"))
        , geo_type(geo_type_)
        , is_variant(is_variant_)
    {
        if (is_variant)
            variant_type_map = buildVariantTypeMap(argument_type);
    }

    String getName() const override { return "groupPolygonIntersection"; }

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
        AggregateFunctionGroupPolygonIntersect::data(place).add(std::move(mp), getName().c_str());
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        AggregateFunctionGroupPolygonIntersect::data(place).merge(
            AggregateFunctionGroupPolygonIntersect::data(rhs), getName().c_str());
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinaryLittleEndian(GEO_SERDE_VERSION, buf);

        const auto & data = AggregateFunctionGroupPolygonIntersect::data(place);
        writeBinaryLittleEndian(static_cast<UInt8>(data.mode), buf);

        if (data.mode == IntersectMode::NonEmpty)
        {
            writeVarUInt(data.chunks.size(), buf);
            for (const auto & chunk : data.chunks)
                serializeGeoMultiPolygon(chunk, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        UInt8 version;
        readBinaryLittleEndian(version, buf);
        if (version != GEO_SERDE_VERSION)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Unsupported serialization version {} for aggregate function {} (expected {})",
                static_cast<int>(version),
                getName(),
                static_cast<int>(GEO_SERDE_VERSION));

        auto & data = AggregateFunctionGroupPolygonIntersect::data(place);
        UInt8 mode_val;
        readBinaryLittleEndian(mode_val, buf);
        if (mode_val > static_cast<UInt8>(IntersectMode::Empty))
            throw Exception(
                ErrorCodes::INCORRECT_DATA, "Invalid serialized mode {} for aggregate function {}", static_cast<int>(mode_val), getName());

        data.mode = static_cast<IntersectMode>(mode_val);

        if (data.mode == IntersectMode::NonEmpty)
        {
            UInt64 chunk_count;
            readVarUInt(chunk_count, buf);
            if (chunk_count == 0)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Corrupted state of aggregate function {}: mode is NonEmpty but chunk count is 0",
                    getName());
            if (chunk_count > MAX_CHUNKS_PER_STATE)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Corrupted state of aggregate function {}: {} chunks (limit {})",
                    getName(),
                    chunk_count,
                    MAX_CHUNKS_PER_STATE);

            data.chunks.resize(chunk_count);
            PolygonalStateBudget budget;
            for (UInt64 i = 0; i < chunk_count; ++i)
            {
                data.chunks[i] = deserializeGeoMultiPolygon(buf, getName().c_str(), budget);
                validateDeserializedMultiPolygon(data.chunks[i], getName().c_str());
            }
            data.total_points = budget.points;
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto result = AggregateFunctionGroupPolygonIntersect::data(place).getResult(getName().c_str());
        insertMultiPolygonIntoColumn(result, to);
    }
};


AggregateFunctionPtr createAggregateFunctionGroupPolygonIntersect(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertUnary(name, argument_types);
    assertNoParameters(name, parameters);

    const auto & arg_type = argument_types[0];

    if (arg_type->getName() == "Geometry")
        return std::make_shared<AggregateFunctionGroupPolygonIntersect>(
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

    return std::make_shared<AggregateFunctionGroupPolygonIntersect>(arg_type, parameters, *geo_type, false);
}

}

void registerAggregateFunctionGroupPolygonIntersection(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Computes the intersection of all polygonal geometries in the group.

For typed columns, accepts `Ring`, `Polygon`, and `MultiPolygon`. Rejects `Point`, `LineString`, and `MultiLineString`.

For `Geometry` (Variant) columns, accepts any active value that is structurally polygonal. Because `Ring` and `LineString` are structurally identical in `ColumnVariant` (both are `Array(Tuple(Float64, Float64))`), and similarly `Polygon` and `MultiLineString`, a `LineString` or `MultiLineString` stored in a `Geometry` column will be interpreted as `Ring` or `Polygon` respectively.

Empty geometry inputs are absorbing: once encountered, the result is immediately empty.

The result is a `MultiPolygon`. Returns an empty `MultiPolygon` for empty groups or when the intersection is empty. Invalid polygonal input raises an exception.

The function short-circuits: once the accumulated intersection becomes empty, further inputs are ignored.
    )";
    FunctionDocumentation::Syntax syntax = "groupPolygonIntersection(polygon)";
    FunctionDocumentation::Arguments arguments
        = {{"polygon",
            "A polygonal geometry value (`Ring`, `Polygon`, `MultiPolygon`) or a `Geometry` value with a structurally polygonal "
            "active type.",
            {"Ring", "Polygon", "MultiPolygon", "Geometry"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the intersection as a MultiPolygon.", {"MultiPolygon"}};
    FunctionDocumentation::Examples examples
        = {{"Intersection of two overlapping polygons",
            R"(
SELECT wkt(groupPolygonIntersection(p)) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))'),
        readWKTPolygon('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')
    ]) AS p
)
            )",
            R"(
MULTIPOLYGON(((1 3,3 3,3 1,1 1,1 3)))
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = false};
    factory.registerFunction("groupPolygonIntersection", {createAggregateFunctionGroupPolygonIntersect, documentation, properties});
}

}
