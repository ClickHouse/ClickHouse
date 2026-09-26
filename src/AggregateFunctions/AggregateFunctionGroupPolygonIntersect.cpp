#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGeoUtils.h>
#include <AggregateFunctions/AggregateFunctionGeoMonotoneComponents.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <DataTypes/DataTypeFactory.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <boost/geometry.hpp>
#include <boost/geometry/index/rtree.hpp>

#include <algorithm>
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

/// Keep accepting version-1 states emitted by the previous chunked implementation.
constexpr size_t MAX_SERIALIZED_INTERSECT_CHUNKS = 8;

/// Compare exact vertex cycles without changing the stored ring or allocating a rotated copy.
/// Rings have already been oriented and checked for finite coordinates.
bool equalExteriorCycles(const CartesianRing & left, const CartesianRing & right)
{
    if (left.size() != right.size())
        return false;

    auto equal = [](const auto & first, const auto & second)
    {
        return first.template get<0>() == second.template get<0>() && first.template get<1>() == second.template get<1>();
    };
    if (std::equal(left.begin(), left.end(), right.begin(), equal))
        return true;

    /// Boost may accept an approximately closed ring. Only an exact closing point is redundant.
    if (!equal(left.front(), left.back()) || !equal(right.front(), right.back()))
        return false;

    /// Exclude only the closing point; repeated vertices remain part of the cycle.
    const size_t size = left.size() - 1;
    auto least_rotation = [&](const CartesianRing & ring)
    {
        size_t first = 0;
        size_t second = 1;
        size_t matched = 0;
        while (first < size && second < size && matched < size)
        {
            const auto & a = ring[(first + matched) % size];
            const auto & b = ring[(second + matched) % size];
            if (equal(a, b))
            {
                ++matched;
                continue;
            }
            if (a.get<0>() > b.get<0>() || (a.get<0>() == b.get<0>() && a.get<1>() > b.get<1>()))
            {
                first += matched + 1;
                if (first == second)
                    ++first;
            }
            else
            {
                second += matched + 1;
                if (first == second)
                    ++second;
            }
            matched = 0;
        }
        return std::min(first, second);
    };

    const size_t first = least_rotation(left);
    const size_t second = least_rotation(right);
    for (size_t i = 0; i < size; ++i)
        if (!equal(left[(first + i) % size], right[(second + i) % size]))
            return false;
    return true;
}


enum class IntersectMode : UInt8
{
    Uninitialized = 0,
    NonEmpty = 1,
    Empty = 2
};


struct GroupPolygonIntersectData
{
    using Box = boost::geometry::model::box<CartesianPoint>;
    using HoleIndex = boost::geometry::index::rtree<
        Box,
        boost::geometry::index::quadratic<16>,
        boost::geometry::index::indexable<Box>,
        boost::geometry::index::equal_to<Box>,
        AllocatorWithMemoryTracking<Box>>;

    IntersectMode mode = IntersectMode::Uninitialized;
    std::vector<CartesianMultiPolygon> chunks; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    size_t total_points = 0;
    std::unique_ptr<HoleIndex> hole_index;

    struct MonotoneHoleState
    {
        CartesianRing exterior;
        GeoRectangle bounds{};
        GeoMonotoneRing hole;
        std::unique_ptr<GeoMonotoneComponents> components;

        size_t points() const
        {
            return exterior.size() + (components ? components->total_points : hole.points());
        }

        template <typename Chain>
        bool strictlyInside(const GeoMonotoneRingImpl<Chain> & ring) const
        {
            auto inside = [&](const CartesianPoint & point)
            {
                return bounds.min_x < point.get<0>() && point.get<0>() < bounds.max_x
                    && bounds.min_y < point.get<1>() && point.get<1>() < bounds.max_y;
            };
            return std::all_of(ring.lower.begin(), ring.lower.end(), inside)
                && std::all_of(ring.upper.begin(), ring.upper.end(), inside);
        }

        template <typename Chain>
        bool insert(const GeoMonotoneRingImpl<Chain> & incoming)
        {
            if (components)
                return components->insert(incoming);
            if (hole.canAppend(incoming))
            {
                hole.append(incoming);
                return true;
            }
            /// Avoid copying an old ring just to discover an unsupported internal overlap.
            if (!(incoming.canAppend(hole)
                    || incoming.lower.back().template get<0>() < hole.lower.front().get<0>()
                    || hole.lower.back().get<0>() < incoming.lower.front().template get<0>()))
                return false;
            auto promoted = std::make_unique<GeoMonotoneComponents>();
            promoted->insertDisjoint(hole);
            if (!promoted->insert(incoming))
                return false;
            components = std::move(promoted);
            hole = {};
            return true;
        }
    };
    std::unique_ptr<MonotoneHoleState> monotone_hole;

    CartesianMultiPolygon monotoneGeometry() const
    {
        CartesianMultiPolygon result;
        result.emplace_back();
        result[0].outer() = monotone_hole->exterior;
        if (monotone_hole->components)
        {
            result[0].inners().reserve(monotone_hole->components->rings.size());
            for (const auto & [left_x, ring] : monotone_hole->components->rings)
                result[0].inners().push_back(ring.materialize());
        }
        else
            result[0].inners().push_back(monotone_hole->hole.materialize());
        return result;
    }

    void materializeMonotoneHole()
    {
        if (!monotone_hole)
            return;
        if (chunks.empty())
            chunks.push_back(monotoneGeometry());
        monotone_hole.reset();
    }

    std::unique_ptr<MonotoneHoleState> makeMonotoneState() const
    {
        if (chunks.size() != 1 || chunks[0].size() != 1 || chunks[0][0].inners().empty())
            return {};
        const auto & polygon = chunks[0][0];
        const auto bounds = tryGetGeoRectangle(polygon.outer());
        if (!bounds)
            return {};
        auto state = std::make_unique<MonotoneHoleState>();
        state->exterior = polygon.outer();
        state->bounds = *bounds;
        if (polygon.inners().size() > 1)
            state->components = std::make_unique<GeoMonotoneComponents>();
        for (const auto & ring : polygon.inners())
        {
            auto parsed = GeoMonotoneRing::fromRing(ring);
            if (!parsed || !state->strictlyInside(*parsed))
                return {};
            if (state->components)
            {
                if (!state->components->insertDisjoint(*parsed))
                    return {};
            }
            else
                state->hole = std::move(*parsed);
        }
        return state;
    }

    void updatedMonotoneState()
    {
        total_points = monotone_hole->points();
        chunks.clear();
        hole_index.reset();
    }

    void checkMonotoneBudget(const char * function_name) const
    {
        if (total_points > MAX_POINTS_IN_POLYGONAL_STATE)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Aggregate function {} state has too many points after reduction: {} (limit {})",
                function_name, total_points, MAX_POINTS_IN_POLYGONAL_STATE);
    }

    bool appendMonotoneHoles(const CartesianMultiPolygon & incoming, const char * function_name)
    {
        if (incoming.size() != 1)
            return false;
        if (!monotone_hole && (chunks.size() != 1 || chunks[0].size() != 1))
            return false;
        const auto & exterior = monotone_hole ? monotone_hole->exterior : chunks[0][0].outer();
        if (!equalExteriorCycles(exterior, incoming[0].outer()))
            return false;
        if (incoming[0].inners().empty())
            return true;
        for (size_t i = 0; i < incoming[0].inners().size(); ++i)
        {
            std::unique_ptr<MonotoneHoleState> initial;
            auto next = GeoMonotoneRing::fromRing(incoming[0].inners()[i]);
            if (next && !monotone_hole)
                initial = makeMonotoneState();
            auto * state = monotone_hole ? monotone_hole.get() : initial.get();
            if (!next || !state || !state->strictlyInside(*next) || !state->insert(*next))
            {
                if (i == 0)
                    return false;
                /// Earlier holes have already been applied exactly. Consume only the unprocessed
                /// suffix through the eager general path; do not reapply a partially consumed RHS.
                CartesianMultiPolygon remaining;
                remaining.resize(1);
                remaining[0].outer() = incoming[0].outer();
                remaining[0].inners().assign(incoming[0].inners().begin() + i, incoming[0].inners().end());
                materializeMonotoneHole();
                add(std::move(remaining), function_name);
                return true;
            }
            if (initial)
                monotone_hole = std::move(initial);
            updatedMonotoneState();
        }
        /// A row or merge is one update. Internal component insertion may temporarily exceed
        /// the point budget before later bridge holes reduce the size of that same update.
        checkMonotoneBudget(function_name);
        return true;
    }

    bool mergeMonotoneHoles(const MonotoneHoleState & other, const char * function_name)
    {
        if (!monotone_hole && (chunks.size() != 1 || chunks[0].size() != 1))
            return false;
        const auto & exterior = monotone_hole ? monotone_hole->exterior : chunks[0][0].outer();
        if (!equalExteriorCycles(exterior, other.exterior))
            return false;
        auto initial = monotone_hole ? nullptr : makeMonotoneState();
        auto * state = monotone_hole ? monotone_hole.get() : initial.get();
        if (!state)
            return false;
        if (!other.components)
        {
            if (!state->insert(other.hole))
                return false;
            if (initial)
                monotone_hole = std::move(initial);
            updatedMonotoneState();
        }
        else
        {
            for (auto it = other.components->rings.begin(); it != other.components->rings.end(); ++it)
            {
                if (!state->insert(it->second))
                {
                    if (it == other.components->rings.begin())
                        return false;
                    CartesianMultiPolygon remaining;
                    remaining.resize(1);
                    remaining[0].outer() = other.exterior;
                    for (; it != other.components->rings.end(); ++it)
                        remaining[0].inners().push_back(it->second.materialize());
                    materializeMonotoneHole();
                    add(std::move(remaining), function_name);
                    return true;
                }
                if (initial)
                    monotone_hole = std::move(initial);
                updatedMonotoneState();
            }
        }
        checkMonotoneBudget(function_name);
        return true;
    }

    /// For a common exterior, intersection adds the incoming holes to the existing holes.
    /// Strictly disjoint bounding boxes prove that no old/new hole boundaries interact.
    /// Each operand has already passed full validation, so this preserves topology and a
    /// nonempty result without revisiting the entire growing boundary. No input is deferred.
    bool appendDisjointHoles(const CartesianMultiPolygon & incoming, const char * function_name)
    {
        if (chunks.size() != 1 || chunks[0].size() != 1 || incoming.size() != 1)
            return false;

        auto & current = chunks[0][0];
        const auto & next = incoming[0];
        if (!equalExteriorCycles(current.outer(), next.outer()))
            return false;
        for (const auto & point : current.outer())
        {
            /// R-tree splitting uses box areas. Keep those products within `Float64`;
            /// geometries outside this range use the existing wide overlay path.
            if (std::abs(point.get<0>()) > 0x1p128 || std::abs(point.get<1>()) > 0x1p128)
                return false;
        }

        if (next.inners().empty())
            return true;

        if (!hole_index)
        {
            hole_index = std::make_unique<HoleIndex>();
            for (const auto & ring : current.inners())
                hole_index->insert(boost::geometry::return_envelope<Box>(ring));
        }

        size_t added_points = 0;
        std::vector<Box, AllocatorWithMemoryTracking<Box>> boxes; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        boxes.reserve(next.inners().size());
        for (const auto & ring : next.inners())
        {
            auto box = boost::geometry::return_envelope<Box>(ring);
            if (hole_index->qbegin(boost::geometry::index::intersects(box)) != hole_index->qend())
                return false;
            boxes.push_back(box);
            added_points += ring.size();
        }

        if (total_points > MAX_POINTS_IN_POLYGONAL_STATE || added_points > MAX_POINTS_IN_POLYGONAL_STATE - total_points)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Aggregate function {} state has too many points after reduction: {} (limit {})",
                function_name,
                total_points + added_points,
                MAX_POINTS_IN_POLYGONAL_STATE);

        current.inners().insert(current.inners().end(), next.inners().begin(), next.inners().end());
        for (const auto & box : boxes)
            hole_index->insert(box);
        total_points += added_points;
        return true;
    }

    bool appendOptimizedHoles(const CartesianMultiPolygon & incoming, const char * function_name)
    {
        if (monotone_hole)
        {
            if (appendMonotoneHoles(incoming, function_name))
                return true;
            materializeMonotoneHole();
            return appendDisjointHoles(incoming, function_name);
        }
        /// Build or reuse the existing hole index before considering a local boundary update.
        /// This also avoids inspecting a growing ring when a disjoint append already succeeds.
        return appendDisjointHoles(incoming, function_name) || appendMonotoneHoles(incoming, function_name);
    }

    void add(CartesianMultiPolygon && mp, const char * function_name)
    {
        if (mode == IntersectMode::Empty)
            return;

        if (mp.empty())
        {
            mode = IntersectMode::Empty;
            chunks.clear();
            hole_index.reset();
            monotone_hole.reset();
            total_points = 0;
            return;
        }

        if (mode == IntersectMode::NonEmpty && appendOptimizedHoles(mp, function_name))
            return;

        total_points += countMultiPolygonPoints(mp);
        chunks.push_back(std::move(mp));

        if (mode == IntersectMode::Uninitialized)
            mode = IntersectMode::NonEmpty;

        /// Keep `NonEmpty` as a fully reduced running intersection. Besides bounding state, this
        /// makes `Empty` observable before the next row is resolved or validated, so an absorbing
        /// result really can skip all subsequent inputs.
        if (chunks.size() > 1 || total_points > MAX_POINTS_IN_POLYGONAL_STATE)
            reduce(function_name);
    }

    void merge(const GroupPolygonIntersectData & other, const char * function_name)
    {
        if (this == &other || mode == IntersectMode::Empty)
            return;

        if (other.mode == IntersectMode::Empty)
        {
            mode = IntersectMode::Empty;
            chunks.clear();
            hole_index.reset();
            monotone_hole.reset();
            total_points = 0;
            return;
        }

        if (other.mode == IntersectMode::Uninitialized)
            return;

        if (mode == IntersectMode::Uninitialized)
        {
            mode = other.mode;
            if (other.monotone_hole)
                chunks.clear();
            else
                chunks = other.chunks;
            total_points = other.total_points;
            if (other.monotone_hole)
            {
                monotone_hole = std::make_unique<MonotoneHoleState>();
                monotone_hole->exterior = other.monotone_hole->exterior;
                monotone_hole->bounds = other.monotone_hole->bounds;
                monotone_hole->hole = other.monotone_hole->hole;
                if (other.monotone_hole->components)
                    monotone_hole->components = std::make_unique<GeoMonotoneComponents>(*other.monotone_hole->components);
            }
            if (!monotone_hole && (chunks.size() > 1 || total_points > MAX_POINTS_IN_POLYGONAL_STATE))
                reduce(function_name);
            return;
        }

        if (other.monotone_hole && mergeMonotoneHoles(*other.monotone_hole, function_name))
            return;

        CartesianMultiPolygon other_geometry;
        const CartesianMultiPolygon * incoming = nullptr;
        if (other.monotone_hole)
        {
            other_geometry = other.monotoneGeometry();
            incoming = &other_geometry;
        }
        else if (other.chunks.size() == 1)
            incoming = other.chunks.data();
        if (incoming && appendOptimizedHoles(*incoming, function_name))
            return;
        materializeMonotoneHole();

        total_points += other.total_points;
        if (incoming)
            chunks.push_back(*incoming);
        else
            chunks.insert(chunks.end(), other.chunks.begin(), other.chunks.end());

        /// A merged state must preserve the same eager running-intersection invariant as `add`.
        reduce(function_name);
    }

    /// Balanced pairwise reduction with early-empty short-circuit.
    void reduce(const char * function_name)
    {
        materializeMonotoneHole();
        if (chunks.size() <= 1)
        {
            recountPoints(function_name);
            return;
        }

        hole_index.reset();

        while (chunks.size() > 1)
        {
            size_t n = chunks.size();
            size_t out = 0;
            for (size_t i = 0; i + 1 < n; i += 2)
            {
                CartesianMultiPolygon tmp;
                intersectPolygonalGeometries(chunks[i], chunks[i + 1], tmp);
                if (tmp.empty())
                {
                    mode = IntersectMode::Empty;
                    chunks.clear();
                    total_points = 0;
                    return;
                }
                normalizeAndValidatePolygonalResult(
                    tmp, function_name, n == 2 ? std::optional{MAX_POINTS_IN_POLYGONAL_STATE} : std::nullopt);
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

    const CartesianMultiPolygon & getResult(const char * function_name)
    {
        static const CartesianMultiPolygon empty_result;

        if (mode == IntersectMode::Uninitialized || mode == IntersectMode::Empty)
            return empty_result;

        if (monotone_hole)
        {
            if (chunks.empty())
                chunks.push_back(monotoneGeometry());
            return chunks[0];
        }
        reduce(function_name);
        if (mode == IntersectMode::Empty || chunks.empty())
            return empty_result;

        return chunks[0];
    }
};


class AggregateFunctionGroupPolygonIntersect final
    : public IAggregateFunctionDataHelper<GroupPolygonIntersectData, AggregateFunctionGroupPolygonIntersect>
{
private:
    static constexpr auto name = "groupPolygonIntersection";
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

    String getName() const override { return name; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & state = AggregateFunctionGroupPolygonIntersect::data(place);
        if (state.mode == IntersectMode::Empty)
            return;

        const auto value = getGeometryColumnValue(columns[0], row_num, geo_type, is_variant, variant_type_map);
        auto current_type = value.type;
        if (is_variant)
            current_type = normalizePolygonalVariantType(current_type);

        if (current_type == GeometryColumnType::Null)
            return;

        auto mp = columnToMultiPolygon(*value.column, value.row_num, current_type, name);
        state.add(std::move(mp), name);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        AggregateFunctionGroupPolygonIntersect::data(place).merge(AggregateFunctionGroupPolygonIntersect::data(rhs), name);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinaryLittleEndian(GEO_SERDE_VERSION, buf);

        const auto & data = AggregateFunctionGroupPolygonIntersect::data(place);
        writeBinaryLittleEndian(static_cast<UInt8>(data.mode), buf);

        if (data.mode == IntersectMode::NonEmpty)
        {
            if (data.monotone_hole)
            {
                writeVarUInt(1, buf);
                serializeGeoMultiPolygon(data.monotoneGeometry(), buf);
                return;
            }
            chassert(data.chunks.size() == 1);
            writeVarUInt(data.chunks.size(), buf);
            for (const auto & chunk : data.chunks)
                serializeGeoMultiPolygon(chunk, buf);
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

        auto & data = AggregateFunctionGroupPolygonIntersect::data(place);
        data.monotone_hole.reset();
        data.hole_index.reset();
        UInt8 mode_val = 0;
        readBinaryLittleEndian(mode_val, buf);
        if (mode_val > static_cast<UInt8>(IntersectMode::Empty))
            throw Exception(
                ErrorCodes::INCORRECT_DATA, "Invalid serialized mode {} for aggregate function {}", static_cast<int>(mode_val), getName());

        data.mode = static_cast<IntersectMode>(mode_val);

        if (data.mode == IntersectMode::NonEmpty)
        {
            UInt64 chunk_count = 0;
            readVarUInt(chunk_count, buf);
            if (chunk_count == 0)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Corrupted state of aggregate function {}: mode is NonEmpty but chunk count is 0",
                    getName());
            /// Version-1 states from the previous chunked implementation could contain up to
            /// eight chunks. Reject larger shapes before the expensive compatibility reduction.
            if (chunk_count > MAX_SERIALIZED_INTERSECT_CHUNKS)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Corrupted state of aggregate function {}: {} chunks (limit {})",
                    getName(),
                    chunk_count,
                    MAX_SERIALIZED_INTERSECT_CHUNKS);

            data.chunks.resize(chunk_count);
            PolygonalStateBudget budget;
            for (UInt64 i = 0; i < chunk_count; ++i)
            {
                data.chunks[i] = deserializeGeoMultiPolygon(buf, name, budget);
                validateDeserializedMultiPolygon(data.chunks[i], name);
            }
            /// `validateDeserializedMultiPolygon` runs `boost::geometry::correct`, which can
            /// append closing points not charged against `budget.points`. Recount from the
            /// normalized geometry and re-enforce the cap.
            data.total_points = recountPolygonalPointsAndCheck(data.chunks, name);

            /// Restore the eager invariant before this state can be merged or serialized again.
            /// A legacy state whose chunks are already disjoint becomes the absorbing `Empty`
            /// state here rather than carrying a logically empty `NonEmpty` value forward.
            if (data.chunks.size() > 1)
                data.reduce(name);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & result = AggregateFunctionGroupPolygonIntersect::data(place).getResult(name);
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

    return std::make_shared<AggregateFunctionGroupPolygonIntersect>(arg_type, parameters, *geo_type, false);
}

}

void registerAggregateFunctionGroupPolygonIntersection(AggregateFunctionFactory & factory);
void registerAggregateFunctionGroupPolygonIntersection(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Computes the intersection of all polygonal geometries in the group.

Coordinates are interpreted as Cartesian (planar), not spherical, regardless of whether the input values represent longitude/latitude. There is no spherical variant of this function.

For typed columns, accepts `Ring`, `Polygon`, and `MultiPolygon`. Rejects `Point`, `MultiPoint`, `LineString`, and `MultiLineString`.

For `Geometry` (Variant) columns, an active `LineString` is interpreted as a `Ring`, and an active `MultiLineString` as a `Polygon`, because their underlying array shapes match. Other non-polygonal active types are rejected.

Empty geometry inputs are absorbing: once encountered, the result is immediately empty.

The result is a `MultiPolygon`. Returns an empty `MultiPolygon` for empty groups or when the intersection is empty. Invalid polygonal input raises an exception.

The function short-circuits: once the accumulated intersection becomes empty, further inputs are ignored.

The geometric intersection is order-independent, but floating-point overlay operations are not exactly associative and the returned `MultiPolygon` is not canonicalized. Its raw array layout and floating-point coordinates can therefore depend on input and merge order.

`NULL` values inside a `Geometry` (Variant) column are skipped. The polygonal argument types are `Array`-based and cannot be wrapped in `Nullable`; a literal `NULL` argument follows the standard aggregate convention and yields `NULL` (like `sum` and unlike `count`).
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
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    /// `returns_default_when_only_null` must stay false: with true, a literal NULL argument is replaced by
    /// `AggregateFunctionNothingUInt64`, i.e. `groupPolygonIntersection(NULL)` would return `0 :: UInt64` instead of `NULL`.
    /// The polygonal argument types are `Array`-based and cannot be wrapped in `Nullable`, so a literal NULL
    /// is the only way the `Null` combinator applies here.
    /// `is_order_dependent` must stay true: the result is geometrically order-invariant, but `boost::geometry`
    /// set operations on floating point are not exactly associative and the returned `MultiPolygon` is not
    /// canonicalized, so the observable array layout may differ across input orders.
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    factory.registerFunction("groupPolygonIntersection", {createAggregateFunctionGroupPolygonIntersect, documentation, properties});
}

}
