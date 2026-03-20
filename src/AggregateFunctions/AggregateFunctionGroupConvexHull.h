#pragma once

#include <AggregateFunctions/IAggregateFunction.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/WKB.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Functions/geometry.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/io/wkt/wkt.hpp>

#include <iomanip>
#include <limits>
#include <sstream>
#include <base/EnumReflection.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

template <typename Point>
struct AggregateFunctionGroupConvexHullData
{
    MultiPolygon<Point> accumulated;
    bool has_value = false;
    size_t add_count = 0;
};

/// WKBGeometry extension constants for types not in the WKB standard.
static constexpr WKBGeometry WKB_RING = static_cast<WKBGeometry>(100);
static constexpr WKBGeometry WKB_GEOMETRY = static_cast<WKBGeometry>(101);

template <typename Point>
class AggregateFunctionGroupConvexHull final
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupConvexHullData<Point>, AggregateFunctionGroupConvexHull<Point>>
{
private:
    using Data = AggregateFunctionGroupConvexHullData<Point>;
    WKBGeometry input_type;
    bool correct_geometry = true;
    size_t prune_interval;

public:
    static constexpr size_t kDefaultPruneInterval = 200;

private:

    static void pruneAccumulated(Data & state)
    {
        if (!state.has_value || state.accumulated.empty())
            return;

        Polygon<Point> hull;
        boost::geometry::convex_hull(state.accumulated, hull);

        state.accumulated.clear();
        state.accumulated.emplace_back(std::move(hull));
        state.add_count = 0;
    }

    static WKBGeometry resolveInputType(const DataTypePtr & type)
    {
        const auto & factory = DataTypeFactory::instance();

        if (factory.get(WKBPointTransform::name)->equals(*type))
            return WKBGeometry::Point;

        /// LineString and Ring share the same underlying structure Array(Tuple(Float64, Float64)).
        /// Disambiguate by checking the custom type name.
        if (factory.get(WKBLineStringTransform::name)->equals(*type) && type->getCustomName()
            && type->getCustomName()->getName() == WKBLineStringTransform::name)
            return WKBGeometry::LineString;

        /// MultiLineString and Polygon share the same underlying structure Array(Array(Tuple(Float64, Float64))).
        /// Disambiguate by checking the custom type name.
        if (factory.get(WKBMultiLineStringTransform::name)->equals(*type) && type->getCustomName()
            && type->getCustomName()->getName() == WKBMultiLineStringTransform::name)
            return WKBGeometry::MultiLineString;

        static const DataTypePtr RING = factory.get("Ring");
        if (RING->equals(*type))
            return WKB_RING;

        if (factory.get(WKBPolygonTransform::name)->equals(*type))
            return WKBGeometry::Polygon;

        if (factory.get(WKBMultiPolygonTransform::name)->equals(*type))
            return WKBGeometry::MultiPolygon;

        if (type->getCustomName() && type->getCustomName()->getName() == "Geometry")
            return WKB_GEOMETRY;

        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Unsupported geometry type for {}: {}. Expected Point, Ring, Polygon, MultiPolygon, LineString, MultiLineString, or Geometry",
            "groupConvexHull",
            type->getName());
    }

public:
    explicit AggregateFunctionGroupConvexHull(
        const DataTypes & argument_types_, bool correct_geometry_ = true, size_t prune_interval_ = kDefaultPruneInterval)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionGroupConvexHull<Point>>(
              argument_types_, {}, DataTypeFactory::instance().get("Ring"))
        , input_type(resolveInputType(argument_types_.at(0)))
        , correct_geometry(correct_geometry_)
        , prune_interval(prune_interval_)
    {
    }

    String getName() const override { return "groupConvexHull"; }

    bool allocatesMemoryInArena() const override { return false; }

    /// Accumulation helpers — shared by the main switch and the Geometry variant switch.
    static void accumulatePoint(Data & state, Point && point, bool should_correct)
    {
        Polygon<Point> polygon;
        polygon.outer().push_back(std::move(point));
        if (should_correct)
            boost::geometry::correct(polygon);
        state.accumulated.emplace_back(std::move(polygon));
        state.has_value = true;
    }

    static void accumulateRing(Data & state, Ring<Point> && ring, bool should_correct)
    {
        Polygon<Point> polygon;
        polygon.outer() = std::move(ring);
        if (should_correct)
            boost::geometry::correct(polygon);
        state.accumulated.emplace_back(std::move(polygon));
        state.has_value = true;
    }

    static void accumulatePolygon(Data & state, Polygon<Point> && poly, bool should_correct)
    {
        if (should_correct)
            boost::geometry::correct(poly);
        state.accumulated.emplace_back(std::move(poly));
        state.has_value = true;
    }

    static void accumulateMultiPolygon(Data & state, MultiPolygon<Point> && mpoly, bool should_correct)
    {
        for (auto & poly : mpoly)
        {
            if (should_correct)
                boost::geometry::correct(poly);
            state.accumulated.emplace_back(std::move(poly));
        }
        state.has_value = true;
    }

    static void accumulateLineString(Data & state, LineString<Point> && ls, bool should_correct)
    {
        Polygon<Point> polygon;
        polygon.outer().assign(ls.begin(), ls.end());
        if (should_correct)
            boost::geometry::correct(polygon);
        state.accumulated.emplace_back(std::move(polygon));
        state.has_value = true;
    }

    static void accumulateMultiLineString(Data & state, MultiLineString<Point> && mls, bool should_correct)
    {
        for (auto & ls : mls)
        {
            Polygon<Point> polygon;
            polygon.outer().assign(ls.begin(), ls.end());
            if (should_correct)
                boost::geometry::correct(polygon);
            state.accumulated.emplace_back(std::move(polygon));
        }
        state.has_value = true;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & state = this->data(place);

        /// If the second argument is provided and is 0, skip geometry correction for this row.
        bool should_correct = correct_geometry;
        if (this->argument_types.size() == 2)
        {
            const auto & col = assert_cast<const ColumnUInt8 &>(*columns[1]);
            should_correct = col.getData()[row_num] != 0;
        }

        if (input_type == WKB_GEOMETRY)
        {
            /// Geometry (Variant) columns need special discriminator-based handling.
            const auto & variant_col = assert_cast<const ColumnVariant &>(*columns[0]);

            auto local_discr = variant_col.localDiscriminatorAt(row_num);
            if (local_discr == ColumnVariant::NULL_DISCRIMINATOR)
                return; /// NULL row, skip

            Field field;
            variant_col.get(row_num, field);

            auto geo_type = magic_enum::enum_cast<GeometryColumnType>(static_cast<int>(variant_col.globalDiscriminatorAt(row_num)));
            if (!geo_type)
                return;

            switch (*geo_type)
            {
                case GeometryColumnType::Point:
                    accumulatePoint(state, getPointFromField<Point>(field), should_correct);
                    break;
                case GeometryColumnType::Ring:
                    accumulateRing(state, getRingFromField<Point>(field), should_correct);
                    break;
                case GeometryColumnType::Polygon:
                    accumulatePolygon(state, getPolygonFromField<Point>(field), should_correct);
                    break;
                case GeometryColumnType::MultiPolygon:
                    accumulateMultiPolygon(state, getMultiPolygonFromField<Point>(field), should_correct);
                    break;
                case GeometryColumnType::Linestring:
                    accumulateLineString(state, getLineStringFromField<Point>(field), should_correct);
                    break;
                case GeometryColumnType::MultiLinestring:
                    accumulateMultiLineString(state, getMultiLineStringFromField<Point>(field), should_correct);
                    break;
                case GeometryColumnType::Null:
                    break;
            }
        }
        else
        {
            /// For concrete geometry types, extract via Field which works for any column
            /// type including ColumnConst (unlike cut + bulk converters which crash on ColumnConst).
            Field field;
            columns[0]->get(row_num, field);

            switch (input_type)
            {
                case WKBGeometry::Point:
                    accumulatePoint(state, getPointFromField<Point>(field), should_correct);
                    break;
                case WKB_RING:
                    accumulateRing(state, getRingFromField<Point>(field), should_correct);
                    break;
                case WKBGeometry::Polygon:
                    accumulatePolygon(state, getPolygonFromField<Point>(field), should_correct);
                    break;
                case WKBGeometry::MultiPolygon:
                    accumulateMultiPolygon(state, getMultiPolygonFromField<Point>(field), should_correct);
                    break;
                case WKBGeometry::LineString:
                    accumulateLineString(state, getLineStringFromField<Point>(field), should_correct);
                    break;
                case WKBGeometry::MultiLineString:
                    accumulateMultiLineString(state, getMultiLineStringFromField<Point>(field), should_correct);
                    break;
                case WKB_GEOMETRY:
                    break; /// Already handled above
            }
        }

        /// Periodically prune interior points to bound memory.
        if (++state.add_count >= prune_interval)
            pruneAccumulated(state);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & state = this->data(place);
        const auto & rhs_state = this->data(rhs);

        if (!rhs_state.has_value)
            return;

        if (!state.has_value)
        {
            state.accumulated = rhs_state.accumulated;
            state.has_value = true;
        }
        else
        {
            /// Append all polygons from rhs into our accumulator.
            state.accumulated.insert(state.accumulated.end(), rhs_state.accumulated.begin(), rhs_state.accumulated.end());
        }

        /// Prune after merge if the combined state is large.
        state.add_count += rhs_state.add_count;
        if (state.add_count >= prune_interval)
            pruneAccumulated(state);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        /// Prune before serializing to minimise the serialized state size.
        /// This is semantically a no-op (the logical value is unchanged),
        /// so the const_cast is safe.
        auto & state = const_cast<Data &>(this->data(place));

        writeBinaryLittleEndian(state.has_value, buf);
        if (!state.has_value)
            return;

        // prune the accumulated state while serializing, regardless of add_count
        pruneAccumulated(state);

        std::stringstream wkt_stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        wkt_stream.exceptions(std::ios::failbit);
        wkt_stream << std::setprecision(std::numeric_limits<double>::max_digits10) << boost::geometry::wkt(state.accumulated);
        std::string wkt_str = wkt_stream.str();

        writeVarUInt(wkt_str.size(), buf);
        buf.write(wkt_str.data(), wkt_str.size());
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        auto & state = this->data(place);

        readBinaryLittleEndian(state.has_value, buf);
        if (!state.has_value)
            return;

        size_t wkt_size;
        readVarUInt(wkt_size, buf);

        std::string wkt_str(wkt_size, '\0');
        buf.readStrict(wkt_str.data(), wkt_size);

        boost::geometry::read_wkt(wkt_str, state.accumulated);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & state = this->data(place);

        auto & column_array = assert_cast<ColumnArray &>(to);

        if (!state.has_value)
        {
            /// Insert an empty ring.
            column_array.getOffsets().push_back(column_array.getOffsets().back());
            return;
        }

        /// Compute the convex hull of the accumulated multi-polygon.
        Polygon<Point> hull_polygon;
        boost::geometry::convex_hull(state.accumulated, hull_polygon);

        /// The result is the outer ring of the convex hull polygon.
        const Ring<Point> & hull_ring = hull_polygon.outer();

        RingSerializer<Point> serializer;
        serializer.add(hull_ring);
        auto result_column = serializer.finalize();

        column_array.insertFrom(*result_column, 0);
    }
};

}
