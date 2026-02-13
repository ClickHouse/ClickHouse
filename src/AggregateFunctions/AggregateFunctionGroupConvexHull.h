#pragma once

#include <AggregateFunctions/IAggregateFunction.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFactory.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/io/wkt/wkt.hpp>

#include <sstream>

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
};

enum class ConvexHullInputType : uint8_t
{
    Point,
    Ring,
    Polygon,
    MultiPolygon,
    LineString,
    MultiLineString,
};

template <typename Point>
class AggregateFunctionGroupConvexHull final
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupConvexHullData<Point>, AggregateFunctionGroupConvexHull<Point>>
{
private:
    using Data = AggregateFunctionGroupConvexHullData<Point>;
    ConvexHullInputType input_type;
    bool correct_geometry = true;

    static ConvexHullInputType resolveInputType(const DataTypePtr & type)
    {
        const auto & factory = DataTypeFactory::instance();

        if (factory.get("Point")->equals(*type))
            return ConvexHullInputType::Point;

        /// LineString and Ring share the same underlying structure Array(Tuple(Float64, Float64)).
        /// Disambiguate by checking the custom type name.
        if (factory.get("LineString")->equals(*type) && type->getCustomName() && type->getCustomName()->getName() == "LineString")
            return ConvexHullInputType::LineString;

        /// MultiLineString and Polygon share the same underlying structure Array(Array(Tuple(Float64, Float64))).
        /// Disambiguate by checking the custom type name.
        if (factory.get("MultiLineString")->equals(*type) && type->getCustomName() && type->getCustomName()->getName() == "MultiLineString")
            return ConvexHullInputType::MultiLineString;

        if (factory.get("Ring")->equals(*type))
            return ConvexHullInputType::Ring;

        if (factory.get("Polygon")->equals(*type))
            return ConvexHullInputType::Polygon;

        if (factory.get("MultiPolygon")->equals(*type))
            return ConvexHullInputType::MultiPolygon;

        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Unsupported geometry type for {}: {}. Expected Point, Ring, Polygon, MultiPolygon, LineString, or MultiLineString",
            "groupConvexHull", type->getName());
    }

public:
    explicit AggregateFunctionGroupConvexHull(const DataTypes & argument_types_, bool correct_geometry_ = true)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionGroupConvexHull<Point>>(
            argument_types_, {}, DataTypeFactory::instance().get("Ring"))
        , input_type(resolveInputType(argument_types_.at(0)))
        , correct_geometry(correct_geometry_)
    {}

    String getName() const override { return "groupConvexHull"; }

    bool allocatesMemoryInArena() const override { return false; }

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

        /// Extract only the single row we need.
        auto single_row_col = columns[0]->cut(row_num, 1);

        switch (input_type)
        {
            case ConvexHullInputType::Point:
            {
                auto points = ColumnToPointsConverter<Point>::convert(single_row_col);
                if (points.empty())
                    return;
                /// A point becomes a degenerate polygon with a single-vertex outer ring.
                Polygon<Point> polygon;
                polygon.outer().push_back(std::move(points[0]));
                if (should_correct)
                    boost::geometry::correct(polygon);
                state.accumulated.emplace_back(std::move(polygon));
                state.has_value = true;
                break;
            }
            case ConvexHullInputType::Ring:
            {
                auto rings = ColumnToRingsConverter<Point>::convert(single_row_col);
                if (rings.empty())
                    return;
                /// Ring becomes a polygon's outer ring.
                Polygon<Point> polygon;
                polygon.outer() = std::move(rings[0]);
                if (should_correct)
                    boost::geometry::correct(polygon);
                state.accumulated.emplace_back(std::move(polygon));
                state.has_value = true;
                break;
            }
            case ConvexHullInputType::Polygon:
            {
                auto polygons = ColumnToPolygonsConverter<Point>::convert(single_row_col);
                if (polygons.empty())
                    return;
                /// Only take the outer ring, discard holes.
                Polygon<Point> polygon;
                polygon.outer() = std::move(polygons[0].outer());
                if (should_correct)
                    boost::geometry::correct(polygon);
                state.accumulated.emplace_back(std::move(polygon));
                state.has_value = true;
                break;
            }
            case ConvexHullInputType::MultiPolygon:
            {
                auto multi_polygons = ColumnToMultiPolygonsConverter<Point>::convert(single_row_col);
                if (multi_polygons.empty())
                    return;
                /// Move each polygon directly; holes don't affect convex hull computation.
                for (auto & poly : multi_polygons[0])
                {
                    if (should_correct)
                        boost::geometry::correct(poly);
                    state.accumulated.emplace_back(std::move(poly));
                }
                state.has_value = true;
                break;
            }
            case ConvexHullInputType::LineString:
            {
                auto linestrings = ColumnToLineStringsConverter<Point>::convert(single_row_col);
                if (linestrings.empty())
                    return;
                /// Treat the linestring's points as a polygon's outer ring.
                Polygon<Point> polygon;
                polygon.outer().assign(linestrings[0].begin(), linestrings[0].end());
                if (should_correct)
                    boost::geometry::correct(polygon);
                state.accumulated.emplace_back(std::move(polygon));
                state.has_value = true;
                break;
            }
            case ConvexHullInputType::MultiLineString:
            {
                auto multi_linestrings = ColumnToMultiLineStringsConverter<Point>::convert(single_row_col);
                if (multi_linestrings.empty())
                    return;
                /// Each linestring becomes one polygon.
                for (auto & ls : multi_linestrings[0])
                {
                    Polygon<Point> polygon;
                    polygon.outer().assign(ls.begin(), ls.end());
                    if (should_correct)
                        boost::geometry::correct(polygon);
                    state.accumulated.emplace_back(std::move(polygon));
                }
                state.has_value = true;
                break;
            }
        }
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
            state.accumulated.insert(
                state.accumulated.end(),
                rhs_state.accumulated.begin(),
                rhs_state.accumulated.end());
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & state = this->data(place);

        writeBinaryLittleEndian(state.has_value, buf);
        if (!state.has_value)
            return;

        std::stringstream wkt_stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        wkt_stream.exceptions(std::ios::failbit);
        wkt_stream << boost::geometry::wkt(state.accumulated);
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
