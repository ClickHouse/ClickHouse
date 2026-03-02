#pragma once

#include <AggregateFunctions/IAggregateFunction.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>

#include <Common/WKB.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Functions/geometry.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/io/wkt/wkt.hpp>

#include <sstream>
#include <magic_enum.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

template <typename Point>
struct AggregateFunctionGroupPolygonIntersectionData
{
    MultiPolygon<Point> accumulated_intersection;
    bool has_value = false;
};

template <typename Point>
class AggregateFunctionGroupPolygonIntersection final : public IAggregateFunctionDataHelper<
                                                            AggregateFunctionGroupPolygonIntersectionData<Point>,
                                                            AggregateFunctionGroupPolygonIntersection<Point>>
{
private:
    using Data = AggregateFunctionGroupPolygonIntersectionData<Point>;
    WKBGeometry input_type;
    bool correct_geometry = true;

    static constexpr WKBGeometry WKB_RING = static_cast<WKBGeometry>(100);
    static constexpr WKBGeometry WKB_GEOMETRY = static_cast<WKBGeometry>(101);

    static WKBGeometry resolveInputType(const DataTypePtr & type)
    {
        const auto & factory = DataTypeFactory::instance();
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
            "Unsupported geometry type for {}: {}. Expected Ring, Polygon, MultiPolygon, or Geometry",
            "groupPolygonIntersection",
            type->getName());
    }

public:
    explicit AggregateFunctionGroupPolygonIntersection(const DataTypes & argument_types_, bool correct_geometry_ = true)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionGroupPolygonIntersection<Point>>(
            argument_types_, {}, DataTypeFactory::instance().get("MultiPolygon"))
        , input_type(resolveInputType(argument_types_.at(0)))
        , correct_geometry(correct_geometry_)
    {
    }

    String getName() const override { return "groupPolygonIntersection"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & state = this->data(place);

        // Optimization: if we already have a value and it is empty, the intersection will always be empty.
        // So we can skip processing subsequent rows.
        if (state.has_value && state.accumulated_intersection.empty())
            return;

        /// If the second argument is provided and is 0, skip geometry correction for this row.
        bool should_correct = correct_geometry;
        if (this->argument_types.size() == 2)
        {
            const auto & col = assert_cast<const ColumnUInt8 &>(*columns[1]);
            should_correct = col.getData()[row_num] != 0;
        }

        /// Extract only the single row we need, avoiding conversion of the entire column.
        auto single_row_col = columns[0]->cut(row_num, 1);

        MultiPolygon<Point> current_multi_polygon;

        switch (input_type)
        {
            case WKB_RING: {
                auto rings = ColumnToRingsConverter<Point>::convert(single_row_col);
                if (rings.empty())
                    return;
                Polygon<Point> polygon;
                polygon.outer() = std::move(rings[0]);
                if (should_correct)
                    boost::geometry::correct(polygon);
                current_multi_polygon.emplace_back(std::move(polygon));
                break;
            }
            case WKBGeometry::Polygon: {
                auto polygons = ColumnToPolygonsConverter<Point>::convert(single_row_col);
                if (polygons.empty())
                    return;
                if (should_correct)
                    boost::geometry::correct(polygons[0]);
                current_multi_polygon.emplace_back(std::move(polygons[0]));
                break;
            }
            case WKBGeometry::MultiPolygon: {
                auto multi_polygons = ColumnToMultiPolygonsConverter<Point>::convert(single_row_col);
                if (multi_polygons.empty())
                    return;
                current_multi_polygon = std::move(multi_polygons[0]);
                if (should_correct)
                    boost::geometry::correct(current_multi_polygon);
                break;
            }
            case WKBGeometry::Point:
            case WKBGeometry::LineString:
            case WKBGeometry::MultiLineString:
                break;
            case WKB_GEOMETRY: {
                const auto & variant_col = assert_cast<const ColumnVariant &>(*columns[0]);

                auto local_discr = variant_col.localDiscriminatorAt(row_num);
                if (local_discr == ColumnVariant::NULL_DISCRIMINATOR)
                    return;

                Field field;
                variant_col.get(row_num, field);

                auto geo_type = magic_enum::enum_cast<GeometryColumnType>(static_cast<int>(variant_col.globalDiscriminatorAt(row_num)));
                if (!geo_type)
                    return;

                switch (*geo_type)
                {
                    case GeometryColumnType::Ring: {
                        auto ring = getRingFromField<Point>(field);
                        Polygon<Point> polygon;
                        polygon.outer() = std::move(ring);
                        if (should_correct)
                            boost::geometry::correct(polygon);
                        current_multi_polygon.emplace_back(std::move(polygon));
                        break;
                    }
                    case GeometryColumnType::Polygon: {
                        auto poly = getPolygonFromField<Point>(field);
                        if (should_correct)
                            boost::geometry::correct(poly);
                        current_multi_polygon.emplace_back(std::move(poly));
                        break;
                    }
                    case GeometryColumnType::MultiPolygon: {
                        auto mpoly = getMultiPolygonFromField<Point>(field);
                        current_multi_polygon = std::move(mpoly);
                        if (should_correct)
                            boost::geometry::correct(current_multi_polygon);
                        break;
                    }
                    case GeometryColumnType::Point:
                    case GeometryColumnType::Linestring:
                    case GeometryColumnType::MultiLinestring:
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Unsupported geometry variant for groupPolygonIntersection: Point, LineString, or MultiLineString. "
                            "Expected Ring, Polygon, or MultiPolygon within Geometry");
                    case GeometryColumnType::Null:
                        break;
                }
                break;
            }
        }

        if (!state.has_value)
        {
            state.accumulated_intersection = std::move(current_multi_polygon);
            state.has_value = true;
        }
        else
        {
            MultiPolygon<Point> new_intersection;
            boost::geometry::intersection(state.accumulated_intersection, current_multi_polygon, new_intersection);
            state.accumulated_intersection = std::move(new_intersection);
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
            state.accumulated_intersection = rhs_state.accumulated_intersection;
            state.has_value = true;
        }
        else
        {
            // Optimization: if either is empty, intersection is empty
            if (state.accumulated_intersection.empty())
                return;
            if (rhs_state.accumulated_intersection.empty())
            {
                state.accumulated_intersection.clear();
                return;
            }

            MultiPolygon<Point> new_intersection;
            boost::geometry::intersection(state.accumulated_intersection, rhs_state.accumulated_intersection, new_intersection);
            state.accumulated_intersection = std::move(new_intersection);
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
        wkt_stream << boost::geometry::wkt(state.accumulated_intersection);
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

        boost::geometry::read_wkt(wkt_str, state.accumulated_intersection);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & state = this->data(place);

        auto & column_array = assert_cast<ColumnArray &>(to);

        if (!state.has_value)
        {
            // Insert empty multi-polygon
            column_array.getOffsets().push_back(column_array.getOffsets().back());
            return;
        }

        // Use MultiPolygonSerializer to add the result
        MultiPolygonSerializer<Point> serializer;
        serializer.add(state.accumulated_intersection);
        auto result_column = serializer.finalize();

        // Copy from result to output
        column_array.insertFrom(*result_column, 0);
    }
};

}
