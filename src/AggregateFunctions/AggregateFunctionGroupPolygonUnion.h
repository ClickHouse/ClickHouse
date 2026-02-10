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
struct AggregateFunctionGroupPolygonUnionData
{
    MultiPolygon<Point> accumulated_union;
    bool has_value = false;
};

enum class InputGeometryType : uint8_t
{
    Ring,
    Polygon,
    MultiPolygon,
};

template <typename Point>
class AggregateFunctionGroupPolygonUnion final
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupPolygonUnionData<Point>, AggregateFunctionGroupPolygonUnion<Point>>
{
private:
    using Data = AggregateFunctionGroupPolygonUnionData<Point>;
    InputGeometryType input_type;

    static InputGeometryType resolveInputType(const DataTypePtr & type)
    {
        const auto & factory = DataTypeFactory::instance();
        if (factory.get("Ring")->equals(*type))
            return InputGeometryType::Ring;
        if (factory.get("Polygon")->equals(*type))
            return InputGeometryType::Polygon;
        if (factory.get("MultiPolygon")->equals(*type))
            return InputGeometryType::MultiPolygon;
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Unsupported geometry type for {}: {}. Expected Ring, Polygon, or MultiPolygon",
            "groupPolygonUnion", type->getName());
    }

public:
    explicit AggregateFunctionGroupPolygonUnion(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionGroupPolygonUnion<Point>>(
            argument_types_, {}, DataTypeFactory::instance().get("MultiPolygon"))
        , input_type(resolveInputType(argument_types_.at(0)))
    {}

    String getName() const override { return "groupPolygonUnion"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & state = this->data(place);

        /// Extract only the single row we need, avoiding conversion of the entire column.
        auto single_row_col = columns[0]->cut(row_num, 1);

        MultiPolygon<Point> current_multi_polygon;

        switch (input_type)
        {
            case InputGeometryType::Ring:
            {
                auto rings = ColumnToRingsConverter<Point>::convert(single_row_col);
                if (rings.empty())
                    return;
                Polygon<Point> polygon;
                polygon.outer() = std::move(rings[0]);
                boost::geometry::correct(polygon);
                current_multi_polygon.emplace_back(std::move(polygon));
                break;
            }
            case InputGeometryType::Polygon:
            {
                auto polygons = ColumnToPolygonsConverter<Point>::convert(single_row_col);
                if (polygons.empty())
                    return;
                boost::geometry::correct(polygons[0]);
                current_multi_polygon.emplace_back(std::move(polygons[0]));
                break;
            }
            case InputGeometryType::MultiPolygon:
            {
                auto multi_polygons = ColumnToMultiPolygonsConverter<Point>::convert(single_row_col);
                if (multi_polygons.empty())
                    return;
                current_multi_polygon = std::move(multi_polygons[0]);
                boost::geometry::correct(current_multi_polygon);
                break;
            }
        }

        if (!state.has_value)
        {
            state.accumulated_union = std::move(current_multi_polygon);
            state.has_value = true;
        }
        else
        {
            MultiPolygon<Point> new_union;
            boost::geometry::union_(state.accumulated_union, current_multi_polygon, new_union);
            state.accumulated_union = std::move(new_union);
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
            state.accumulated_union = rhs_state.accumulated_union;
            state.has_value = true;
        }
        else
        {
            MultiPolygon<Point> new_union;
            boost::geometry::union_(state.accumulated_union, rhs_state.accumulated_union, new_union);
            state.accumulated_union = std::move(new_union);
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
        wkt_stream << boost::geometry::wkt(state.accumulated_union);
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

        boost::geometry::read_wkt(wkt_str, state.accumulated_union);
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
        serializer.add(state.accumulated_union);
        auto result_column = serializer.finalize();

        // Copy from result to output
        column_array.insertFrom(*result_column, 0);
    }
};

}
