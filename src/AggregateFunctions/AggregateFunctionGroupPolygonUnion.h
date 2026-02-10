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

    /// Convert a column to a vector of MultiPolygon, regardless of input geometry type.
    std::vector<MultiPolygon<Point>> convertToMultiPolygons(ColumnPtr col) const
    {
        switch (input_type)
        {
            case InputGeometryType::Ring:
            {
                auto rings = ColumnToRingsConverter<Point>::convert(col);
                std::vector<MultiPolygon<Point>> result;
                result.reserve(rings.size());
                for (auto & ring : rings)
                {
                    Polygon<Point> polygon;
                    polygon.outer() = std::move(ring);
                    MultiPolygon<Point> mp;
                    mp.emplace_back(std::move(polygon));
                    result.emplace_back(std::move(mp));
                }
                return result;
            }
            case InputGeometryType::Polygon:
            {
                auto polygons = ColumnToPolygonsConverter<Point>::convert(col);
                std::vector<MultiPolygon<Point>> result;
                result.reserve(polygons.size());
                for (auto & polygon : polygons)
                {
                    MultiPolygon<Point> mp;
                    mp.emplace_back(std::move(polygon));
                    result.emplace_back(std::move(mp));
                }
                return result;
            }
            case InputGeometryType::MultiPolygon:
            {
                return ColumnToMultiPolygonsConverter<Point>::convert(col);
            }
        }
        UNREACHABLE();
    }

    /// Union a multi-polygon into the aggregate state.
    static void unionIntoState(Data & state, MultiPolygon<Point> && current)
    {
        if (!state.has_value)
        {
            state.accumulated_union = std::move(current);
            state.has_value = true;
        }
        else
        {
            MultiPolygon<Point> new_union;
            boost::geometry::union_(state.accumulated_union, current, new_union);
            state.accumulated_union = std::move(new_union);
        }
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
        auto multi_polygons = convertToMultiPolygons(columns[0]->getPtr());
        if (row_num >= multi_polygons.size())
            return;
        unionIntoState(state, std::move(multi_polygons[row_num]));
    }

    // Since we convert the entire column to the desired type, before doing any processing,
    // it is more efficient to use the same converted column for each row's union, instead of computing again each time.
    // This logic is implemented in addBatchSinglePlace, and add() is left as is for compatibility with non-vectorized execution.
    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        auto & state = this->data(place);
        auto multi_polygons = convertToMultiPolygons(columns[0]->getPtr());

        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (flags[i] && i < multi_polygons.size())
                    unionIntoState(state, std::move(multi_polygons[i]));
            }
        }
        else
        {
            for (size_t i = row_begin; i < row_end && i < multi_polygons.size(); ++i)
                unionIntoState(state, std::move(multi_polygons[i]));
        }

        (void)arena;
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
