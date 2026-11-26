#pragma once

#include <AggregateFunctions/IAggregateFunction.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Functions/geometry.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/io/wkt/wkt.hpp>

#include <iomanip>
#include <limits>
#include <optional>
#include <sstream>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_DATA;
}

template <typename Point>
struct AggregateFunctionGroupPolygonData
{
    MultiPolygon<Point> accumulated;
    bool has_value = false;
};

template <typename Point, typename Policy>
class AggregateFunctionGroupPolygonOp final
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupPolygonData<Point>, AggregateFunctionGroupPolygonOp<Point, Policy>>
{
private:
    using Data = AggregateFunctionGroupPolygonData<Point>;
    GeometryColumnType input_type;
    bool correct_geometry = true;

    /// The custom geo type name (e.g. `LineString`) takes precedence; only a type without a custom name
    /// (e.g. a computed `Array(Tuple(Float64, Float64))`) is resolved structurally, the same way as for the
    /// other geo functions. Named non-areal types (`Point`, `LineString`, `MultiLineString`, `MultiPoint`)
    /// share the storage layout with areal ones, but must not be reinterpreted as polygons.
    static GeometryColumnType resolveInputType(const DataTypePtr & type)
    {
        auto geometry_type = getGeometryColumnTypeFromDataType(type);
        if (geometry_type
            && (*geometry_type == GeometryColumnType::Ring || *geometry_type == GeometryColumnType::Polygon
                || *geometry_type == GeometryColumnType::MultiPolygon))
            return *geometry_type;

        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Unsupported geometry type for {}: {}. Expected Ring, Polygon, or MultiPolygon",
            Policy::name,
            type->getName());
    }

    static std::optional<MultiPolygon<Point>> extractMultiPolygon(const Field & field, GeometryColumnType type, bool should_correct)
    {
        MultiPolygon<Point> result;
        switch (type)
        {
            case GeometryColumnType::Ring: {
                auto ring = getRingFromField<Point>(field);
                if (ring.empty())
                    return std::nullopt;
                Polygon<Point> polygon;
                polygon.outer() = std::move(ring);
                if (should_correct)
                    boost::geometry::correct(polygon);
                result.emplace_back(std::move(polygon));
                break;
            }
            case GeometryColumnType::Polygon: {
                auto polygon = getPolygonFromField<Point>(field);
                if (polygon.outer().empty())
                    return std::nullopt;
                if (should_correct)
                    boost::geometry::correct(polygon);
                result.emplace_back(std::move(polygon));
                break;
            }
            case GeometryColumnType::MultiPolygon: {
                result = getMultiPolygonFromField<Point>(field);
                if (result.empty())
                    return std::nullopt;
                if (should_correct)
                    boost::geometry::correct(result);
                break;
            }
            default: UNREACHABLE();
        }
        return result;
    }

public:
    explicit AggregateFunctionGroupPolygonOp(const DataTypes & argument_types_, bool correct_geometry_ = true)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionGroupPolygonOp<Point, Policy>>(
              argument_types_, {}, DataTypeFactory::instance().get("MultiPolygon"))
        , input_type(resolveInputType(argument_types_.at(0)))
        , correct_geometry(correct_geometry_)
    {
    }

    String getName() const override { return Policy::name; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & state = this->data(place);

        if (state.has_value && Policy::canSkipAdd(state.accumulated))
            return;

        bool should_correct = correct_geometry;
        if (this->argument_types.size() == 2)
        {
            const auto & col = assert_cast<const ColumnUInt8 &>(*columns[1]);
            should_correct = col.getData()[row_num] != 0;
        }

        Field field;
        columns[0]->get(row_num, field);
        auto current = extractMultiPolygon(field, input_type, should_correct);
        if (!current)
            return;

        if (!state.has_value)
        {
            state.accumulated = std::move(*current);
            state.has_value = true;
        }
        else
        {
            MultiPolygon<Point> combined;
            Policy::combine(state.accumulated, *current, combined);
            state.accumulated = std::move(combined);
        }
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
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
            if (Policy::tryShortCircuitMerge(state.accumulated, rhs_state.accumulated))
                return;

            MultiPolygon<Point> combined;
            Policy::combine(state.accumulated, rhs_state.accumulated, combined);
            state.accumulated = std::move(combined);
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

        /// The same layout as `writeVarUInt` + raw bytes in `serialize`, but with a check of the size.
        String wkt_str;
        readStringBinary(wkt_str, buf);

        try
        {
            boost::geometry::read_wkt(wkt_str, state.accumulated);
        }
        catch (const boost::geometry::read_wkt_exception & e)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize the state of aggregate function {}: {}", getName(), e.what());
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & state = this->data(place);

        auto & column_array = assert_cast<ColumnArray &>(to);

        if (!state.has_value)
        {
            column_array.getOffsets().push_back(column_array.getOffsets().back());
            return;
        }

        MultiPolygonSerializer<Point> serializer;
        serializer.add(state.accumulated);
        auto result_column = serializer.finalize();

        column_array.insertFrom(*result_column, 0);
    }
};

}
