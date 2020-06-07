#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <Core/Types.h>

#include <boost/variant.hpp>
#include <boost/geometry/geometries/geometries.hpp>
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/IDataType.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/castColumn.h>

#include <common/logger_useful.h>

namespace DB {

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

using Float64Point = boost::geometry::model::d2::point_xy<Float64>;
using Float64Ring = boost::geometry::model::ring<Float64Point>;
using Float64Polygon = boost::geometry::model::polygon<Float64Point>;
using Float64MultiPolygon = boost::geometry::model::multi_polygon<Float64Polygon>;
using Float64Geometry = boost::variant<Float64Point, Float64Ring, Float64Polygon, Float64MultiPolygon>;

class Float64PointFromColumnParser
{
public:
    Float64PointFromColumnParser(const IColumn & col)
    {
        const auto * tuple = checkAndGetColumn<ColumnTuple>(col);
        if (!tuple)
        {
            throw Exception("not tuple ", ErrorCodes::ILLEGAL_COLUMN);
        }
        const auto & tuple_columns = tuple->getColumns();

        if (tuple_columns.size() != 2)
        {
            throw Exception("tuple size is " + toString(tuple_columns.size()) + " != 2", ErrorCodes::ILLEGAL_COLUMN);
        }

        const auto * x_data = checkAndGetColumn<ColumnFloat64>(*tuple_columns[0]);
        if (!x_data)
        {
            throw Exception("not x ", ErrorCodes::ILLEGAL_COLUMN);
        }
        x = x_data->getData().data();
        if (!x)
        {
            throw Exception("failed to get x column", ErrorCodes::ILLEGAL_COLUMN);
        }

        const auto * y_data = checkAndGetColumn<ColumnFloat64>(*tuple_columns[1]);
        if (!y_data)
        {
            throw Exception("not y ", ErrorCodes::ILLEGAL_COLUMN);
        }
        y = y_data->getData().data();
        if (!y)
        {
            throw Exception("failed to get y column", ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    Float64Geometry createContainer() const
    {
        return Float64Point();
    }

    void get(Float64Geometry & container, size_t i) const
    {
        get(boost::get<Float64Point>(container), i);
    }

    void get(Float64Point & container, size_t i) const
    {
        boost::geometry::set<0>(container, x[i]);
        boost::geometry::set<1>(container, y[i]);
    }
private:
    const Float64 * x;
    const Float64 * y;
};

template<class Geometry, class RingType, class PointParser>
class RingFromColumnParser
{
public:
    RingFromColumnParser(const IColumn & col)
        : offsets(static_cast<const ColumnArray &>(col).getOffsets())
        , pointParser(static_cast<const ColumnArray &>(col).getData())
    {
    }

    Geometry createContainer() const
    {
        return RingType();
    }

    void get(Geometry & container, size_t i) const
    {
        get(boost::get<RingType>(container), i);
    }

    void get(RingType & container, size_t i) const
    {
        size_t l = offsets[i - 1];
        size_t r = offsets[i];

        // reserve extra point for case when polygon is open
        container.reserve(r - l + 1);
        container.resize(r - l);

        for (size_t j = l; j < r; j++) {
            // LOG_FATAL
            pointParser.get(container[j - l], j);
        }

        // make ring closed
        if (!boost::geometry::equals(container[0], container.back()))
        {
            container.push_back(container[0]);
        }
    }

private:
    const IColumn::Offsets & offsets;
    const PointParser pointParser;
};

template<class Geometry, class PolygonType, class RingParser>
class PolygonFromColumnParser
{
public:
    PolygonFromColumnParser(const IColumn & col)
        : offsets(static_cast<const ColumnArray &>(col).getOffsets())
        , ringParser(static_cast<const ColumnArray &>(col).getData())
    {}

    Geometry createContainer() const
    {
        return PolygonType();
    }

    void get(Geometry & container, size_t i) const
    {
        get(boost::get<PolygonType>(container), i);
    }

    void get(PolygonType & container, size_t i) const
    {
        size_t l = offsets[i - 1];
        size_t r = offsets[i];

        ringParser.get(container.outer(), l);

        container.inners().resize(r - l - 1);
        for (size_t j = l + 1; j < r; j++)
        {
            ringParser.get(container.inners()[j - l - 1], j);
        }
    }

private:
    const IColumn::Offsets & offsets;
    const RingParser ringParser;
};

template<class Geometry, class MultiPolygonType, class PolygonParser>
class MultiPolygonFromColumnParser
{
public:
    MultiPolygonFromColumnParser(const IColumn & col)
        : offsets(static_cast<const ColumnArray &>(col).getOffsets())
        , polygonParser(static_cast<const ColumnArray &>(col).getData())
    {}

    Geometry createContainer() const
    {
        return MultiPolygonType();
    }

    void get(Geometry & container, size_t i) const
    {
        MultiPolygonType & multi_polygon = boost::get<MultiPolygonType>(container);
        size_t l = offsets[i - 1];
        size_t r = offsets[i];

        multi_polygon.resize(r - l);
        for (size_t j = l; j < r; j++)
        {
            polygonParser.get(multi_polygon[j - l], j - l);
        }
    }

private:
    const IColumn::Offsets & offsets;
    const PolygonParser polygonParser;
};

using Float64RingFromColumnParser = RingFromColumnParser<Float64Geometry, Float64Ring, Float64PointFromColumnParser>;
using Float64PolygonFromColumnParser = PolygonFromColumnParser<Float64Geometry, Float64Polygon, Float64RingFromColumnParser>;
using Float64MultiPolygonFromColumnParser = MultiPolygonFromColumnParser<Float64Geometry, Float64MultiPolygon, Float64PolygonFromColumnParser>;

using GeometryFromColumnParser = boost::variant<
    Float64PointFromColumnParser,
    Float64RingFromColumnParser,
    Float64PolygonFromColumnParser,
    Float64MultiPolygonFromColumnParser
>;

Float64Geometry createContainer(const GeometryFromColumnParser & parser);

void get(const GeometryFromColumnParser & parser, Float64Geometry & container, size_t i);

GeometryFromColumnParser makeGeometryFromColumnParser(const ColumnWithTypeAndName & col);

}
