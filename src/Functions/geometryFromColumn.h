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
        const auto & tuple_columns = static_cast<const ColumnTuple &>(col).getColumns();

        if (tuple_columns.size() != 2)
        {
            throw Exception("tuple size must be equal to 2", ErrorCodes::ILLEGAL_COLUMN);
        }

        x = static_cast<const ColumnFloat64 &>(*tuple_columns[0]).getData().data();
        if (!x)
        {
            throw Exception("failed to get x column", ErrorCodes::ILLEGAL_COLUMN);
        }

        y = static_cast<const ColumnFloat64 &>(*tuple_columns[1]).getData().data();
        if (!y)
        {
            throw Exception("failed to get y column", ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    Float64Point createContainer() const
    {
        return Float64Point();
    }

    void get(Float64Point & container, size_t i) const
    {
        boost::geometry::set<0>(container, x[i]);
        boost::geometry::set<0>(container, y[i]);
    }

private:
    const Float64 * x;
    const Float64 * y;
};

template<class RingType, class PointParser>
class RingFromColumnParser
{
public:
    RingFromColumnParser(const IColumn & col)
        : offsets(static_cast<const ColumnArray &>(col).getOffsets())
        , pointParser(static_cast<const ColumnArray &>(col).getData())
    {
    }

    RingType createContainer() const
    {
        return RingType();
    }

    void get(RingType & container, size_t i) const
    {
        size_t l = offsets[i - 1];
        size_t r = offsets[i];

        container.resize(r - l);

        for (size_t j = l; j < r; j++) {
            pointParser.parse(container[j - l], l);
        }
    }

private:
    const IColumn::Offsets & offsets;
    PointParser pointParser;
};

using Float64RingFromColumnParser = RingFromColumnParser<Float64Ring, Float64PointFromColumnParser>;

using GeometryFromColumnParser = boost::variant<Float64PointFromColumnParser, Float64RingFromColumnParser>;

GeometryFromColumnParser makeGeometryFromColumnParser(const ColumnWithTypeAndName & col);

}
