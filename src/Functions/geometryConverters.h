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

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace bg = boost::geometry;

template <typename Point>
using Ring = bg::model::ring<Point>;

template <typename Point>
using Polygon = bg::model::polygon<Point>;

template <typename Point>
using MultiPolygon = bg::model::multi_polygon<Polygon<Point>>;

template <typename Point>
using Geometry = boost::variant<Point, Ring<Point>, Polygon<Point>, MultiPolygon<Point>>;


using CartesianPoint = bg::model::d2::point_xy<Float64>;
using CartesianRing = Ring<CartesianPoint>;
using CartesianPolygon = Polygon<CartesianPoint>;
using CartesianMultiPolygon = MultiPolygon<CartesianPoint>;
using CartesianGeometry = Geometry<CartesianPoint>;

using GeographicPoint = bg::model::point<Float64, 2, bg::cs::geographic<bg::degree>>;
using GeographicRing = Ring<GeographicPoint>;
using GeographicPolygon = Polygon<GeographicPoint>;
using GeographicMultiPolygon = MultiPolygon<GeographicPoint>;
using GeographicGeometry = Geometry<GeographicPoint>;

/**
 * Class which takes some boost type and returns a pair of numbers.
 * They are (x,y) in case of cartesian coordinated and (lon,lat) in case of geographic.
*/
template <typename PointType>
class PointFromColumnParser
{
public:
    PointFromColumnParser(ColumnPtr col_) : col(col_)
    {
        const auto & tuple = dynamic_cast<const ColumnTuple &>(*col_);
        const auto & tuple_columns = tuple.getColumns();

#ifndef NDEBUG
        size = tuple.size();
#endif
        const auto & x_data = dynamic_cast<const ColumnFloat64 &>(*tuple_columns[0]);
        first = x_data.getData().data();

        const auto & y_data = dynamic_cast<const ColumnFloat64 &>(*tuple_columns[1]);
        second = y_data.getData().data();
    }

    template<class Q = PointType>
    typename std::enable_if_t<std::is_same_v<Q, CartesianPoint>, CartesianGeometry> createContainer() const
    {
        return CartesianPoint();
    }

    template<class Q = PointType>
    typename std::enable_if_t<std::is_same_v<Q, GeographicPoint>, GeographicGeometry> createContainer() const
    {
        return GeographicPoint();
    }

    template<class Q = PointType>
    void get(std::enable_if_t<std::is_same_v<Q, CartesianPoint>, CartesianGeometry> & container, size_t i) const
    {
#ifndef NDEBUG
        assert(i < size);
#endif
        get(boost::get<PointType>(container), i);
    }

    template<class Q = PointType>
    void get(std::enable_if_t<std::is_same_v<Q, GeographicPoint>, GeographicGeometry> & container, size_t i) const
    {
#ifndef NDEBUG
        assert(i < size);
#endif
        get(boost::get<PointType>(container), i);
    }

    void get(PointType & container, size_t i) const
    {
#ifndef NDEBUG
        assert(i < size);
#endif
        boost::geometry::set<0>(container, first[i]);
        boost::geometry::set<1>(container, second[i]);
    }

private:
    /// To prevent use-after-free and increase column lifetime.
    ColumnPtr col;
#ifndef NDEBUG
    size_t size;
#endif
    const Float64 * first;
    const Float64 * second;
};

template<class Point>
class RingFromColumnParser
{
public:
    RingFromColumnParser(ColumnPtr col_)
        : col(col_)
        , offsets(dynamic_cast<const ColumnArray &>(*col_).getOffsets())
        , point_parser(dynamic_cast<const ColumnArray &>(*col_).getDataPtr())
    {
    }

    Geometry<Point> createContainer() const
    {
        return Ring<Point>();
    }

    void get(Geometry<Point> & container, size_t i) const
    {
        get(boost::get<Ring<Point>>(container), i);
    }

    void get(Ring<Point> & container, size_t i) const
    {
        size_t left = i == 0 ? 0 : offsets[i - 1];
        size_t right = offsets[i];

        if (left == right)
            throw Exception("Empty polygons are not allowed in line " + toString(i), ErrorCodes::BAD_ARGUMENTS);

        // reserve extra point for case when polygon is open
        container.reserve(right - left + 1);
        container.resize(right - left);

        for (size_t j = left; j < right; j++)
            point_parser.get(container[j - left], j);

        // make ring closed
        if (!boost::geometry::equals(container[0], container.back()))
        {
            container.push_back(container[0]);
        }
    }

private:
    /// To prevent use-after-free and increase column lifetime.
    ColumnPtr col;
    const IColumn::Offsets & offsets;
    const PointFromColumnParser<Point> point_parser;
};

template<class Point>
class PolygonFromColumnParser
{
public:
    PolygonFromColumnParser(ColumnPtr col_)
        : col(col_)
        , offsets(static_cast<const ColumnArray &>(*col_).getOffsets())
        , ring_parser(static_cast<const ColumnArray &>(*col_).getDataPtr())
    {}

    Geometry<Point> createContainer() const
    {
        return Polygon<Point>();
    }

    void get(Geometry<Point> & container, size_t i) const
    {
        get(boost::get<Polygon<Point>>(container), i);
    }

    void get(Polygon<Point> & container, size_t i) const
    {
        size_t l = offsets[i - 1];
        size_t r = offsets[i];

        ring_parser.get(container.outer(), l);

        container.inners().resize(r - l - 1);
        for (size_t j = l + 1; j < r; j++)
        {
            ring_parser.get(container.inners()[j - l - 1], j);
        }
    }

private:
    /// To prevent use-after-free and increase column lifetime.
    ColumnPtr col;
    const IColumn::Offsets & offsets;
    const RingFromColumnParser<Point> ring_parser;
};

template<class Point>
class MultiPolygonFromColumnParser
{
public:
    MultiPolygonFromColumnParser(ColumnPtr col_)
        : col(col_)
        , offsets(static_cast<const ColumnArray &>(*col_).getOffsets())
        , polygon_parser(static_cast<const ColumnArray &>(*col_).getDataPtr())
    {}

    Geometry<Point> createContainer() const
    {
        return MultiPolygon<Point>();
    }

    void get(Geometry<Point> & container, size_t i) const
    {
        auto & multi_polygon = boost::get<MultiPolygon<Point>>(container);
        size_t l = offsets[i - 1];
        size_t r = offsets[i];

        multi_polygon.resize(r - l);
        for (size_t j = l; j < r; j++)
        {
            polygon_parser.get(multi_polygon[j - l], j);
        }
    }

private:
    /// To prevent use-after-free and increase column lifetime.
    ColumnPtr col;
    const IColumn::Offsets & offsets;
    const PolygonFromColumnParser<Point> polygon_parser;
};

template <typename Point>
using GeometryFromColumnParser = boost::variant<
    PointFromColumnParser<Point>,
    RingFromColumnParser<Point>,
    PolygonFromColumnParser<Point>,
    MultiPolygonFromColumnParser<Point>
>;

template <typename Point>
Geometry<Point> createContainer(const GeometryFromColumnParser<Point> & parser);

extern template Geometry<CartesianPoint> createContainer(const GeometryFromColumnParser<CartesianPoint> & parser);
extern template Geometry<GeographicPoint> createContainer(const GeometryFromColumnParser<GeographicPoint> & parser);

template <typename Point>
void get(const GeometryFromColumnParser<Point> & parser, Geometry<Point> & container, size_t i);

extern template void get(const GeometryFromColumnParser<CartesianPoint> & parser, Geometry<CartesianPoint> & container, size_t i);
extern template void get(const GeometryFromColumnParser<GeographicPoint> & parser, Geometry<GeographicPoint> & container, size_t i);

template <typename Point>
GeometryFromColumnParser<Point> makeGeometryFromColumnParser(const ColumnWithTypeAndName & col);

extern template GeometryFromColumnParser<CartesianPoint> makeGeometryFromColumnParser(const ColumnWithTypeAndName & col);
extern template GeometryFromColumnParser<GeographicPoint> makeGeometryFromColumnParser(const ColumnWithTypeAndName & col);

/// To serialize Geographic or Cartesian point (a pair of numbers in both cases).
template <typename Point>
class PointSerializerVisitor : public boost::static_visitor<void>
{
public:
    PointSerializerVisitor()
        : first(ColumnFloat64::create())
        , second(ColumnFloat64::create())
    {}

    PointSerializerVisitor(size_t n)
        : first(ColumnFloat64::create(n))
        , second(ColumnFloat64::create(n))
    {}

    void operator()(const Point & point)
    {
        first->insertValue(point.template get<0>());
        second->insertValue(point.template get<1>());
    }

    void operator()(const Ring<Point> & ring)
    {
        if (ring.size() != 1)
            throw Exception("Unable to write ring of size " + toString(ring.size()) + " != 1 to point column", ErrorCodes::BAD_ARGUMENTS);

        (*this)(ring[0]);
    }

    void operator()(const Polygon<Point> & polygon)
    {
        if (polygon.inners().size() != 0)
            throw Exception("Unable to write polygon with holes to point column", ErrorCodes::BAD_ARGUMENTS);

        (*this)(polygon.outer());
    }

    void operator()(const MultiPolygon<Point> & multi_polygon)
    {
        if (multi_polygon.size() != 1)
            throw Exception("Unable to write multi-polygon of size " + toString(multi_polygon.size()) + " != 1 to point column", ErrorCodes::BAD_ARGUMENTS);

        (*this)(multi_polygon[0]);
    }

    ColumnPtr finalize()
    {
        Columns columns(2);
        columns[0] = std::move(first);
        columns[1] = std::move(second);

        return ColumnTuple::create(columns);
    }

private:
    ColumnFloat64::MutablePtr first;
    ColumnFloat64::MutablePtr second;
};

template <typename Point>
class RingSerializerVisitor : public boost::static_visitor<void>
{
public:
    RingSerializerVisitor()
        : offsets(ColumnUInt64::create())
    {}

    RingSerializerVisitor(size_t n)
        : offsets(ColumnUInt64::create(n))
    {}

    void operator()(const Point & point)
    {
        size++;
        offsets->insertValue(size);

        pointSerializer(point);
    }

    void operator()(const Ring<Point> & ring)
    {
        size += ring.size();
        offsets->insertValue(size);
        for (const auto & point : ring)
        {
            pointSerializer(point);
        }
    }

    void operator()(const Polygon<Point> & polygon)
    {
        if (polygon.inners().size() != 0)
            throw Exception("Unable to write polygon with holes to ring column", ErrorCodes::BAD_ARGUMENTS);

        (*this)(polygon.outer());
    }

    void operator()(const MultiPolygon<Point> & multi_polygon)
    {
        if (multi_polygon.size() != 1)
            throw Exception("Unable to write multi-polygon of size " + toString(multi_polygon.size()) + " != 1 to ring column", ErrorCodes::BAD_ARGUMENTS);

        (*this)(multi_polygon[0]);
    }

    ColumnPtr finalize()
    {
        return ColumnArray::create(pointSerializer.finalize(), std::move(offsets));
    }

private:
    size_t size = 0;
    PointSerializerVisitor<Point> pointSerializer;
    ColumnUInt64::MutablePtr offsets;
};

template <typename Point>
class PolygonSerializerVisitor : public boost::static_visitor<void>
{
public:
    PolygonSerializerVisitor()
        : offsets(ColumnUInt64::create())
    {}

    PolygonSerializerVisitor(size_t n)
        : offsets(ColumnUInt64::create(n))
    {}

    void operator()(const Point & point)
    {
        size++;
        offsets->insertValue(size);
        ringSerializer(point);
    }

    void operator()(const Ring<Point> & ring)
    {
        size++;
        offsets->insertValue(size);
        ringSerializer(ring);
    }

    void operator()(const Polygon<Point> & polygon)
    {
        size += 1 + polygon.inners().size();
        offsets->insertValue(size);
        ringSerializer(polygon.outer());
        for (const auto & ring : polygon.inners())
        {
            ringSerializer(ring);
        }
    }

    void operator()(const MultiPolygon<Point> & multi_polygon)
    {
        if (multi_polygon.size() != 1)
            throw Exception("Unable to write multi-polygon of size " + toString(multi_polygon.size()) + " != 1 to polygon column", ErrorCodes::BAD_ARGUMENTS);

        (*this)(multi_polygon[0]);
    }

    ColumnPtr finalize()
    {
        return ColumnArray::create(ringSerializer.finalize(), std::move(offsets));
    }

private:
    size_t size = 0;
    RingSerializerVisitor<Point> ringSerializer;
    ColumnUInt64::MutablePtr offsets;
};

template <typename Point>
class MultiPolygonSerializerVisitor : public boost::static_visitor<void>
{
public:
    MultiPolygonSerializerVisitor()
        : offsets(ColumnUInt64::create())
    {}

    MultiPolygonSerializerVisitor(size_t n)
        : offsets(ColumnUInt64::create(n))
    {}

    void operator()(const Point & point)
    {
        size++;
        offsets->insertValue(size);
        polygonSerializer(point);
    }

    void operator()(const Ring<Point> & ring)
    {
        size++;
        offsets->insertValue(size);
        polygonSerializer(ring);
    }

    void operator()(const Polygon<Point> & polygon)
    {
        size++;
        offsets->insertValue(size);
        polygonSerializer(polygon);
    }

    void operator()(const MultiPolygon<Point> & multi_polygon)
    {
        size += multi_polygon.size();
        offsets->insertValue(size);
        for (const auto & polygon : multi_polygon)
        {
            polygonSerializer(polygon);
        }
    }

    ColumnPtr finalize()
    {
        return ColumnArray::create(polygonSerializer.finalize(), std::move(offsets));
    }

private:
    size_t size = 0;
    PolygonSerializerVisitor<Point> polygonSerializer;
    ColumnUInt64::MutablePtr offsets;
};

template <class Geometry, class Visitor>
class GeometrySerializer
{
public:
    void add(const Geometry & geometry)
    {
        boost::apply_visitor(visitor, geometry);
    }

    ColumnPtr finalize()
    {
        return visitor.finalize();
    }
private:
    Visitor visitor;
};

template <typename Point>
using PointSerializer = GeometrySerializer<Geometry<Point>, PointSerializerVisitor<Point>>;

template <typename Point>
using RingSerializer = GeometrySerializer<Geometry<Point>, RingSerializerVisitor<Point>>;

template <typename Point>
using PolygonSerializer = GeometrySerializer<Geometry<Point>, PolygonSerializerVisitor<Point>>;

template <typename Point>
using MultiPolygonSerializer = GeometrySerializer<Geometry<Point>, MultiPolygonSerializerVisitor<Point>>;

}
