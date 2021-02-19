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
#include <Common/NaNUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/IDataType.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/castColumn.h>

#include <cmath>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename Point>
using Ring = boost::geometry::model::ring<Point>;

template <typename Point>
using Polygon = boost::geometry::model::polygon<Point>;

template <typename Point>
using MultiPolygon = boost::geometry::model::multi_polygon<Polygon<Point>>;

template <typename Point>
using Geometry = boost::variant<Point, Ring<Point>, Polygon<Point>, MultiPolygon<Point>>;

template <typename Point>
using Figure = boost::variant<Ring<Point>, Polygon<Point>, MultiPolygon<Point>>;


using CartesianPoint = boost::geometry::model::d2::point_xy<Float64>;
using CartesianRing = Ring<CartesianPoint>;
using CartesianPolygon = Polygon<CartesianPoint>;
using CartesianMultiPolygon = MultiPolygon<CartesianPoint>;
using CartesianGeometry = Geometry<CartesianPoint>;

using GeographicPoint = boost::geometry::model::point<Float64, 2, boost::geometry::cs::geographic<boost::geometry::degree>>;
using GeographicRing = Ring<GeographicPoint>;
using GeographicPolygon = Polygon<GeographicPoint>;
using GeographicMultiPolygon = MultiPolygon<GeographicPoint>;
using GeographicGeometry = Geometry<GeographicPoint>;


template<class Point>
class RingFromColumnParser;

template<class Point>
class PolygonFromColumnParser;

template<class Point>
class MultiPolygonFromColumnParser;

/**
 * Class which takes some boost type and returns a pair of numbers.
 * They are (x,y) in case of cartesian coordinated and (lon,lat) in case of geographic.
*/
template <typename Point>
class PointFromColumnParser
{
public:
    using Container = std::conditional_t<std::is_same_v<Point, CartesianPoint>, CartesianGeometry, GeographicGeometry>;

    explicit PointFromColumnParser(ColumnPtr col_) : col(col_)
    {
    }

    std::vector<Point> parse(size_t shift, size_t count) const;

private:
    /// To prevent use-after-free and increase column lifetime.
    ColumnPtr col;
};


template<class Point>
class RingFromColumnParser
{
public:
    explicit RingFromColumnParser(ColumnPtr col_)
        : col(col_)
        , offsets(typeid_cast<const ColumnArray &>(*col_).getOffsets())
        , point_parser(typeid_cast<const ColumnArray &>(*col_).getDataPtr())
    {
    }

    std::vector<Ring<Point>> parse(size_t /*shift*/, size_t /*size*/) const;

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
    explicit PolygonFromColumnParser(ColumnPtr col_)
        : col(col_)
        , offsets(typeid_cast<const ColumnArray &>(*col_).getOffsets())
        , ring_parser(typeid_cast<const ColumnArray &>(*col_).getDataPtr())
    {
    }

    std::vector<Polygon<Point>> parse(size_t /*shift*/, size_t /*size*/) const;

private:
    friend class MultiPolygonFromColumnParser<Point>;

    /// To prevent use-after-free and increase column lifetime.
    ColumnPtr col;
    const IColumn::Offsets & offsets;
    const RingFromColumnParser<Point> ring_parser;
};

template<class Point>
class MultiPolygonFromColumnParser
{
public:
    explicit MultiPolygonFromColumnParser(ColumnPtr col_)
        : col(col_)
        , offsets(typeid_cast<const ColumnArray &>(*col_).getOffsets())
        , polygon_parser(typeid_cast<const ColumnArray &>(*col_).getDataPtr())
    {}

    std::vector<MultiPolygon<Point>> parse(size_t /*shift*/, size_t /*size*/) const;

private:
    /// To prevent use-after-free and increase column lifetime.
    ColumnPtr col;
    const IColumn::Offsets & offsets;
    const PolygonFromColumnParser<Point> polygon_parser;
};

template <typename Point>
using GeometryFromColumnParser = boost::variant<
    RingFromColumnParser<Point>,
    PolygonFromColumnParser<Point>,
    MultiPolygonFromColumnParser<Point>
>;


template <typename Point>
std::vector<Figure<Point>> parseFigure(const GeometryFromColumnParser<Point> & parser);

extern template std::vector<Figure<CartesianPoint>> parseFigure(const GeometryFromColumnParser<CartesianPoint> &);
extern template std::vector<Figure<GeographicPoint>> parseFigure(const GeometryFromColumnParser<GeographicPoint> &);


/// To serialize Geographic or Cartesian point (a pair of numbers in both cases).
template <typename Point>
class PointSerializerVisitor : public boost::static_visitor<void>
{
public:
    PointSerializerVisitor()
        : first(ColumnFloat64::create())
        , second(ColumnFloat64::create())
    {}

    explicit PointSerializerVisitor(size_t n)
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

    explicit RingSerializerVisitor(size_t n)
        : offsets(ColumnUInt64::create(n))
    {}

    void operator()(const Point & point)
    {
        size++;
        offsets->insertValue(size);

        point_serializer(point);
    }

    void operator()(const Ring<Point> & ring)
    {
        size += ring.size();
        offsets->insertValue(size);
        for (const auto & point : ring)
        {
            point_serializer(point);
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
        return ColumnArray::create(point_serializer.finalize(), std::move(offsets));
    }

private:
    size_t size = 0;
    PointSerializerVisitor<Point> point_serializer;
    ColumnUInt64::MutablePtr offsets;
};

template <typename Point>
class PolygonSerializerVisitor : public boost::static_visitor<void>
{
public:
    PolygonSerializerVisitor()
        : offsets(ColumnUInt64::create())
    {}

    explicit PolygonSerializerVisitor(size_t n)
        : offsets(ColumnUInt64::create(n))
    {}

    void operator()(const Point & point)
    {
        size++;
        offsets->insertValue(size);
        ring_serializer(point);
    }

    void operator()(const Ring<Point> & ring)
    {
        size++;
        offsets->insertValue(size);
        ring_serializer(ring);
    }

    void operator()(const Polygon<Point> & polygon)
    {
        size += 1 + polygon.inners().size();
        offsets->insertValue(size);
        ring_serializer(polygon.outer());
        for (const auto & ring : polygon.inners())
        {
            ring_serializer(ring);
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
        return ColumnArray::create(ring_serializer.finalize(), std::move(offsets));
    }

private:
    size_t size = 0;
    RingSerializerVisitor<Point> ring_serializer;
    ColumnUInt64::MutablePtr offsets;
};

template <typename Point>
class MultiPolygonSerializerVisitor : public boost::static_visitor<void>
{
public:
    MultiPolygonSerializerVisitor()
        : offsets(ColumnUInt64::create())
    {}

    explicit MultiPolygonSerializerVisitor(size_t n)
        : offsets(ColumnUInt64::create(n))
    {}

    void operator()(const Point & point)
    {
        size++;
        offsets->insertValue(size);
        polygon_serializer(point);
    }

    void operator()(const Ring<Point> & ring)
    {
        size++;
        offsets->insertValue(size);
        polygon_serializer(ring);
    }

    void operator()(const Polygon<Point> & polygon)
    {
        size++;
        offsets->insertValue(size);
        polygon_serializer(polygon);
    }

    void operator()(const MultiPolygon<Point> & multi_polygon)
    {
        size += multi_polygon.size();
        offsets->insertValue(size);
        for (const auto & polygon : multi_polygon)
        {
            polygon_serializer(polygon);
        }
    }

    ColumnPtr finalize()
    {
        return ColumnArray::create(polygon_serializer.finalize(), std::move(offsets));
    }

private:
    size_t size = 0;
    PolygonSerializerVisitor<Point> polygon_serializer;
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


template <typename Point, template<typename> typename Desired>
void checkColumnTypeOrThrow(const ColumnWithTypeAndName & column);

template <typename Point>
GeometryFromColumnParser<Point> getConverterBasedOnType(const ColumnWithTypeAndName & column);

}
