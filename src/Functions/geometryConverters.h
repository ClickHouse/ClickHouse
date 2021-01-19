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
    extern const int BAD_ARGUMENTS;
}

namespace bg = boost::geometry;
using CartesianPoint = bg::model::d2::point_xy<Float64>;
using CartesianRing = bg::model::ring<CartesianPoint>;
using CartesianPolygon = bg::model::polygon<CartesianPoint>;
using CartesianMultiPolygon = bg::model::multi_polygon<CartesianPolygon>;
using CartesianGeometry = boost::variant<CartesianPoint, CartesianRing, CartesianPolygon, CartesianMultiPolygon>;

using GeographicPoint = bg::model::point<Float64, 2, bg::cs::geographic<bg::degree>>;
using GeographicRing = bg::model::ring<GeographicPoint>;
using GeographicPolygon = bg::model::polygon<GeographicPoint>;
using GeographicMultiPolygon = bg::model::multi_polygon<GeographicPolygon>;
using GeographicGeometry = boost::variant<GeographicPoint, GeographicRing, GeographicPolygon, GeographicMultiPolygon>;

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
        const auto & tuple = static_cast<const ColumnTuple &>(*col_);
        const auto & tuple_columns = tuple.getColumns();

#ifndef NDEBUG
        size = tuple.size();
#endif
        const auto & x_data = static_cast<const ColumnFloat64 &>(*tuple_columns[0]);
        first = x_data.getData().data();

        const auto & y_data = static_cast<const ColumnFloat64 &>(*tuple_columns[1]);
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
    /// Note, this is needed to prevent use-after-free.
    ColumnPtr col;
#ifndef NDEBUG
    size_t size;
#endif
    const Float64 * first;
    const Float64 * second;
};

template<class Geometry, class RingType, class PointParser>
class RingFromColumnParser
{
public:
    RingFromColumnParser(ColumnPtr col_)
        : offsets(static_cast<const ColumnArray &>(*col_).getOffsets())
        , pointParser(static_cast<const ColumnArray &>(*col_).getDataPtr())
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
    PolygonFromColumnParser(ColumnPtr col_)
        : offsets(static_cast<const ColumnArray &>(*col_).getOffsets())
        , ringParser(static_cast<const ColumnArray &>(*col_).getDataPtr())
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
    MultiPolygonFromColumnParser(ColumnPtr col_)
        : offsets(static_cast<const ColumnArray &>(*col_).getOffsets())
        , polygonParser(static_cast<const ColumnArray &>(*col_).getDataPtr())
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
            polygonParser.get(multi_polygon[j - l], j);
        }
    }

private:
    const IColumn::Offsets & offsets;
    const PolygonParser polygonParser;
};

/// Cartesian coordinates

using CartesianRingFromColumnParser = RingFromColumnParser<CartesianGeometry, CartesianRing, PointFromColumnParser<CartesianPoint>>;
using CartesianPolygonFromColumnParser = PolygonFromColumnParser<CartesianGeometry, CartesianPolygon, CartesianRingFromColumnParser>;
using CartesianMultiPolygonFromColumnParser = MultiPolygonFromColumnParser<CartesianGeometry, CartesianMultiPolygon, CartesianPolygonFromColumnParser>;

using CartesianGeometryFromColumnParser = boost::variant<
    PointFromColumnParser<CartesianPoint>,
    CartesianRingFromColumnParser,
    CartesianPolygonFromColumnParser,
    CartesianMultiPolygonFromColumnParser
>;

CartesianGeometry createContainer(const CartesianGeometryFromColumnParser & parser);

void get(const CartesianGeometryFromColumnParser & parser, CartesianGeometry & container, size_t i);

CartesianGeometryFromColumnParser makeCartesianGeometryFromColumnParser(const ColumnWithTypeAndName & col);

/// Geographic coordinates 

using GeographicRingFromColumnParser = RingFromColumnParser<GeographicGeometry, GeographicRing, PointFromColumnParser<GeographicPoint>>;
using GeographicPolygonFromColumnParser = PolygonFromColumnParser<GeographicGeometry, GeographicPolygon, GeographicRingFromColumnParser>;
using GeographicMultiPolygonFromColumnParser = MultiPolygonFromColumnParser<GeographicGeometry, GeographicMultiPolygon, GeographicPolygonFromColumnParser>;

using GeographicGeometryFromColumnParser = boost::variant<
    PointFromColumnParser<GeographicPoint>,
    GeographicRingFromColumnParser,
    GeographicPolygonFromColumnParser,
    GeographicMultiPolygonFromColumnParser
>;

GeographicGeometry createContainer(const GeographicGeometryFromColumnParser & parser);

void get(const GeographicGeometryFromColumnParser & parser, GeographicGeometry & container, size_t i);

GeographicGeometryFromColumnParser makeGeographicGeometryFromColumnParser(const ColumnWithTypeAndName & col);


class CartesianPointSerializerVisitor : public boost::static_visitor<void>
{
public:
    CartesianPointSerializerVisitor()
        : x(ColumnFloat64::create())
        , y(ColumnFloat64::create())
    {}

    CartesianPointSerializerVisitor(size_t n)
        : x(ColumnFloat64::create(n))
        , y(ColumnFloat64::create(n))
    {}

    void operator()(const CartesianPoint & point)
    {
        x->insertValue(point.get<0>());
        y->insertValue(point.get<1>());
    }

    void operator()(const CartesianRing & ring)
    {
        if (ring.size() != 1) {
            throw Exception("Unable to write ring of size " + toString(ring.size()) + " != 1 to point column", ErrorCodes::BAD_ARGUMENTS);
        }
        (*this)(ring[0]);
    }

    void operator()(const CartesianPolygon & polygon)
    {
        if (polygon.inners().size() != 0) {
            throw Exception("Unable to write polygon with holes to point column", ErrorCodes::BAD_ARGUMENTS);
        }
        (*this)(polygon.outer());
    }

    void operator()(const CartesianMultiPolygon & multi_polygon)
    {
        if (multi_polygon.size() != 1) {
            throw Exception("Unable to write multi-polygon of size " + toString(multi_polygon.size()) + " != 1 to point column", ErrorCodes::BAD_ARGUMENTS);
        }
        (*this)(multi_polygon[0]);
    }

    ColumnPtr finalize()
    {
        Columns columns(2);
        columns[0] = std::move(x);
        columns[1] = std::move(y);

        return ColumnTuple::create(columns);
    }

private:
    ColumnFloat64::MutablePtr x;
    ColumnFloat64::MutablePtr y;
};

class CartesianRingSerializerVisitor : public boost::static_visitor<void>
{
public:
    CartesianRingSerializerVisitor()
        : offsets(ColumnUInt64::create())
    {}

    CartesianRingSerializerVisitor(size_t n)
        : offsets(ColumnUInt64::create(n))
    {}

    void operator()(const CartesianPoint & point)
    {
        size++;
        offsets->insertValue(size);

        pointSerializer(point);
    }

    void operator()(const CartesianRing & ring)
    {
        size += ring.size();
        offsets->insertValue(size);
        for (const auto & point : ring)
        {
            pointSerializer(point);
        }
    }

    void operator()(const CartesianPolygon & polygon)
    {
        if (polygon.inners().size() != 0) {
            throw Exception("Unable to write polygon with holes to ring column", ErrorCodes::BAD_ARGUMENTS);
        }
        (*this)(polygon.outer());
    }

    void operator()(const CartesianMultiPolygon & multi_polygon)
    {
        if (multi_polygon.size() != 1) {
            throw Exception("Unable to write multi-polygon of size " + toString(multi_polygon.size()) + " != 1 to ring column", ErrorCodes::BAD_ARGUMENTS);
        }
        (*this)(multi_polygon[0]);
    }

    ColumnPtr finalize()
    {
        return ColumnArray::create(pointSerializer.finalize(), std::move(offsets));
    }

private:
    size_t size = 0;
    CartesianPointSerializerVisitor pointSerializer;
    ColumnUInt64::MutablePtr offsets;
};

class CartesianPolygonSerializerVisitor : public boost::static_visitor<void>
{
public:
    CartesianPolygonSerializerVisitor()
        : offsets(ColumnUInt64::create())
    {}

    CartesianPolygonSerializerVisitor(size_t n)
        : offsets(ColumnUInt64::create(n))
    {}

    void operator()(const CartesianPoint & point)
    {
        size++;
        offsets->insertValue(size);
        ringSerializer(point);
    }

    void operator()(const CartesianRing & ring)
    {
        size++;
        offsets->insertValue(size);
        ringSerializer(ring);
    }

    void operator()(const CartesianPolygon & polygon)
    {
        size += 1 + polygon.inners().size();
        offsets->insertValue(size);
        ringSerializer(polygon.outer());
        for (const auto & ring : polygon.inners())
        {
            ringSerializer(ring);
        }
    }

    void operator()(const CartesianMultiPolygon & multi_polygon)
    {
        if (multi_polygon.size() != 1) {
            throw Exception("Unable to write multi-polygon of size " + toString(multi_polygon.size()) + " != 1 to polygon column", ErrorCodes::BAD_ARGUMENTS);
        }
        (*this)(multi_polygon[0]);
    }

    ColumnPtr finalize()
    {
        return ColumnArray::create(ringSerializer.finalize(), std::move(offsets));
    }

private:
    size_t size = 0;
    CartesianRingSerializerVisitor ringSerializer;
    ColumnUInt64::MutablePtr offsets;
};

class CartesianMultiPolygonSerializerVisitor : public boost::static_visitor<void>
{
public:
    CartesianMultiPolygonSerializerVisitor()
        : offsets(ColumnUInt64::create())
    {}

    CartesianMultiPolygonSerializerVisitor(size_t n)
        : offsets(ColumnUInt64::create(n))
    {}

    void operator()(const CartesianPoint & point)
    {
        size++;
        offsets->insertValue(size);
        polygonSerializer(point);
    }

    void operator()(const CartesianRing & ring)
    {
        size++;
        offsets->insertValue(size);
        polygonSerializer(ring);
    }

    void operator()(const CartesianPolygon & polygon)
    {
        size++;
        offsets->insertValue(size);
        polygonSerializer(polygon);
    }

    void operator()(const CartesianMultiPolygon & multi_polygon)
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
    CartesianPolygonSerializerVisitor polygonSerializer;
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

using CartesianPointSerializer = GeometrySerializer<CartesianGeometry, CartesianPointSerializerVisitor>;
using CartesianRingSerializer = GeometrySerializer<CartesianGeometry, CartesianRingSerializerVisitor>;
using CartesianPolygonSerializer = GeometrySerializer<CartesianGeometry, CartesianPolygonSerializerVisitor>;
using CartesianMultiPolygonSerializer = GeometrySerializer<CartesianGeometry, CartesianMultiPolygonSerializerVisitor>;

}
