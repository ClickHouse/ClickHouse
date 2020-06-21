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

using Float64Point = boost::geometry::model::d2::point_xy<Float64>;
using Float64Ring = boost::geometry::model::ring<Float64Point>;
using Float64Polygon = boost::geometry::model::polygon<Float64Point>;
using Float64MultiPolygon = boost::geometry::model::multi_polygon<Float64Polygon>;
using Float64Geometry = boost::variant<Float64Point, Float64Ring, Float64Polygon, Float64MultiPolygon>;

class Float64PointFromColumnParser
{
public:
    Float64PointFromColumnParser(ColumnPtr col_)
        : col(col_)
    {
        const auto & tuple = static_cast<const ColumnTuple &>(*col_);
        const auto & tuple_columns = tuple.getColumns();

        const auto & x_data = static_cast<const ColumnFloat64 &>(*tuple_columns[0]);
        x = x_data.getData().data();

        const auto & y_data = static_cast<const ColumnFloat64 &>(*tuple_columns[1]);
        y = y_data.getData().data();
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
    ColumnPtr col;

    const Float64 * x;
    const Float64 * y;
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

class Float64PointSerializerVisitor : public boost::static_visitor<void>
{
public:
    Float64PointSerializerVisitor()
        : x(ColumnFloat64::create())
        , y(ColumnFloat64::create())
    {}

    Float64PointSerializerVisitor(size_t n)
        : x(ColumnFloat64::create(n))
        , y(ColumnFloat64::create(n))
    {}

    void operator()(const Float64Point & point)
    {
        x->insertValue(point.x());
        y->insertValue(point.y());
    }

    void operator()(const Float64Ring & ring)
    {
        if (ring.size() != 1) {
            throw Exception("Unable to write ring of size " + toString(ring.size()) + " != 1 to point column", ErrorCodes::BAD_ARGUMENTS);
        }
        (*this)(ring[0]);
    }

    void operator()(const Float64Polygon & polygon)
    {
        if (polygon.inners().size() != 0) {
            throw Exception("Unable to write polygon with holes to point column", ErrorCodes::BAD_ARGUMENTS);
        }
        (*this)(polygon.outer());
    }

    void operator()(const Float64MultiPolygon & multi_polygon)
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

class Float64RingSerializerVisitor : public boost::static_visitor<void>
{
public:
    Float64RingSerializerVisitor()
        : offsets(ColumnUInt64::create())
    {}

    Float64RingSerializerVisitor(size_t n)
        : offsets(ColumnUInt64::create(n))
    {}

    void operator()(const Float64Point & point)
    {
        size++;
        offsets->insertValue(size);

        pointSerializer(point);
    }

    void operator()(const Float64Ring & ring)
    {
        size += ring.size();
        offsets->insertValue(size);
        for (const auto & point : ring)
        {
            pointSerializer(point);
        }
    }

    void operator()(const Float64Polygon & polygon)
    {
        if (polygon.inners().size() != 0) {
            throw Exception("Unable to write polygon with holes to ring column", ErrorCodes::BAD_ARGUMENTS);
        }
        (*this)(polygon.outer());
    }

    void operator()(const Float64MultiPolygon & multi_polygon)
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
    Float64PointSerializerVisitor pointSerializer;
    ColumnUInt64::MutablePtr offsets;
};

class Float64PolygonSerializerVisitor : public boost::static_visitor<void>
{
public:
    Float64PolygonSerializerVisitor()
        : offsets(ColumnUInt64::create())
    {}

    Float64PolygonSerializerVisitor(size_t n)
        : offsets(ColumnUInt64::create(n))
    {}

    void operator()(const Float64Point & point)
    {
        size++;
        offsets->insertValue(size);
        ringSerializer(point);
    }

    void operator()(const Float64Ring & ring)
    {
        size++;
        offsets->insertValue(size);
        ringSerializer(ring);
    }

    void operator()(const Float64Polygon & polygon)
    {
        size += 1 + polygon.inners().size();
        offsets->insertValue(size);
        ringSerializer(polygon.outer());
        for (const auto & ring : polygon.inners())
        {
            ringSerializer(ring);
        }
    }

    void operator()(const Float64MultiPolygon & multi_polygon)
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
    Float64RingSerializerVisitor ringSerializer;
    ColumnUInt64::MutablePtr offsets;
};

class Float64MultiPolygonSerializerVisitor : public boost::static_visitor<void>
{
public:
    Float64MultiPolygonSerializerVisitor()
        : offsets(ColumnUInt64::create())
    {}

    Float64MultiPolygonSerializerVisitor(size_t n)
        : offsets(ColumnUInt64::create(n))
    {}

    void operator()(const Float64Point & point)
    {
        size++;
        offsets->insertValue(size);
        polygonSerializer(point);
    }

    void operator()(const Float64Ring & ring)
    {
        size++;
        offsets->insertValue(size);
        polygonSerializer(ring);
    }

    void operator()(const Float64Polygon & polygon)
    {
        size++;
        offsets->insertValue(size);
        polygonSerializer(polygon);
    }

    void operator()(const Float64MultiPolygon & multi_polygon)
    {
        size += 1 + multi_polygon.size();
        offsets->insertValue(size);
        for (const auto & polygon : multi_polygon)
        {
            polygonSerializer(polygon);
        }
    }

    ColumnPtr finalize()
    {
        LOG_FATAL(&Poco::Logger::get("PI"), "MultiPolygon Offsets: " + toString(size));
        return ColumnArray::create(polygonSerializer.finalize(), std::move(offsets));
    }

private:
    size_t size = 0;
    Float64PolygonSerializerVisitor polygonSerializer;
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

using Float64PointSerializer = GeometrySerializer<Float64Geometry, Float64PointSerializerVisitor>;
using Float64RingSerializer = GeometrySerializer<Float64Geometry, Float64RingSerializerVisitor>;
using Float64PolygonSerializer = GeometrySerializer<Float64Geometry, Float64PolygonSerializerVisitor>;
using Float64MultiPolygonSerializer = GeometrySerializer<Float64Geometry, Float64MultiPolygonSerializerVisitor>;

}
