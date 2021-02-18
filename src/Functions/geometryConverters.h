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

// TODO: maybe use isInfinite from clickhouse codebase
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

/**
 * Class which takes some boost type and returns a pair of numbers.
 * They are (x,y) in case of cartesian coordinated and (lon,lat) in case of geographic.
*/
template <typename PointType>
class PointFromColumnParser
{
public:
    using Container = std::conditional_t<std::is_same_v<PointType, CartesianPoint>, CartesianGeometry, GeographicGeometry>;

    explicit PointFromColumnParser(ColumnPtr col_) : col(col_)
    {
    }

    std::vector<PointType> parse(size_t shift, size_t count) const
    {
        const auto * tuple = typeid_cast<const ColumnTuple *>(col.get());
        const auto & tuple_columns = tuple->getColumns();

        const auto * x_data = typeid_cast<const ColumnFloat64 *>(tuple_columns[0].get());
        const auto * y_data = typeid_cast<const ColumnFloat64 *>(tuple_columns[1].get());

        const auto * first_container = x_data->getData().data() + shift;
        const auto * second_container = y_data->getData().data() + shift;

        std::vector<PointType> answer(count);

        for (size_t i = 0; i < count; ++i)
        {
            const Float64 first = first_container[i];
            const Float64 second = second_container[i];

            if (isNaN(first) || isNaN(second))
                throw Exception("Point's component must not be NaN", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            if (isinf(first) || isinf(second))
                throw Exception("Point's component must not be infinite", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            answer[i] = PointType(first, second);
        }

        return answer;
    }

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

    std::vector<Ring<Point>> parse(size_t shift, size_t /*size*/) const
    {
        size_t prev_offset = shift;

        std::vector<Ring<Point>> answer;
        answer.reserve(offsets.size());

        for (size_t offset : offsets)
        {
            offset += shift;
            auto points = point_parser.parse(prev_offset, offset - prev_offset);
            answer.emplace_back(points.begin(), points.end());
            prev_offset = offset;
        }

        return answer;
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
    explicit PolygonFromColumnParser(ColumnPtr col_)
        : col(col_)
        , offsets(typeid_cast<const ColumnArray &>(*col_).getOffsets())
        , ring_parser(typeid_cast<const ColumnArray &>(*col_).getDataPtr())
    {}

    std::vector<Polygon<Point>> parse(size_t shift, size_t /*size*/) const
    {
        size_t prev_offset = shift;
        std::vector<Polygon<Point>> answer(offsets.size());
        size_t iter = 0;

        for (size_t offset : offsets)
        {
            offset += shift;
            auto tmp = ring_parser.parse(prev_offset, offset - prev_offset);

            /// FIXME: other rings are holes in first
            answer[iter].outer() = tmp[0];
            prev_offset = offset;
            ++iter;
        }

        return answer;
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
    explicit MultiPolygonFromColumnParser(ColumnPtr col_)
        : col(col_)
        , offsets(typeid_cast<const ColumnArray &>(*col_).getOffsets())
        , polygon_parser(typeid_cast<const ColumnArray &>(*col_).getDataPtr())
    {}

    std::vector<MultiPolygon<Point>> parse(size_t shift, size_t /*size*/) const
    {
        size_t prev_offset = shift;
        
        std::vector<MultiPolygon<Point>> answer;
        answer.resize(offsets.size());

        size_t iter = 0;

        for (size_t offset : offsets)
        {
            offset += shift;
            auto polygons = polygon_parser.parse(prev_offset, offset - prev_offset);
            answer[iter].swap(polygons); 
            prev_offset = offset;
            ++iter;
        }

        return answer;
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

// template <typename Point>
// Geometry<Point> createContainer(const GeometryFromColumnParser<Point> & parser);

// template <typename Point>
// void get(const GeometryFromColumnParser<Point> & parser, Geometry<Point> & container, size_t i);

// template <typename Point>
// GeometryFromColumnParser<Point> makeGeometryFromColumnParser(const ColumnWithTypeAndName & col);


// extern template Geometry<CartesianPoint> createContainer(const GeometryFromColumnParser<CartesianPoint> & parser);
// extern template Geometry<GeographicPoint> createContainer(const GeometryFromColumnParser<GeographicPoint> & parser);
// extern template void get(const GeometryFromColumnParser<CartesianPoint> & parser, Geometry<CartesianPoint> & container, size_t i);
// extern template void get(const GeometryFromColumnParser<GeographicPoint> & parser, Geometry<GeographicPoint> & container, size_t i);
// extern template GeometryFromColumnParser<CartesianPoint> makeGeometryFromColumnParser(const ColumnWithTypeAndName & col);
// extern template GeometryFromColumnParser<GeographicPoint> makeGeometryFromColumnParser(const ColumnWithTypeAndName & col);

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

}
