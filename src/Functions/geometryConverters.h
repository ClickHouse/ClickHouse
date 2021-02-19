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
#include <DataTypes/DataTypeCustomGeo.h>
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
    PointFromColumnParser() = default;

    explicit PointFromColumnParser(ColumnPtr col_) : col(col_)
    {
    }

    std::vector<Point> parse() const;

private:
    std::vector<Point> parseImpl(size_t shift, size_t count) const;

    friend class RingFromColumnParser<Point>;
    ColumnPtr col{nullptr};
};


template<class Point>
class RingFromColumnParser
{
public:
    RingFromColumnParser() = default;

    explicit RingFromColumnParser(ColumnPtr col_)
        : col(col_)
        , point_parser(typeid_cast<const ColumnArray &>(*col_).getDataPtr())
    {
    }

    std::vector<Ring<Point>> parse() const;

private:
    friend class PointFromColumnParser<Point>;
    /// To prevent use-after-free and increase column lifetime.
    ColumnPtr col{nullptr};
    const PointFromColumnParser<Point> point_parser{};
};

template<class Point>
class PolygonFromColumnParser
{
public:
    PolygonFromColumnParser() = default;

    explicit PolygonFromColumnParser(ColumnPtr col_)
        : col(col_)
        , ring_parser(typeid_cast<const ColumnArray &>(*col_).getDataPtr())
    {
    }

    std::vector<Polygon<Point>> parse() const;

private:
    friend class MultiPolygonFromColumnParser<Point>;

    /// To prevent use-after-free and increase column lifetime.
    ColumnPtr col{nullptr};
    const RingFromColumnParser<Point> ring_parser{};
};

template<class Point>
class MultiPolygonFromColumnParser
{
public:
    MultiPolygonFromColumnParser() = default;

    explicit MultiPolygonFromColumnParser(ColumnPtr col_)
        : col(col_)
        , polygon_parser(typeid_cast<const ColumnArray &>(*col_).getDataPtr())
    {}

    std::vector<MultiPolygon<Point>> parse() const;

private:
    /// To prevent use-after-free and increase column lifetime.
    ColumnPtr col{nullptr};
    const PolygonFromColumnParser<Point> polygon_parser{};
};


extern template class PointFromColumnParser<CartesianPoint>;
extern template class PointFromColumnParser<GeographicPoint>;
extern template class RingFromColumnParser<CartesianPoint>;
extern template class RingFromColumnParser<GeographicPoint>;
extern template class PolygonFromColumnParser<CartesianPoint>;
extern template class PolygonFromColumnParser<GeographicPoint>;
extern template class MultiPolygonFromColumnParser<CartesianPoint>;
extern template class MultiPolygonFromColumnParser<GeographicPoint>;


/// To serialize Geographic or Cartesian point (a pair of numbers in both cases).
template <typename Point>
class PointSerializer
{
public:
    PointSerializer()
        : first(ColumnFloat64::create())
        , second(ColumnFloat64::create())
        , first_container(first->getData())
        , second_container(second->getData())
    {}

    explicit PointSerializer(size_t n)
        : first(ColumnFloat64::create(n))
        , second(ColumnFloat64::create(n))
        , first_container(first->getData())
        , second_container(second->getData())
    {}

    void add(const Point & point)
    {
        first_container.emplace_back(point.template get<0>());
        second_container.emplace_back(point.template get<1>());
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

    ColumnFloat64::Container & first_container;
    ColumnFloat64::Container & second_container;
};

template <typename Point>
class RingSerializer
{
public:
    RingSerializer()
        : offsets(ColumnUInt64::create())
    {}

    explicit RingSerializer(size_t n)
        : offsets(ColumnUInt64::create(n))
    {}

    void add(const Ring<Point> & ring)
    {
        size += ring.size();
        offsets->insertValue(size);
        for (const auto & point : ring)
            point_serializer.add(point);
    }

    ColumnPtr finalize()
    {
        return ColumnArray::create(point_serializer.finalize(), std::move(offsets));
    }

private:
    size_t size = 0;
    PointSerializer<Point> point_serializer;
    ColumnUInt64::MutablePtr offsets;
};

template <typename Point>
class PolygonSerializer
{
public:
    PolygonSerializer()
        : offsets(ColumnUInt64::create())
    {}

    explicit PolygonSerializer(size_t n)
        : offsets(ColumnUInt64::create(n))
    {}

    void add(const Ring<Point> & ring)
    {
        size++;
        offsets->insertValue(size);
        ring_serializer.add(ring);
    }

    void add(const Polygon<Point> & polygon)
    {
        size += 1 + polygon.inners().size();
        offsets->insertValue(size);
        ring_serializer.add(polygon.outer());
        for (const auto & ring : polygon.inners())
            ring_serializer.add(ring);
    }

    ColumnPtr finalize()
    {
        return ColumnArray::create(ring_serializer.finalize(), std::move(offsets));
    }

private:
    size_t size = 0;
    RingSerializer<Point> ring_serializer;
    ColumnUInt64::MutablePtr offsets;
};

template <typename Point>
class MultiPolygonSerializer
{
public:
    MultiPolygonSerializer()
        : offsets(ColumnUInt64::create())
    {}

    explicit MultiPolygonSerializer(size_t n)
        : offsets(ColumnUInt64::create(n))
    {}

    void add(const Ring<Point> & ring)
    {
        size++;
        offsets->insertValue(size);
        polygon_serializer.add(ring);
    }

    void add(const Polygon<Point> & polygon)
    {
        size++;
        offsets->insertValue(size);
        polygon_serializer.add(polygon);
    }

    void add(const MultiPolygon<Point> & multi_polygon)
    {
        size += multi_polygon.size();
        offsets->insertValue(size);
        for (const auto & polygon : multi_polygon)
        {
            polygon_serializer.add(polygon);
        }
    }

    ColumnPtr finalize()
    {
        return ColumnArray::create(polygon_serializer.finalize(), std::move(offsets));
    }

private:
    size_t size = 0;
    PolygonSerializer<Point> polygon_serializer;
    ColumnUInt64::MutablePtr offsets;
};


template <typename PType>
struct ParserType
{
    using Type = PType;
};

template <typename Point, typename F>
static void callOnGeometryDataType(DataTypePtr type, F && f)
{
    if (DataTypeCustomRingSerialization::nestedDataType()->equals(*type))
        return f(ParserType<RingFromColumnParser<Point>>());
    if (DataTypeCustomPolygonSerialization::nestedDataType()->equals(*type))
        return f(ParserType<PolygonFromColumnParser<Point>>());
    if (DataTypeCustomMultiPolygonSerialization::nestedDataType()->equals(*type))
        return f(ParserType<MultiPolygonFromColumnParser<Point>>());
    throw Exception(fmt::format("Unknown geometry type {}", type->getName()), ErrorCodes::BAD_ARGUMENTS);
}


template <typename Point, typename F>
static void callOnTwoGeometryDataTypes(DataTypePtr left_type, DataTypePtr right_type, F && func)
{
    return callOnGeometryDataType<Point>(left_type, [&](const auto & left_types)
    {
        using LeftParserType = std::decay_t<decltype(left_types)>;

        return callOnGeometryDataType<Point>(right_type, [&](const auto & right_types)
        {
            using RightParserType = std::decay_t<decltype(right_types)>;

            return func(LeftParserType(), RightParserType());
        });
    });
}

}
