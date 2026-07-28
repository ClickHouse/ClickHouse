#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <Core/Types.h>

#include <boost/geometry/geometries/geometries.hpp>
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Common/NaNUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/castColumn.h>

#include <cmath>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename Point>
using LineString = boost::geometry::model::linestring<Point>;

template <typename Point>
using MultiLineString = boost::geometry::model::multi_linestring<LineString<Point>>;

template <typename Point>
using Ring = boost::geometry::model::ring<Point>;

template <typename Point>
using Polygon = boost::geometry::model::polygon<Point>;

template <typename Point>
using MultiPolygon = boost::geometry::model::multi_polygon<Polygon<Point>>;

using CartesianPoint = boost::geometry::model::d2::point_xy<Float64>;
using CartesianLineString = LineString<CartesianPoint>;
using CartesianMultiLineString = MultiLineString<CartesianPoint>;
using CartesianRing = Ring<CartesianPoint>;
using CartesianPolygon = Polygon<CartesianPoint>;
using CartesianMultiPolygon = MultiPolygon<CartesianPoint>;

using SphericalPoint = boost::geometry::model::point<Float64, 2, boost::geometry::cs::spherical_equatorial<boost::geometry::degree>>;
using SphericalLineString = LineString<SphericalPoint>;
using SphericalMultiLineString = MultiLineString<SphericalPoint>;
using SphericalRing = Ring<SphericalPoint>;
using SphericalPolygon = Polygon<SphericalPoint>;
using SphericalMultiPolygon = MultiPolygon<SphericalPoint>;

/**
 * Class which takes converts Column with type Tuple(Float64, Float64) to a vector of boost point type.
 * They are (x,y) in case of cartesian coordinated and (lon,lat) in case of Spherical.
*/
template <typename Point>
struct ColumnToPointsConverter
{
    static std::vector<Point> convert(ColumnPtr col)
    {
        const auto * tuple = typeid_cast<const ColumnTuple *>(col.get());
        const auto & tuple_columns = tuple->getColumns();

        const auto * x_data = typeid_cast<const ColumnFloat64 *>(tuple_columns[0].get());
        const auto * y_data = typeid_cast<const ColumnFloat64 *>(tuple_columns[1].get());

        const auto * first_container = x_data->getData().data();
        const auto * second_container = y_data->getData().data();

        std::vector<Point> answer(col->size());

        for (size_t i = 0; i < col->size(); ++i)
        {
            const Float64 first = first_container[i];
            const Float64 second = second_container[i];

            if (isNaN(first) || isNaN(second))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Point's component must not be NaN");

            if (std::isinf(first) || std::isinf(second))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Point's component must not be infinite");

            answer[i] = Point(first, second);
        }

        return answer;
    }
};


/**
 * Class which converts Column with type Array(Tuple(Float64, Float64)) to a vector of boost linestring type.
*/
template <typename Point>
struct ColumnToLineStringsConverter
{
    static std::vector<LineString<Point>> convert(ColumnPtr col)
    {
        const IColumn::Offsets & offsets = typeid_cast<const ColumnArray &>(*col).getOffsets();
        size_t prev_offset = 0;
        std::vector<LineString<Point>> answer;
        answer.reserve(offsets.size());
        auto tmp = ColumnToPointsConverter<Point>::convert(typeid_cast<const ColumnArray &>(*col).getDataPtr());
        for (size_t offset : offsets)
        {
            answer.emplace_back(tmp.begin() + prev_offset, tmp.begin() + offset);
            prev_offset = offset;
        }
        return answer;
    }
};

/**
 * Class which converts Column with type Array(Array(Tuple(Float64, Float64))) to a vector of boost multi_linestring type.
*/
template <typename Point>
struct ColumnToMultiLineStringsConverter
{
    static std::vector<MultiLineString<Point>> convert(ColumnPtr col)
    {
        const IColumn::Offsets & offsets = typeid_cast<const ColumnArray &>(*col).getOffsets();
        size_t prev_offset = 0;
        std::vector<MultiLineString<Point>> answer(offsets.size());
        auto all_linestrings = ColumnToLineStringsConverter<Point>::convert(typeid_cast<const ColumnArray &>(*col).getDataPtr());
        for (size_t iter = 0; iter < offsets.size() && iter < all_linestrings.size(); ++iter)
        {
            for (size_t linestring_iter = prev_offset; linestring_iter < offsets[iter]; ++linestring_iter)
                answer[iter].emplace_back(std::move(all_linestrings[linestring_iter]));
            prev_offset = offsets[iter];
        }
        return answer;
    }
};

/**
 * Class which converts Column with type Array(Tuple(Float64, Float64)) to a vector of boost ring type.
*/
template <typename Point>
struct ColumnToRingsConverter
{
    static std::vector<Ring<Point>> convert(ColumnPtr col)
    {
        const IColumn::Offsets & offsets = typeid_cast<const ColumnArray &>(*col).getOffsets();
        size_t prev_offset = 0;
        std::vector<Ring<Point>> answer;
        answer.reserve(offsets.size());
        auto tmp = ColumnToPointsConverter<Point>::convert(typeid_cast<const ColumnArray &>(*col).getDataPtr());
        for (size_t offset : offsets)
        {
            answer.emplace_back(tmp.begin() + prev_offset, tmp.begin() + offset);
            prev_offset = offset;
        }
        return answer;
    }
};

/**
 * Class which converts Column with type Array(Array(Tuple(Float64, Float64))) to a vector of boost polygon type.
*/
template <typename Point>
struct ColumnToPolygonsConverter
{
    static std::vector<Polygon<Point>> convert(ColumnPtr col)
    {
        const IColumn::Offsets & offsets = typeid_cast<const ColumnArray &>(*col).getOffsets();
        std::vector<Polygon<Point>> answer(offsets.size());
        auto all_rings = ColumnToRingsConverter<Point>::convert(typeid_cast<const ColumnArray &>(*col).getDataPtr());

        size_t prev_offset = 0;
        for (size_t iter = 0; iter < offsets.size(); ++iter)
        {
            const auto current_array_size = offsets[iter] - prev_offset;
            if (current_array_size == 0)
            {
                answer.emplace_back();
                continue;
            }

            answer[iter].outer() = std::move(all_rings[prev_offset]);
            answer[iter].inners().reserve(current_array_size);
            for (size_t inner_holes = prev_offset + 1; inner_holes < offsets[iter]; ++inner_holes)
                answer[iter].inners().emplace_back(std::move(all_rings[inner_holes]));
            prev_offset = offsets[iter];
        }

        return answer;
    }
};

/**
 * Class which converts Column with type Array(Array(Array(Tuple(Float64, Float64)))) to a vector of boost multi_polygon type.
*/
template <typename Point>
struct ColumnToMultiPolygonsConverter
{
    static std::vector<MultiPolygon<Point>> convert(ColumnPtr col)
    {
        const IColumn::Offsets & offsets = typeid_cast<const ColumnArray &>(*col).getOffsets();
        size_t prev_offset = 0;
        std::vector<MultiPolygon<Point>> answer(offsets.size());

        auto all_polygons = ColumnToPolygonsConverter<Point>::convert(typeid_cast<const ColumnArray &>(*col).getDataPtr());

        for (size_t iter = 0; iter < offsets.size() && iter < all_polygons.size(); ++iter)
        {
            for (size_t polygon_iter = prev_offset; polygon_iter < offsets[iter]; ++polygon_iter)
                answer[iter].emplace_back(std::move(all_polygons[polygon_iter]));
            prev_offset = offsets[iter];
        }

        return answer;
    }
};


/// To serialize Spherical or Cartesian point (a pair of numbers in both cases).
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

/// Serialize Point, LineString as LineString
template <typename Point>
class LineStringSerializer
{
public:
    LineStringSerializer()
        : offsets(ColumnUInt64::create())
    {}

    explicit LineStringSerializer(size_t n)
        : offsets(ColumnUInt64::create(n))
    {}

    void add(const LineString<Point> & ring)
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

/// Serialize Point, MultiLineString as MultiLineString
template <typename Point>
class MultiLineStringSerializer
{
public:
    MultiLineStringSerializer()
        : offsets(ColumnUInt64::create())
    {}

    explicit MultiLineStringSerializer(size_t n)
        : offsets(ColumnUInt64::create(n))
    {}

    void add(const MultiLineString<Point> & multilinestring)
    {
        size += multilinestring.size();
        offsets->insertValue(size);
        for (const auto & linestring : multilinestring)
            linestring_serializer.add(linestring);
    }

    ColumnPtr finalize()
    {
        return ColumnArray::create(linestring_serializer.finalize(), std::move(offsets));
    }

private:
    size_t size = 0;
    LineStringSerializer<Point> linestring_serializer;
    ColumnUInt64::MutablePtr offsets;
};

/// Almost the same as LineStringSerializer
/// Serialize Point, Ring as Ring
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

/// Serialize Point, Ring, Polygon as Polygon
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
        /// Outer ring + all inner rings (holes).
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

/// Serialize Point, Ring, Polygon, MultiPolygon as MultiPolygon
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
struct ConverterType
{
    using Type = PType;
};

template <typename Point, typename F>
static void callOnGeometryDataType(DataTypePtr type, F && f)
{
    const auto & factory = DataTypeFactory::instance();

    /// There is no Point type, because for most of geometry functions it is useless.
    if (factory.get("Point")->equals(*type))
        return f(ConverterType<ColumnToPointsConverter<Point>>());

    /// We should take the name into consideration to avoid ambiguity.
    /// Because for example both Ring and LineString are resolved to Array(Tuple(Point)).
    if (factory.get("LineString")->equals(*type) && type->getCustomName() && type->getCustomName()->getName() == "LineString")
        return f(ConverterType<ColumnToLineStringsConverter<Point>>());

    /// We should take the name into consideration to avoid ambiguity.
    /// Because for example both MultiLineString and Polygon are resolved to Array(Array(Point)).
    if (factory.get("MultiLineString")->equals(*type) && type->getCustomName() && type->getCustomName()->getName() == "MultiLineString")
        return f(ConverterType<ColumnToMultiLineStringsConverter<Point>>());

    /// For backward compatibility if we call this function not on a custom type, we will consider Array(Tuple(Point)) as type Ring.
    if (factory.get("Ring")->equals(*type))
        return f(ConverterType<ColumnToRingsConverter<Point>>());

    if (factory.get("Polygon")->equals(*type))
        return f(ConverterType<ColumnToPolygonsConverter<Point>>());
    if (factory.get("MultiPolygon")->equals(*type))
        return f(ConverterType<ColumnToMultiPolygonsConverter<Point>>());
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown geometry type {}", type->getName());
}


template <typename Point, typename F>
static void callOnTwoGeometryDataTypes(DataTypePtr left_type, DataTypePtr right_type, F && func)
{
    return callOnGeometryDataType<Point>(left_type, [&](const auto & left_types)
    {
        using LeftConverterType = std::decay_t<decltype(left_types)>;

        return callOnGeometryDataType<Point>(right_type, [&](const auto & right_types)
        {
            using RightConverterType = std::decay_t<decltype(right_types)>;

            return func(LeftConverterType(), RightConverterType());
        });
    });
}

}
