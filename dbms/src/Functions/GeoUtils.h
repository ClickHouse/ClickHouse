#pragma once
#include <Core/Types.h>
#include <Core/Defines.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Core/TypeListNumber.h>
#include <Common/typeid_cast.h>
#include <ext/range.h>

/// Warning in boost::geometry during template strategy substitution.
#pragma GCC diagnostic push

#if !__clang__
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

#pragma GCC diagnostic ignored "-Wunused-parameter"

#include <boost/geometry.hpp>

#pragma GCC diagnostic pop

#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>
#include <boost/geometry/geometries/multi_polygon.hpp>
#include <boost/geometry/geometries/segment.hpp>
#include <boost/geometry/algorithms/comparable_distance.hpp>
#include <boost/geometry/strategies/cartesian/distance_pythagoras.hpp>

#include <array>
#include <vector>
#include <iterator>
#include <cmath>
#include <algorithm>
#include <IO/WriteBufferFromString.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}


namespace GeoUtils
{


template <typename Polygon>
UInt64 getPolygonAllocatedBytes(const Polygon & polygon)
{
    UInt64 size = 0;

    using RingType = typename Polygon::ring_type;
    using ValueType = typename RingType::value_type;

    auto sizeOfRing = [](const RingType & ring) { return sizeof(ring) + ring.capacity() * sizeof(ValueType); };

    size += sizeOfRing(polygon.outer());

    const auto & inners = polygon.inners();
    size += sizeof(inners) + inners.capacity() * sizeof(RingType);
    for (auto & inner : inners)
        size += sizeOfRing(inner);

    return size;
}

template <typename MultiPolygon>
UInt64 getMultiPolygonAllocatedBytes(const MultiPolygon & multi_polygon)
{
    using ValueType = typename MultiPolygon::value_type;
    UInt64 size = multi_polygon.capacity() * sizeof(ValueType);

    for (const auto & polygon : multi_polygon)
        size += getPolygonAllocatedBytes(polygon);

    return size;
}

template <typename CoordinateType = Float32>
class PointInPolygonWithGrid
{
public:
    using Point = boost::geometry::model::d2::point_xy<CoordinateType>;
    /// Counter-Clockwise ordering.
    using Polygon = boost::geometry::model::polygon<Point, false>;
    using MultiPolygon = boost::geometry::model::multi_polygon<Polygon>;
    using Box = boost::geometry::model::box<Point>;
    using Segment = boost::geometry::model::segment<Point>;

    explicit PointInPolygonWithGrid(const Polygon & polygon, UInt16 grid_size = 8)
            : grid_size(std::max<UInt16>(1, grid_size)), polygon(polygon) {}

    void init();

    /// True if bound box is empty.
    bool hasEmptyBound() const { return has_empty_bound; }

    UInt64 getAllocatedBytes() const;

    inline bool ALWAYS_INLINE contains(CoordinateType x, CoordinateType y);

private:
    enum class CellType
    {
        inner,
        outer,
        singleLine,
        pairOfLinesSingleConvexPolygon,
        pairOfLinesSingleNonConvexPolygons,
        pairOfLinesDifferentPolygons,
        complexPolygon
    };

    struct HalfPlane
    {
        /// Line, a * x + b * y + c = 0. Vector (a, b) points inside half-plane.
        CoordinateType a;
        CoordinateType b;
        CoordinateType c;

        /// Take left half-plane.
        void fill(const Point & from, const Point & to)
        {
            a = -(to.y() - from.y());
            b = to.x() - from.x();
            c = -from.x() * a - from.y() * b;
        }

        /// Inner part of the HalfPlane is the left side of initialized vector.
        bool ALWAYS_INLINE contains(CoordinateType x, CoordinateType y) const { return a * x + b * y + c >= 0; }
    };

    struct Cell
    {
        static const int max_stored_half_planes = 2;

        HalfPlane half_planes[max_stored_half_planes];
        size_t index_of_inner_polygon;
        CellType type;
    };

    const UInt16 grid_size;

    Polygon polygon;
    std::vector<Cell> cells;
    std::vector<MultiPolygon> polygons;

    CoordinateType cell_width;
    CoordinateType cell_height;

    CoordinateType x_shift;
    CoordinateType y_shift;
    CoordinateType x_scale;
    CoordinateType y_scale;

    bool has_empty_bound = false;
    bool was_grid_built = false;

    void buildGrid();

    void calcGridAttributes(Box & box);

    template <typename T>
    T ALWAYS_INLINE getCellIndex(T row, T col) const { return row * grid_size + col; }

    /// Complex case. Will check intersection directly.
    inline void addComplexPolygonCell(size_t index, const Box & box);

    /// Empty intersection or intersection == box.
    inline void addCell(size_t index, const Box & empty_box);

    /// Intersection is a single polygon.
    inline void addCell(size_t index, const Box & box, const Polygon & intersection);

    /// Intersection is a pair of polygons.
    inline void addCell(size_t index, const Box & box, const Polygon & first, const Polygon & second);

    /// Returns a list of half-planes were formed from intersection edges without box edges.
    inline std::vector<HalfPlane> findHalfPlanes(const Box & box, const Polygon & intersection);

    /// Check that polygon.outer() is convex.
    inline bool isConvex(const Polygon & polygon);

    using Distance = typename boost::geometry::default_comparable_distance_result<Point, Segment>::type;

    /// min(distance(point, edge) : edge in polygon)
    inline Distance distance(const Point & point, const Polygon & polygon);
};

template <typename CoordinateType>
UInt64 PointInPolygonWithGrid<CoordinateType>::getAllocatedBytes() const
{
    UInt64 size = sizeof(*this);

    size += cells.capacity() * sizeof(Cell);
    size += polygons.capacity() * sizeof(MultiPolygon);
    size += getPolygonAllocatedBytes(polygon);

    for (const auto & polygon : polygons)
        size += getMultiPolygonAllocatedBytes(polygon);

    return size;
}

template <typename CoordinateType>
void PointInPolygonWithGrid<CoordinateType>::init()
{
    if (!was_grid_built)
        buildGrid();

    was_grid_built = true;
}

template <typename CoordinateType>
void PointInPolygonWithGrid<CoordinateType>::calcGridAttributes(
        PointInPolygonWithGrid<CoordinateType>::Box & box)
{
    boost::geometry::envelope(polygon, box);

    const Point & min_corner = box.min_corner();
    const Point & max_corner = box.max_corner();

    cell_width = (max_corner.x() - min_corner.x()) / grid_size;
    cell_height = (max_corner.y() - min_corner.y()) / grid_size;

    if (cell_width == 0 || cell_height == 0)
    {
        has_empty_bound = true;
        return;
    }

    x_scale = 1 / cell_width;
    y_scale = 1 / cell_height;
    x_shift = -min_corner.x();
    y_shift = -min_corner.y();
}

template <typename CoordinateType>
void PointInPolygonWithGrid<CoordinateType>::buildGrid()
{
    Box box;
    calcGridAttributes(box);

    if (has_empty_bound)
        return;

    cells.assign(grid_size * grid_size, {});

    const Point & min_corner = box.min_corner();

    for (size_t row = 0; row < grid_size; ++row)
    {
        CoordinateType y_min = min_corner.y() + row * cell_height;
        CoordinateType y_max = min_corner.y() + (row + 1) * cell_height;

        for (size_t col = 0; col < grid_size; ++col)
        {
            CoordinateType x_min = min_corner.x() + col * cell_width;
            CoordinateType x_max = min_corner.x() + (col + 1) * cell_width;
            Box cell_box(Point(x_min, y_min), Point(x_max, y_max));

            Polygon cell_bound;
            boost::geometry::convert(cell_box, cell_bound);

            MultiPolygon intersection;
            boost::geometry::intersection(polygon, cell_bound, intersection);

            size_t cellIndex = getCellIndex(row, col);

            if (intersection.empty())
                addCell(cellIndex, cell_box);
            else if (intersection.size() == 1)
                addCell(cellIndex, cell_box, intersection.front());
            else if (intersection.size() == 2)
                addCell(cellIndex, cell_box, intersection.front(), intersection.back());
            else
                addComplexPolygonCell(cellIndex, cell_box);
        }
    }
}

template <typename CoordinateType>
bool PointInPolygonWithGrid<CoordinateType>::contains(CoordinateType x, CoordinateType y)
{
    if (has_empty_bound)
        return false;

    CoordinateType float_row = (y + y_shift) * y_scale;
    CoordinateType float_col = (x + x_shift) * x_scale;

    if (float_row < 0 || float_row > grid_size)
        return false;
    if (float_col < 0 || float_col > grid_size)
        return false;

    int row = std::min<int>(float_row, grid_size - 1);
    int col = std::min<int>(float_col, grid_size - 1);

    int index = getCellIndex(row, col);
    const auto & cell = cells[index];

    switch (cell.type)
    {
        case CellType::inner:
            return true;
        case CellType::outer:
            return false;
        case CellType::singleLine:
            return cell.half_planes[0].contains(x, y);
        case CellType::pairOfLinesSingleConvexPolygon:
            return cell.half_planes[0].contains(x, y) && cell.half_planes[1].contains(x, y);
        case CellType::pairOfLinesDifferentPolygons:
        case CellType::pairOfLinesSingleNonConvexPolygons:
            return cell.half_planes[0].contains(x, y) || cell.half_planes[1].contains(x, y);
        case CellType::complexPolygon:
            return boost::geometry::within(Point(x, y), polygons[cell.index_of_inner_polygon]);
        default:
            return false;

    }
}

template <typename CoordinateType>
typename PointInPolygonWithGrid<CoordinateType>::Distance
PointInPolygonWithGrid<CoordinateType>::distance(
        const PointInPolygonWithGrid<CoordinateType>::Point & point,
        const PointInPolygonWithGrid<CoordinateType>::Polygon & polygon)
{
    const auto & outer = polygon.outer();
    Distance distance = 0;
    for (auto i : ext::range(0, outer.size() - 1))
    {
        Segment segment(outer[i], outer[i + 1]);
        Distance current = boost::geometry::comparable_distance(point, segment);
        distance = i ? std::min(current, distance) : current;
    }
    return distance;
}

template <typename CoordinateType>
bool PointInPolygonWithGrid<CoordinateType>::isConvex(const PointInPolygonWithGrid<CoordinateType>::Polygon & polygon)
{
    const auto & outer = polygon.outer();
    /// Segment or point.
    if (outer.size() < 4)
        return false;

    auto vecProduct = [](const Point & from, const Point & to) { return from.x() * to.y() - from.y() * to.x(); };
    auto getVector = [](const Point & from, const Point & to) -> Point
    {
        return Point(to.x() - from.x(), to.y() - from.y());
    };

    Point first = getVector(outer[0], outer[1]);
    Point prev = first;

    for (auto i : ext::range(1, outer.size() - 1))
    {
        Point cur = getVector(outer[i], outer[i + 1]);
        if (vecProduct(prev, cur) < 0)
            return false;

        prev = cur;
    }

    return vecProduct(prev, first) >= 0;
}

template <typename CoordinateType>
std::vector<typename PointInPolygonWithGrid<CoordinateType>::HalfPlane>
PointInPolygonWithGrid<CoordinateType>::findHalfPlanes(
        const PointInPolygonWithGrid<CoordinateType>::Box & box,
        const PointInPolygonWithGrid<CoordinateType>::Polygon & intersection)
{
    std::vector<HalfPlane> half_planes;
    Polygon bound;
    boost::geometry::convert(box, bound);
    const auto & outer = intersection.outer();

    for (auto i : ext::range(0, outer.size() - 1))
    {
        /// Want to detect is intersection edge was formed from box edge or from polygon edge.
        /// If center of the edge closer to box, than don't form the half-plane.
        Segment segment(outer[i], outer[i + 1]);
        Point center((segment.first.x() + segment.second.x()) / 2, (segment.first.y() + segment.second.y()) / 2);
        if (distance(center, polygon) < distance(center, bound))
        {
            half_planes.push_back({});
            half_planes.back().fill(segment.first, segment.second);
        }
    }

    return half_planes;
}

template <typename CoordinateType>
void PointInPolygonWithGrid<CoordinateType>::addComplexPolygonCell(
        size_t index, const PointInPolygonWithGrid<CoordinateType>::Box & box)
{
    cells[index].type = CellType::complexPolygon;
    cells[index].index_of_inner_polygon = polygons.size();

    /// Expand box in (1 + eps_factor) times to eliminate errors for points on box bound.
    static constexpr float eps_factor = 0.01;
    float x_eps = eps_factor * (box.max_corner().x() - box.min_corner().x());
    float y_eps = eps_factor * (box.max_corner().y() - box.min_corner().y());

    Point min_corner(box.min_corner().x() - x_eps, box.min_corner().y() - y_eps);
    Point max_corner(box.max_corner().x() + x_eps, box.max_corner().y() + y_eps);
    Box box_with_eps_bound(min_corner, max_corner);

    Polygon bound;
    boost::geometry::convert(box_with_eps_bound, bound);

    MultiPolygon intersection;
    boost::geometry::intersection(polygon, bound, intersection);

    polygons.push_back(intersection);
}

template <typename CoordinateType>
void PointInPolygonWithGrid<CoordinateType>::addCell(
        size_t index, const PointInPolygonWithGrid<CoordinateType>::Box & empty_box)
{
    const auto & min_corner = empty_box.min_corner();
    const auto & max_corner = empty_box.max_corner();

    Point center((min_corner.x() + max_corner.x()) / 2, (min_corner.y() + max_corner.y()) / 2);

    if (boost::geometry::within(center, polygon))
        cells[index].type = CellType::inner;
    else
        cells[index].type = CellType::outer;

}

template <typename CoordinateType>
void PointInPolygonWithGrid<CoordinateType>::addCell(
        size_t index,
        const PointInPolygonWithGrid<CoordinateType>::Box & box,
        const PointInPolygonWithGrid<CoordinateType>::Polygon & intersection)
{
    if (!intersection.inners().empty())
        addComplexPolygonCell(index, box);

    auto half_planes = findHalfPlanes(box, intersection);

    if (half_planes.empty())
        addCell(index, box);
    else if (half_planes.size() == 1)
    {
        cells[index].type = CellType::singleLine;
        cells[index].half_planes[0] = half_planes[0];
    }
    else if (half_planes.size() == 2)
    {
        cells[index].type = isConvex(intersection) ? CellType::pairOfLinesSingleConvexPolygon
                                                   : CellType::pairOfLinesSingleNonConvexPolygons;
        cells[index].half_planes[0] = half_planes[0];
        cells[index].half_planes[1] = half_planes[1];
    }
    else
        addComplexPolygonCell(index, box);
}

template <typename CoordinateType>
void PointInPolygonWithGrid<CoordinateType>::addCell(
        size_t index,
        const PointInPolygonWithGrid<CoordinateType>::Box & box,
        const PointInPolygonWithGrid<CoordinateType>::Polygon & first,
        const PointInPolygonWithGrid<CoordinateType>::Polygon & second)
{
    if (!first.inners().empty() || !second.inners().empty())
        addComplexPolygonCell(index, box);

    auto first_half_planes = findHalfPlanes(box, first);
    auto second_half_planes = findHalfPlanes(box, second);

    if (first_half_planes.empty())
        addCell(index, box, first);
    else if (second_half_planes.empty())
        addCell(index, box, second);
    else if (first_half_planes.size() == 1 && second_half_planes.size() == 1)
    {
        cells[index].type = CellType::pairOfLinesDifferentPolygons;
        cells[index].half_planes[0] = first_half_planes[0];
        cells[index].half_planes[1] = second_half_planes[0];
    }
    else
        addComplexPolygonCell(index, box);
}


template <typename Strategy, typename CoordinateType = Float32>
class PointInPolygon
{
public:
    using Point = boost::geometry::model::d2::point_xy<CoordinateType>;
    /// Counter-Clockwise ordering.
    using Polygon = boost::geometry::model::polygon<Point, false>;
    using Box = boost::geometry::model::box<Point>;

    explicit PointInPolygon(const Polygon & polygon) : polygon(polygon) {}

    void init()
    {
        boost::geometry::envelope(polygon, box);

        const Point & min_corner = box.min_corner();
        const Point & max_corner = box.max_corner();

        if (min_corner.x() == max_corner.x() || min_corner.y() == max_corner.y())
            has_empty_bound = true;
    }

    bool hasEmptyBound() const { return has_empty_bound; }

    inline bool ALWAYS_INLINE contains(CoordinateType x, CoordinateType y)
    {
        Point point(x, y);

        if (!boost::geometry::within(point, box))
            return false;

        return boost::geometry::covered_by(point, polygon, strategy);
    }

    UInt64 getAllocatedBytes() const { return sizeof(*this); }

private:
    const Polygon & polygon;
    Box box;
    bool has_empty_bound = false;
    Strategy strategy;
};


/// Algorithms.

template <typename T, typename U, typename PointInPolygonImpl>
ColumnPtr pointInPolygon(const ColumnVector<T> & x, const ColumnVector<U> & y, PointInPolygonImpl && impl)
{
    auto size = x.size();

    impl.init();

    if (impl.hasEmptyBound())
    {
        return ColumnVector<UInt8>::create(size, 0);
    }

    auto result = ColumnVector<UInt8>::create(size);
    auto & data = result->getData();

    const auto & x_data = x.getData();
    const auto & y_data = y.getData();

    for (auto i : ext::range(0, size))
    {
        data[i] = static_cast<UInt8>(impl.contains(x_data[i], y_data[i]));
    }

    return std::move(result);
}

template <typename ... Types>
struct CallPointInPolygon;

template <typename Type, typename ... Types>
struct CallPointInPolygon<Type, Types ...>
{
    template <typename T, typename PointInPolygonImpl>
    static ColumnPtr call(const ColumnVector<T> & x, const IColumn & y, PointInPolygonImpl && impl)
    {
        if (auto column = typeid_cast<const ColumnVector<Type> *>(&y))
            return pointInPolygon(x, *column, impl);
        return CallPointInPolygon<Types ...>::template call<T>(x, y, impl);
    }

    template <typename PointInPolygonImpl>
    static ColumnPtr call(const IColumn & x, const IColumn & y, PointInPolygonImpl && impl)
    {
        using Impl = typename ApplyTypeListForClass<::DB::GeoUtils::CallPointInPolygon, TypeListNumbers>::Type;
        if (auto column = typeid_cast<const ColumnVector<Type> *>(&x))
            return Impl::template call<Type>(*column, y, impl);
        return CallPointInPolygon<Types ...>::call(x, y, impl);
    }
};

template <>
struct CallPointInPolygon<>
{
    template <typename T, typename PointInPolygonImpl>
    static ColumnPtr call(const ColumnVector<T> &, const IColumn & y, PointInPolygonImpl &&)
    {
        throw Exception(std::string("Unknown numeric column type: ") + demangle(typeid(y).name()), ErrorCodes::LOGICAL_ERROR);
    }

    template <typename PointInPolygonImpl>
    static ColumnPtr call(const IColumn & x, const IColumn &, PointInPolygonImpl &&)
    {
        throw Exception(std::string("Unknown numeric column type: ") + demangle(typeid(x).name()), ErrorCodes::LOGICAL_ERROR);
    }
};

template <typename PointInPolygonImpl>
ColumnPtr pointInPolygon(const IColumn & x, const IColumn & y, PointInPolygonImpl && impl)
{
    using Impl = typename ApplyTypeListForClass<::DB::GeoUtils::CallPointInPolygon, TypeListNumbers>::Type;
    return Impl::call(x, y, impl);
}

/// Total angle (signed) between neighbor vectors in linestring. Zero if linestring.size() < 2.
template <typename Linestring>
float calcLinestringRotation(const Linestring & points)
{
    using Point = std::decay_t<decltype(*points.begin())>;
    float rotation = 0;

    auto sqrLength = [](const Point & point) { return point.x() * point.x() + point.y() * point.y(); };
    auto vecProduct = [](const Point & from, const Point & to) { return from.x() * to.y() - from.y() * to.x(); };
    auto getVector = [](const Point & from, const Point & to) -> Point
    {
        return Point(to.x() - from.x(), to.y() - from.y());
    };

    for (auto it = points.begin(); std::next(it) != points.end(); ++it)
    {
        if (it != points.begin())
        {
            auto prev = std::prev(it);
            auto next = std::next(it);
            Point from = getVector(*prev, *it);
            Point to = getVector(*it, *next);
            float sqr_from_len = sqrLength(from);
            float sqr_to_len = sqrLength(to);
            float sqr_len_product = (sqr_from_len * sqr_to_len);
            if (std::isfinite(sqr_len_product))
            {
                float vec_prod = vecProduct(from, to);
                float sin_ang = vec_prod * std::fabs(vec_prod) / sqr_len_product;
                sin_ang = std::max(-1.f, std::min(1.f, sin_ang));
                rotation += std::asin(sin_ang);
            }
        }
    }

    return rotation;
}

/// Make inner linestring counter-clockwise and outers clockwise oriented.
template <typename Polygon>
void normalizePolygon(Polygon && polygon)
{
    auto & outer = polygon.outer();
    if (calcLinestringRotation(outer) < 0)
        std::reverse(outer.begin(), outer.end());

    auto & inners = polygon.inners();
    for (auto & inner : inners)
        if (calcLinestringRotation(inner) > 0)
            std::reverse(inner.begin(), inner.end());
}


template <typename Polygon>
std::string serialize(Polygon && polygon)
{
    std::string result;

    {
        WriteBufferFromString buffer(result);

        using RingType = typename std::decay_t<Polygon>::ring_type;

        auto serializeFloat = [&buffer](float value) { buffer.write(reinterpret_cast<char *>(&value), sizeof(value)); };
        auto serializeSize = [&buffer](size_t size) { buffer.write(reinterpret_cast<char *>(&size), sizeof(size)); };

        auto serializeRing = [& serializeFloat, & serializeSize](const RingType & ring)
        {
            serializeSize(ring.size());
            for (const auto & point : ring)
            {
                serializeFloat(point.x());
                serializeFloat(point.y());
            }
        };

        serializeRing(polygon.outer());

        const auto & inners = polygon.inners();
        serializeSize(inners.size());
        for (auto & inner : inners)
            serializeRing(inner);
    }

    return result;
}


} /// GeoUtils

} /// DB
