#pragma once

#include <Core/Types.h>
#include <Core/Defines.h>
#include <Core/TypeListNumber.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <Common/SipHash.h>
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

#include <array>
#include <vector>
#include <iterator>
#include <cmath>
#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


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


/// This algorithm can be used as a baseline for comparison.
template <typename CoordinateType>
class PointInPolygonTrivial
{
public:
    using Point = boost::geometry::model::d2::point_xy<CoordinateType>;
    /// Counter-Clockwise ordering.
    using Polygon = boost::geometry::model::polygon<Point, false>;
    using MultiPolygon = boost::geometry::model::multi_polygon<Polygon>;
    using Box = boost::geometry::model::box<Point>;
    using Segment = boost::geometry::model::segment<Point>;

    explicit PointInPolygonTrivial(const Polygon & polygon_)
        : polygon(polygon_) {}

    /// True if bound box is empty.
    bool hasEmptyBound() const { return false; }

    UInt64 getAllocatedBytes() const { return 0; }

    bool contains(CoordinateType x, CoordinateType y) const
    {
        return boost::geometry::covered_by(Point(x, y), polygon);
    }

private:
    Polygon polygon;
};


/// Simple algorithm with bounding box.
template <typename Strategy, typename CoordinateType>
class PointInPolygon
{
public:
    using Point = boost::geometry::model::d2::point_xy<CoordinateType>;
    /// Counter-Clockwise ordering.
    using Polygon = boost::geometry::model::polygon<Point, false>;
    using Box = boost::geometry::model::box<Point>;

    explicit PointInPolygon(const Polygon & polygon_) : polygon(polygon_)
    {
        boost::geometry::envelope(polygon, box);

        const Point & min_corner = box.min_corner();
        const Point & max_corner = box.max_corner();

        if (min_corner.x() == max_corner.x() || min_corner.y() == max_corner.y())
            has_empty_bound = true;
    }

    bool hasEmptyBound() const { return has_empty_bound; }

    inline bool ALWAYS_INLINE contains(CoordinateType x, CoordinateType y) const
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


/// Optimized algorithm with bounding box and grid.
template <typename CoordinateType>
class PointInPolygonWithGrid
{
public:
    using Point = boost::geometry::model::d2::point_xy<CoordinateType>;
    /// Counter-Clockwise ordering.
    using Polygon = boost::geometry::model::polygon<Point, false>;
    using MultiPolygon = boost::geometry::model::multi_polygon<Polygon>;
    using Box = boost::geometry::model::box<Point>;
    using Segment = boost::geometry::model::segment<Point>;

    explicit PointInPolygonWithGrid(const Polygon & polygon_, UInt16 grid_size_ = 8)
        : grid_size(std::max<UInt16>(1, grid_size_)), polygon(polygon_)
    {
        buildGrid();
    }

    /// True if bound box is empty.
    bool hasEmptyBound() const { return has_empty_bound; }

    UInt64 getAllocatedBytes() const;

    inline bool ALWAYS_INLINE contains(CoordinateType x, CoordinateType y) const;

private:
    enum class CellType
    {
        inner,                                  /// The cell is completely inside polygon.
        outer,                                  /// The cell is completely outside of polygon.
        singleLine,                             /// The cell is splitted to inner/outer part by a single line.
        pairOfLinesSingleConvexPolygon,         /// The cell is splitted to inner/outer part by a polyline of two sections and inner part is convex.
        pairOfLinesSingleNonConvexPolygons,     /// The cell is splitted to inner/outer part by a polyline of two sections and inner part is non convex.
        pairOfLinesDifferentPolygons,           /// The cell is spliited by two lines to three different parts.
        complexPolygon                          /// Generic case.
    };

    struct HalfPlane
    {
        /// Line, a * x + b * y + c = 0. Vector (a, b) points inside half-plane.
        CoordinateType a;
        CoordinateType b;
        CoordinateType c;

        HalfPlane() = default;

        /// Take left half-plane.
        HalfPlane(const Point & from, const Point & to)
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

    void buildGrid();

    /// Calculate bounding box and shift/scale of cells.
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
};


template <typename CoordinateType>
UInt64 PointInPolygonWithGrid<CoordinateType>::getAllocatedBytes() const
{
    UInt64 size = sizeof(*this);

    size += cells.capacity() * sizeof(Cell);
    size += polygons.capacity() * sizeof(MultiPolygon);
    size += getPolygonAllocatedBytes(polygon);

    for (const auto & elem : polygons)
        size += getMultiPolygonAllocatedBytes(elem);

    return size;
}

template <typename CoordinateType>
void PointInPolygonWithGrid<CoordinateType>::calcGridAttributes(
        PointInPolygonWithGrid<CoordinateType>::Box & box)
{
    boost::geometry::envelope(polygon, box);

    const Point & min_corner = box.min_corner();
    const Point & max_corner = box.max_corner();

#pragma GCC diagnostic push
#if !__clang__
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

    cell_width = (max_corner.x() - min_corner.x()) / grid_size;
    cell_height = (max_corner.y() - min_corner.y()) / grid_size;

#pragma GCC diagnostic pop

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

    cells.assign(size_t(grid_size) * grid_size, {});

    const Point & min_corner = box.min_corner();

    for (size_t row = 0; row < grid_size; ++row)
    {
#pragma GCC diagnostic push
#if !__clang__
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
        CoordinateType y_min = min_corner.y() + row * cell_height;
        CoordinateType y_max = min_corner.y() + (row + 1) * cell_height;

        for (size_t col = 0; col < grid_size; ++col)
        {
            CoordinateType x_min = min_corner.x() + col * cell_width;
            CoordinateType x_max = min_corner.x() + (col + 1) * cell_width;
#pragma GCC diagnostic pop
            Box cell_box(Point(x_min, y_min), Point(x_max, y_max));

            MultiPolygon intersection;
            boost::geometry::intersection(polygon, cell_box, intersection);

            size_t cell_index = getCellIndex(row, col);

            if (intersection.empty())
                addCell(cell_index, cell_box);
            else if (intersection.size() == 1)
                addCell(cell_index, cell_box, intersection.front());
            else if (intersection.size() == 2)
                addCell(cell_index, cell_box, intersection.front(), intersection.back());
            else
                addComplexPolygonCell(cell_index, cell_box);
        }
    }
}

template <typename CoordinateType>
bool PointInPolygonWithGrid<CoordinateType>::contains(CoordinateType x, CoordinateType y) const
{
    if (has_empty_bound)
        return false;

    if (std::isnan(x) || std::isnan(y))
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
        case CellType::pairOfLinesDifferentPolygons: [[fallthrough]];
        case CellType::pairOfLinesSingleNonConvexPolygons:
            return cell.half_planes[0].contains(x, y) || cell.half_planes[1].contains(x, y);
        case CellType::complexPolygon:
            return boost::geometry::within(Point(x, y), polygons[cell.index_of_inner_polygon]);
    }

    __builtin_unreachable();
}


template <typename CoordinateType>
bool PointInPolygonWithGrid<CoordinateType>::isConvex(const PointInPolygonWithGrid<CoordinateType>::Polygon & poly)
{
    const auto & outer = poly.outer();
    /// Segment or point.
    if (outer.size() < 4)
        return false;

    auto vec_product = [](const Point & from, const Point & to) { return from.x() * to.y() - from.y() * to.x(); };
    auto get_vector = [](const Point & from, const Point & to) -> Point
    {
        return Point(to.x() - from.x(), to.y() - from.y());
    };

    Point first = get_vector(outer[0], outer[1]);
    Point prev = first;

    for (auto i : ext::range(1, outer.size() - 1))
    {
        Point cur = get_vector(outer[i], outer[i + 1]);
        if (vec_product(prev, cur) < 0)
            return false;

        prev = cur;
    }

    return vec_product(prev, first) >= 0;
}

template <typename CoordinateType>
std::vector<typename PointInPolygonWithGrid<CoordinateType>::HalfPlane>
PointInPolygonWithGrid<CoordinateType>::findHalfPlanes(
        const PointInPolygonWithGrid<CoordinateType>::Box & box,
        const PointInPolygonWithGrid<CoordinateType>::Polygon & intersection)
{
    std::vector<HalfPlane> half_planes;
    const auto & outer = intersection.outer();

    for (auto i : ext::range(0, outer.size() - 1))
    {
        /// Want to detect is intersection edge was formed from box edge or from polygon edge.
        /// If section (x1, y1), (x2, y2) is on box edge, then either x1 = x2 = one of box_x or y1 = y2 = one of box_y

        auto x1 = outer[i].x();
        auto y1 = outer[i].y();
        auto x2 = outer[i + 1].x();
        auto y2 = outer[i + 1].y();

        auto box_x1 = box.min_corner().x();
        auto box_y1 = box.min_corner().y();
        auto box_x2 = box.max_corner().x();
        auto box_y2 = box.max_corner().y();

        if (! ((x1 == x2 && (x1 == box_x1 || x2 == box_x2))
            || (y1 == y2 && (y1 == box_y1 || y2 == box_y2))))
        {
            half_planes.emplace_back(Point(x1, y1), Point(x2, y2));
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
    static constexpr CoordinateType eps_factor = 0.01;
    auto x_eps = eps_factor * (box.max_corner().x() - box.min_corner().x());
    auto y_eps = eps_factor * (box.max_corner().y() - box.min_corner().y());

    Point min_corner(box.min_corner().x() - x_eps, box.min_corner().y() - y_eps);
    Point max_corner(box.max_corner().x() + x_eps, box.max_corner().y() + y_eps);
    Box box_with_eps_bound(min_corner, max_corner);

    MultiPolygon intersection;
    boost::geometry::intersection(polygon, box_with_eps_bound, intersection);

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
    {
        addCell(index, box);
    }
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


/// Algorithms.

template <typename T, typename U, typename PointInPolygonImpl>
ColumnPtr pointInPolygon(const ColumnVector<T> & x, const ColumnVector<U> & y, PointInPolygonImpl && impl)
{
    auto size = x.size();

    if (impl.hasEmptyBound())
        return ColumnVector<UInt8>::create(size, 0);

    auto result = ColumnVector<UInt8>::create(size);
    auto & data = result->getData();

    const auto & x_data = x.getData();
    const auto & y_data = y.getData();

    for (auto i : ext::range(0, size))
        data[i] = static_cast<UInt8>(impl.contains(x_data[i], y_data[i]));

    return result;
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
        using Impl = typename ApplyTypeListForClass<CallPointInPolygon, TypeListNativeNumbers>::Type;
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
NO_INLINE ColumnPtr pointInPolygon(const IColumn & x, const IColumn & y, PointInPolygonImpl && impl)
{
    using Impl = typename ApplyTypeListForClass<CallPointInPolygon, TypeListNativeNumbers>::Type;
    return Impl::call(x, y, impl);
}


template <typename Polygon>
UInt128 sipHash128(Polygon && polygon)
{
    SipHash hash;

    auto hash_ring = [&hash](const auto & ring)
    {
        UInt32 size = ring.size();
        hash.update(size);
        hash.update(reinterpret_cast<const char *>(ring.data()), size * sizeof(ring[0]));
    };

    hash_ring(polygon.outer());

    const auto & inners = polygon.inners();
    hash.update(inners.size());
    for (auto & inner : inners)
        hash_ring(inner);

    UInt128 res;
    hash.get128(res.low, res.high);
    return res;
}

}
