#pragma once

#include <base/demangle.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Core/Defines.h>
#include <base/TypeLists.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <Common/NaNUtils.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/range.h>

/// Warning in boost::geometry during template strategy substitution.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#include <boost/geometry.hpp>
#pragma clang diagnostic pop

#include <boost/geometry/geometries/multi_polygon.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>
#include <boost/geometry/geometries/segment.hpp>
#include <boost/geometry/index/rtree.hpp>

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
    extern const int BAD_ARGUMENTS;
}

namespace bgi = boost::geometry::index;

template <typename Polygon>
UInt64 getPolygonAllocatedBytes(const Polygon & polygon)
{
    UInt64 size = 0;

    using RingType = typename Polygon::ring_type;
    using ValueType = typename RingType::value_type;

    auto size_of_ring = [](const RingType & ring) { return sizeof(ring) + ring.capacity() * sizeof(ValueType); };

    size += size_of_ring(polygon.outer());

    const auto & inners = polygon.inners();
    size += sizeof(inners) + inners.capacity() * sizeof(RingType);
    for (auto & inner : inners)
        size += size_of_ring(inner);

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

    inline bool contains(CoordinateType x, CoordinateType y) const
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

/// Optimized algorithm with R-tree of bounding boxes of polygons.
template <typename PointInPolygonImpl>
class PointInMultiPolygonRTree
{
public:
    using Point = typename PointInPolygonImpl::Point;
    using Polygon = typename PointInPolygonImpl::Polygon;
    using Box = typename PointInPolygonImpl::Box;
    using MultiPolygon = boost::geometry::model::multi_polygon<Polygon>;
    using CoordinateType = decltype(std::declval<Point>().x());

    using PolyBox = std::pair<Box, std::size_t>;

    /// Max children per R-tree node before splitting.
    /// — Larger value -> shallower tree, fewer node visits per query, but each
    ///   visit scans a longer list and node splits are more expensive.
    /// ─ Smaller value -> deeper tree, more pointer hops per query, yet each hop
    ///   touches fewer boxes and nodes fit cache lines better
    /// ─ Default value is 16, which is a good compromise for most cases.
    static constexpr std::size_t max_elements_per_rtree_node = 16;

    explicit PointInMultiPolygonRTree(const MultiPolygon & multi_polygon, UInt16 grid_size_ = 8)
    {
        build(multi_polygon, grid_size_);
    }

    /// O(log N + K) where K = polygons that contain the point.
    bool contains(CoordinateType x, CoordinateType y) const
    {
        if (has_empty_bound || !isFinite(x) || !isFinite(y))
            return false;

        for (auto it = rtree.qbegin(bgi::contains(Point(x, y))); it != rtree.qend(); ++it)
        {
            if (polygon_impls[it->second].contains(x, y))
                return true;
        }

        return false;
    }

    bool hasEmptyBound() const { return has_empty_bound; }

    UInt64 getAllocatedBytes() const
    {
        UInt64 size = sizeof(*this) + polygon_impls.capacity() * sizeof(PointInPolygonImpl) + rtree.size() * sizeof(PolyBox);

        for (const auto & impl : polygon_impls)
            size += impl.getAllocatedBytes();

        return size;
    }

private:
    VectorWithMemoryTracking<PointInPolygonImpl> polygon_impls;

    /// Boost.Geometry split policy choices
    ///   linear     — quick to build, queries slowest
    ///   quadratic  — build cost medium, queries medium
    ///   rstar      — build slowest, queries fastest
    /// With the default block size, the quadratic split was the fastest in performance tests, so we use it.
    using RTree = bgi::rtree<PolyBox, bgi::quadratic<max_elements_per_rtree_node>>;
    RTree rtree;

    /// Only becomes true if all polygons have empty bounding box.
    bool has_empty_bound = false;

    /// The input multipolygon is consumed only to build the per-polygon impls and the R-tree; it is
    /// intentionally not retained (contains() needs only the impls and the tree), which avoids keeping
    /// a second full copy of every vertex alongside the per-polygon copies in polygon_impls.
    void build(const MultiPolygon & multi_polygon, UInt16 grid_size)
    {
        polygon_impls.reserve(multi_polygon.size());

        VectorWithMemoryTracking<PolyBox> boxes; // bulk-build container
        boxes.reserve(multi_polygon.size());

        std::size_t idx = 0;
        for (const auto & poly : multi_polygon)
        {
            polygon_impls.emplace_back(poly, grid_size);

            if (!polygon_impls.back().hasEmptyBound())
            {
                Box box = boost::geometry::return_envelope<Box>(poly);
                boxes.emplace_back(box, idx);
            }

            ++idx;
        }

        /// All polygons have empty bounding boxes; skip R-tree building
        /// and mark the multipolygon as having an empty bound.
        if (boxes.empty())
        {
            has_empty_bound = true;
            return;
        }

        rtree = RTree(boxes.begin(), boxes.end());
    }
};

/// Optimized algorithm with bounding box and grid.
template <typename TCoordinateType>
class PointInPolygonWithGrid
{
public:
    using CoordinateType = TCoordinateType;
    using Point = boost::geometry::model::d2::point_xy<CoordinateType>;
    /// Counter-Clockwise ordering.
    using Polygon = boost::geometry::model::polygon<Point, false>;
    using MultiPolygon = boost::geometry::model::multi_polygon<Polygon>;
    using Box = boost::geometry::model::box<Point>;
    using Ring = typename Polygon::ring_type;

    explicit PointInPolygonWithGrid(const Polygon & polygon_, UInt16 grid_size_ = 8)
        : grid_size(std::max<UInt16>(1, grid_size_)), polygon(polygon_)
    {
        buildGrid();
    }

    /// True if bound box is empty.
    bool hasEmptyBound() const { return has_empty_bound; }

    UInt64 getAllocatedBytes() const;

    bool contains(CoordinateType x, CoordinateType y) const;

private:
    enum class CellType : uint8_t
    {
        inner,                                  /// The cell is completely inside polygon.
        outer,                                  /// The cell is completely outside of polygon.
        singleLine,                             /// The cell is split to inner/outer part by a single line.
        pairOfLinesSingleConvexPolygon,         /// The cell is split to inner/outer part by a polyline of two sections and inner part is convex.
        pairOfLinesSingleNonConvexPolygons,     /// The cell is split to inner/outer part by a polyline of two sections and inner part is non convex.
        pairOfLinesDifferentPolygons,           /// The cell is spliited by two lines to three different parts.
        complexPolygon                          /// Generic case.
    };

    struct HalfPlane
    {
        /// Left closed half-plane of the line through (x0, y0) with direction (dx, dy), evaluated relative to (x0, y0).
        CoordinateType x0;
        CoordinateType y0;
        CoordinateType dx;
        CoordinateType dy;

        HalfPlane() = default;

        /// Take left half-plane.
        HalfPlane(const Point & from, const Point & to)
        {
            x0 = from.x();
            y0 = from.y();
            dx = to.x() - from.x();
            dy = to.y() - from.y();
        }

        /// Inner part of the HalfPlane is the left side of initialized vector.
        bool contains(CoordinateType x, CoordinateType y) const { return dx * (y - y0) - dy * (x - x0) >= 0; }
    };

    struct Cell
    {
        static const int max_stored_half_planes = 2;

        HalfPlane half_planes[max_stored_half_planes];
        size_t index_of_inner_polygon{};
        CellType type;
    };

    /// Edge ring[edge] -> ring[edge + 1] of the polygon that enters a cell.
    struct Crossing
    {
        const Ring * ring = nullptr;
        size_t edge = 0;
        Point middle{};
    };

    const UInt16 grid_size;

    Polygon polygon;
    VectorWithMemoryTracking<Cell> cells;
    VectorWithMemoryTracking<MultiPolygon> polygons;

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
    T getCellIndex(T row, T col) const { return row * grid_size + col; }

    /// Complex case. Will check intersection directly.
    inline void addComplexPolygonCell(size_t index, const Box & box);

    /// No polygon edge enters the cell: the cell is inside or outside as its centre.
    inline void addCell(size_t index, const Box & empty_box);

    /// True if the segment has a point strictly inside the box (an endpoint, or the midpoint of its part inside the closed
    /// box when that part has positive length); the point is returned in `middle`.
    static bool crossesBox(const Point & from, const Point & to, const Box & box, Point & middle);

    /// Sutherland-Hodgman clip of a closed ring by a box (closed or empty result). The winding number
    /// of every point inside the box is unchanged.
    static Ring clipRing(const Ring & ring, const Box & box);
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

    cell_width = (max_corner.x() - min_corner.x()) / grid_size;
    cell_height = (max_corner.y() - min_corner.y()) / grid_size;

    /// Negative spans come from the inverse box boost::geometry::envelope leaves for an empty
    /// geometry. NaN is not <= 0, so it reaches the finiteness check below.
    if (cell_width <= 0 || cell_height <= 0)
    {
        has_empty_bound = true;
        return;
    }

    /// 1 / +-inf is +-0.0, which is finite: the scales below would pass their own check.
    if (!isFinite(cell_width) || !isFinite(cell_height))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Polygon is not valid: bounding box is unbounded");

    x_scale = 1 / cell_width;
    y_scale = 1 / cell_height;
    x_shift = -min_corner.x();
    y_shift = -min_corner.y();

    if (!(isFinite(x_scale)
        && isFinite(y_scale)
        && isFinite(x_shift)
        && isFinite(y_shift)
        && isFinite(grid_size)))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Polygon is not valid: bounding box is unbounded");
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
        CoordinateType y_min = min_corner.y() + static_cast<CoordinateType>(row) * cell_height;
        CoordinateType y_max = min_corner.y() + static_cast<CoordinateType>(row + 1) * cell_height;

        for (size_t col = 0; col < grid_size; ++col)
        {
            CoordinateType x_min = min_corner.x() + static_cast<CoordinateType>(col) * cell_width;
            CoordinateType x_max = min_corner.x() + static_cast<CoordinateType>(col + 1) * cell_width;
            Box cell_box(Point(x_min, y_min), Point(x_max, y_max));

            size_t cell_index = getCellIndex(row, col);
            auto & cell = cells[cell_index];

            Crossing crossings[3];
            size_t num_crossings = 0;
            auto add_crossings = [&](const Ring & ring)
            {
                for (size_t i = 0; i + 1 < ring.size() && num_crossings < std::size(crossings); ++i)
                {
                    Point middle;
                    if (crossesBox(ring[i], ring[i + 1], cell_box, middle))
                        crossings[num_crossings++] = {&ring, i, middle};
                }
            };
            add_crossings(polygon.outer());
            for (const auto & inner : polygon.inners())
                add_crossings(inner);

            /// Rings are corrected, so the interior is on the left of every edge.
            auto half_plane = [](const Crossing & crossing)
            {
                return HalfPlane((*crossing.ring)[crossing.edge], (*crossing.ring)[crossing.edge + 1]);
            };

            if (num_crossings == 0)
            {
                addCell(cell_index, cell_box);
            }
            else if (num_crossings == 1)
            {
                cell.type = CellType::singleLine;
                cell.half_planes[0] = half_plane(crossings[0]);
            }
            else if (num_crossings == 2)
            {
                const Crossing & first = crossings[0];
                const Crossing & second = crossings[1];
                cell.half_planes[0] = half_plane(first);
                cell.half_planes[1] = half_plane(second);

                size_t edges_in_ring = first.ring->size() - 1;
                bool first_then_second = first.ring == second.ring && (first.edge + 1) % edges_in_ring == second.edge;
                bool second_then_first = first.ring == second.ring && (second.edge + 1) % edges_in_ring == first.edge;

                if (first_then_second || second_then_first)
                {
                    const Crossing & in = first_then_second ? first : second;
                    const Crossing & out = first_then_second ? second : first;
                    const Ring & ring = *in.ring;
                    Point in_direction(ring[in.edge + 1].x() - ring[in.edge].x(), ring[in.edge + 1].y() - ring[in.edge].y());
                    Point out_direction(ring[out.edge + 1].x() - ring[out.edge].x(), ring[out.edge + 1].y() - ring[out.edge].y());
                    bool left_turn = in_direction.x() * out_direction.y() - in_direction.y() * out_direction.x() >= 0;
                    cell.type = left_turn ? CellType::pairOfLinesSingleConvexPolygon : CellType::pairOfLinesSingleNonConvexPolygons;
                }
                else
                {
                    bool strip_is_inner = cell.half_planes[0].contains(second.middle.x(), second.middle.y());
                    cell.type = strip_is_inner ? CellType::pairOfLinesSingleConvexPolygon : CellType::pairOfLinesDifferentPolygons;
                }
            }
            else
            {
                addComplexPolygonCell(cell_index, cell_box);
            }
        }
    }
}

template <typename CoordinateType>
bool PointInPolygonWithGrid<CoordinateType>::contains(CoordinateType x, CoordinateType y) const
{
    if (has_empty_bound)
        return false;

    if (!isFinite(x) || !isFinite(y))
        return false;

    CoordinateType float_row = (y + y_shift) * y_scale;
    CoordinateType float_col = (x + x_shift) * x_scale;

    if (float_row < 0 || float_row > grid_size)
        return false;
    if (float_col < 0 || float_col > grid_size)
        return false;

    int row = std::min<int>(static_cast<int>(float_row), grid_size - 1);
    int col = std::min<int>(static_cast<int>(float_col), grid_size - 1);

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
}


template <typename CoordinateType>
bool PointInPolygonWithGrid<CoordinateType>::crossesBox(
        const Point & from, const Point & to, const Box & box, Point & middle)
{
    auto strictly_inside = [&box](const Point & point)
    {
        return point.x() > box.min_corner().x() && point.x() < box.max_corner().x()
            && point.y() > box.min_corner().y() && point.y() < box.max_corner().y();
    };

    for (const Point * endpoint : {&from, &to})
    {
        if (strictly_inside(*endpoint))
        {
            middle = *endpoint;
            return true;
        }
    }

    CoordinateType dx = to.x() - from.x();
    CoordinateType dy = to.y() - from.y();

    /// Liang-Barsky: the part inside the box is from + t * (dx, dy) for t in [t_min, t_max].
    const CoordinateType p[4] = {-dx, dx, -dy, dy};
    const CoordinateType q[4] = {
        from.x() - box.min_corner().x(),
        box.max_corner().x() - from.x(),
        from.y() - box.min_corner().y(),
        box.max_corner().y() - from.y()};

    CoordinateType t_min = 0;
    CoordinateType t_max = 1;
    for (size_t k = 0; k < 4; ++k)
    {
        if (p[k] == 0)
        {
            if (q[k] < 0)
                return false;
            continue;
        }

        CoordinateType t = q[k] / p[k];
        if (p[k] < 0)
            t_min = std::max(t_min, t);
        else
            t_max = std::min(t_max, t);
    }

    if (!(t_min < t_max))
        return false;

    CoordinateType t = (t_min + t_max) / 2;
    middle = Point(from.x() + t * dx, from.y() + t * dy);

    return strictly_inside(middle);
}

template <typename CoordinateType>
typename PointInPolygonWithGrid<CoordinateType>::Ring
PointInPolygonWithGrid<CoordinateType>::clipRing(const Ring & ring, const Box & box)
{
    if (ring.empty())
        return {};

    Ring points(ring.begin(), ring.end() - 1);
    Ring clipped;

    auto clip = [&](size_t axis, CoordinateType bound, bool keep_greater)
    {
        auto coordinate = [axis](const Point & point) { return axis == 0 ? point.x() : point.y(); };
        auto inside = [&](const Point & point) { return keep_greater ? coordinate(point) >= bound : coordinate(point) <= bound; };

        clipped.clear();
        for (size_t i = 0; i < points.size(); ++i)
        {
            const Point & from = points[i];
            const Point & to = points[(i + 1) % points.size()];

            if (inside(from))
                clipped.push_back(from);

            if (inside(from) != inside(to))
            {
                CoordinateType t = (bound - coordinate(from)) / (coordinate(to) - coordinate(from));
                Point crossing(from.x() + t * (to.x() - from.x()), from.y() + t * (to.y() - from.y()));
                if (axis == 0)
                    crossing.x(bound);
                else
                    crossing.y(bound);
                clipped.push_back(crossing);
            }
        }

        points.swap(clipped);
    };

    clip(0, box.min_corner().x(), true);
    clip(0, box.max_corner().x(), false);
    clip(1, box.min_corner().y(), true);
    clip(1, box.max_corner().y(), false);

    if (points.size() < 3)
        return {};

    points.push_back(points.front());
    return points;
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

    Polygon clipped;
    clipped.outer() = clipRing(polygon.outer(), box_with_eps_bound);
    for (const auto & inner : polygon.inners())
    {
        Ring clipped_inner = clipRing(inner, box_with_eps_bound);
        if (!clipped_inner.empty())
            clipped.inners().push_back(std::move(clipped_inner));
    }

    polygons.emplace_back();
    polygons.back().push_back(std::move(clipped));
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


/// Algorithms.

template <typename T, typename U, typename PointInPolygonImpl>
ColumnPtr pointInPolygon(const ColumnVector<T> & x, const ColumnVector<U> & y, PointInPolygonImpl && impl)
{
    auto size = x.size();

    if (impl.hasEmptyBound())
        return ColumnVector<UInt8>::create(size, static_cast<UInt8>(0));

    auto result = ColumnVector<UInt8>::create(size);
    auto & data = result->getData();

    const auto & x_data = x.getData();
    const auto & y_data = y.getData();

    using CoordinateType = typename std::decay_t<PointInPolygonImpl>::CoordinateType;
    for (auto i : collections::range(0, size))
        data[i] = static_cast<UInt8>(impl.contains(static_cast<CoordinateType>(x_data[i]), static_cast<CoordinateType>(y_data[i])));

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
            return pointInPolygon(x, *column, std::forward<PointInPolygonImpl>(impl));
        return CallPointInPolygon<Types ...>::call(x, y, std::forward<PointInPolygonImpl>(impl));
    }

    template <typename PointInPolygonImpl>
    static ColumnPtr call(const IColumn & x, const IColumn & y, PointInPolygonImpl && impl)
    {
        using Impl = TypeListChangeRoot<CallPointInPolygon, TypeListNativeNumber>;
        if (auto column = typeid_cast<const ColumnVector<Type> *>(&x))
            return Impl::call(*column, y, std::forward<PointInPolygonImpl>(impl));
        return CallPointInPolygon<Types ...>::call(x, y, std::forward<PointInPolygonImpl>(impl));
    }
};

template <>
struct CallPointInPolygon<>
{
    template <typename T, typename PointInPolygonImpl>
    static ColumnPtr call(const ColumnVector<T> &, const IColumn & y, PointInPolygonImpl &&)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown numeric column type: {}", demangle(typeid(y).name()));
    }

    template <typename PointInPolygonImpl>
    static ColumnPtr call(const IColumn & x, const IColumn &, PointInPolygonImpl &&)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown numeric column type: {}", demangle(typeid(x).name()));
    }
};

template <typename PointInPolygonImpl>
NO_INLINE ColumnPtr pointInPolygon(const IColumn & x, const IColumn & y, PointInPolygonImpl && impl)
{
    using Impl = TypeListChangeRoot<CallPointInPolygon, TypeListNativeNumber>;
    return Impl::call(x, y, impl);
}
}
