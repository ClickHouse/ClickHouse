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
#include <boost/geometry/strategy/cartesian/side_robust.hpp>

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
    };

    const UInt16 grid_size;

    Polygon polygon;
    VectorWithMemoryTracking<Cell> cells;
    VectorWithMemoryTracking<Polygon> polygons;

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

    /// True if the segment has a point strictly inside the box. Without `exact_sides`, true if their bounding boxes overlap.
    static bool crossesBox(const Point & from, const Point & to, const Box & box, bool exact_sides);

    /// Exact sign of the orientation of (a, b, c): positive if c is on the left of a -> b.
    static int side(const Point & a, const Point & b, const Point & c);

    /// Crossing number test, as for non-constant polygons: inside the outer ring and not inside any hole.
    static bool isInside(const Polygon & polygon, CoordinateType x, CoordinateType y);

    /// Sutherland-Hodgman clip of a closed ring by a box (closed or empty result). The winding number
    /// of every point inside the box is unchanged.
    static Ring clipRing(const Ring & ring, const Box & box);
};


template <typename CoordinateType>
UInt64 PointInPolygonWithGrid<CoordinateType>::getAllocatedBytes() const
{
    UInt64 size = sizeof(*this);

    size += cells.capacity() * sizeof(Cell);
    size += polygons.capacity() * sizeof(Polygon);
    size += getPolygonAllocatedBytes(polygon);

    for (const auto & elem : polygons)
        size += getPolygonAllocatedBytes(elem);

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

    /// side() is exact unless products of coordinate differences overflow or underflow.
    bool exact_sides = true;
    auto check_ring = [&](const Ring & ring)
    {
        for (const Point & point : ring)
            for (CoordinateType coordinate : {point.x(), point.y()})
                if (coordinate != 0 && !(std::abs(coordinate) >= 0x1p-300 && std::abs(coordinate) <= 0x1p300))
                    exact_sides = false;
    };
    check_ring(polygon.outer());
    for (const auto & inner : polygon.inners())
        check_ring(inner);

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
                    if (crossesBox(ring[i], ring[i + 1], cell_box, exact_sides))
                        crossings[num_crossings++] = {&ring, i};
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
            else if (num_crossings == 1 && exact_sides)
            {
                cell.type = CellType::singleLine;
                cell.half_planes[0] = half_plane(crossings[0]);
            }
            else if (num_crossings == 2 && exact_sides)
            {
                const Crossing & first = crossings[0];
                const Crossing & second = crossings[1];
                cell.half_planes[0] = half_plane(first);
                cell.half_planes[1] = half_plane(second);
                const Point & first_from = (*first.ring)[first.edge];
                const Point & first_to = (*first.ring)[first.edge + 1];
                const Point & second_from = (*second.ring)[second.edge];
                const Point & second_to = (*second.ring)[second.edge + 1];

                size_t edges_in_ring = first.ring->size() - 1;
                bool first_then_second = first.ring == second.ring && (first.edge + 1) % edges_in_ring == second.edge;
                bool second_then_first = first.ring == second.ring && (second.edge + 1) % edges_in_ring == first.edge;

                if (first_then_second || second_then_first)
                {
                    const Crossing & in = first_then_second ? first : second;
                    const Crossing & out = first_then_second ? second : first;
                    const Ring & ring = *in.ring;
                    bool left_turn = side(ring[in.edge], ring[out.edge], ring[out.edge + 1]) >= 0;
                    cell.type = left_turn ? CellType::pairOfLinesSingleConvexPolygon : CellType::pairOfLinesSingleNonConvexPolygons;
                }
                else
                {
                    /// The strip between the edges is inner if it is on the left of them. Unless the edges intersect, one
                    /// of them is strictly on one side of the line of the other.
                    int second_side = side(first_from, first_to, second_from) + side(first_from, first_to, second_to);
                    int first_side = side(second_from, second_to, first_from) + side(second_from, second_to, first_to);
                    if (std::abs(second_side) == 2 || std::abs(first_side) == 2)
                    {
                        bool strip_is_inner = std::abs(second_side) == 2 ? second_side > 0 : first_side > 0;
                        cell.type = strip_is_inner ? CellType::pairOfLinesSingleConvexPolygon : CellType::pairOfLinesDifferentPolygons;
                    }
                    else
                        addComplexPolygonCell(cell_index, cell_box);
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
            return isInside(polygons[cell.index_of_inner_polygon], x, y);
    }
}


template <typename CoordinateType>
bool PointInPolygonWithGrid<CoordinateType>::crossesBox(
        const Point & from, const Point & to, const Box & box, bool exact_sides)
{
    const Point & low = box.min_corner();
    const Point & high = box.max_corner();

    if (std::max(from.x(), to.x()) <= low.x() || std::min(from.x(), to.x()) >= high.x()
        || std::max(from.y(), to.y()) <= low.y() || std::min(from.y(), to.y()) >= high.y())
        return false;

    /// A zero-length segment is strictly inside here.
    if (!exact_sides || (from.x() == to.x() && from.y() == to.y()))
        return true;

    /// Otherwise the segment enters the open box if and only if its line strictly separates two corners.
    bool left = false;
    bool right = false;
    for (const Point & corner : {low, Point(high.x(), low.y()), high, Point(low.x(), high.y())})
    {
        int corner_side = side(from, to, corner);
        left |= corner_side > 0;
        right |= corner_side < 0;
    }

    return left && right;
}

template <typename CoordinateType>
int PointInPolygonWithGrid<CoordinateType>::side(const Point & a, const Point & b, const Point & c)
{
    using Side = boost::geometry::strategy::side::side_robust<CoordinateType, boost::geometry::strategy::side::fp_equals_policy>;
    return Side::apply(a, b, c);
}

template <typename CoordinateType>
bool PointInPolygonWithGrid<CoordinateType>::isInside(const Polygon & poly, CoordinateType x, CoordinateType y)
{
    auto inside_ring = [x, y](const Ring & ring)
    {
        bool inside = false;
        for (size_t i = 0, j = ring.size() - 1; i < ring.size(); j = i++)
        {
            const Point & a = ring[i];
            const Point & b = ring[j];
            if ((a.y() > y) != (b.y() > y) && x < (b.x() - a.x()) * (y - a.y()) / (b.y() - a.y()) + a.x())
                inside = !inside;
        }
        return inside;
    };

    if (!inside_ring(poly.outer()))
        return false;

    for (const auto & inner : poly.inners())
        if (inside_ring(inner))
            return false;

    return true;
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

    polygons.push_back(std::move(clipped));
}

template <typename CoordinateType>
void PointInPolygonWithGrid<CoordinateType>::addCell(
        size_t index, const PointInPolygonWithGrid<CoordinateType>::Box & empty_box)
{
    const auto & min_corner = empty_box.min_corner();
    const auto & max_corner = empty_box.max_corner();

    Point center((min_corner.x() + max_corner.x()) / 2, (min_corner.y() + max_corner.y()) / 2);

    if (isInside(polygon, center.x(), center.y()))
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
