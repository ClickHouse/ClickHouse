#pragma once

#include <base/types.h>
#include <Common/Exception.h>
#include <Common/iota.h>
#include <Common/ThreadPool.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/setThreadName.h>
#include <Poco/Logger.h>

#include <array>
#include <cmath>
#include <numeric>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>

#include <Dictionaries/PolygonDictionary.h>

namespace CurrentMetrics
{
    extern const Metric PolygonDictionaryThreads;
    extern const Metric PolygonDictionaryThreadsActive;
    extern const Metric PolygonDictionaryThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace bg = boost::geometry;

using Coord = IPolygonDictionary::Coord;
using Point = IPolygonDictionary::Point;
using Polygon = IPolygonDictionary::Polygon;
using Ring = IPolygonDictionary::Ring;
using Box = bg::model::box<IPolygonDictionary::Point>;

/// Boost.Geometry relate and overlay misclassify small polygons on Float32 points, so grid predicates and clips use Float64 boxes.
using PointF64 = bg::model::d2::point_xy<Float64>;
using BoxF64 = bg::model::box<PointF64>;
using PolygonF64 = bg::model::polygon<PointF64>;

inline BoxF64 toBoxF64(const Box & box, Float64 pad = 0)
{
    const auto & lo = box.min_corner();
    const auto & hi = box.max_corner();
    return BoxF64(
        PointF64(static_cast<Float64>(lo.x()) - pad, static_cast<Float64>(lo.y()) - pad),
        PointF64(static_cast<Float64>(hi.x()) + pad, static_cast<Float64>(hi.y()) + pad));
}

/** SlabsPolygonIndex builds index based on shooting ray down from point.
  * When this ray crosses odd number of edges in single polygon, point is considered inside.
  *
  * SlabsPolygonIndex divides plane into vertical slabs, separated by vertical lines going through all points.
  * For each slab, all edges falling in that slab are effectively stored.
  * For each find query, required slab is found with binary search, and result is computed
  * by iterating over all edges in that slab.
  */
class SlabsPolygonIndex
{
public:
    SlabsPolygonIndex() = default;

    /** Builds an index by splitting all edges with all points x coordinates. */
    explicit SlabsPolygonIndex(const VectorWithMemoryTracking<Polygon> & polygons);

    /** Finds polygon id the same way as IPolygonIndex. */
    bool find(const Point & point, size_t & id) const;

    /** Returns the bytes held by this index's owned buffers (sorted_x, all_edges,
      * and the segment-tree node vectors), including capacity overhead.
      */
    [[nodiscard]] size_t getBytesAllocated() const;

    /** Edge describes edge (adjacent points) of any polygon, and contains polygon's id.
      * Invariant here is first point has x not greater than second point.
      */
    struct Edge
    {
        Point l;
        Point r;
        size_t polygon_id;
        size_t edge_id;

        Coord k;
        Coord b;

        Edge(const Point & l, const Point & r, size_t polygon_id, size_t edge_id);

        static bool compareByLeftPoint(const Edge & a, const Edge & b);
        static bool compareByRightPoint(const Edge & a, const Edge & b);
    };

    /** EdgeLine is optimized version of Edge. */
    struct EdgeLine
    {
        explicit EdgeLine(const Edge & e): k(e.k), b(e.b), polygon_id(e.polygon_id) {}
        Coord k;
        Coord b;
        size_t polygon_id;
    };

private:
    /** Returns unique x coordinates among all points */
    static VectorWithMemoryTracking<Coord> uniqueX(const VectorWithMemoryTracking<Polygon> & polygons);

    /** Builds index described above */
    void indexBuild(const VectorWithMemoryTracking<Polygon> & polygons);

    /** Auxiliary function for adding ring to the index */
    void indexAddRing(const Ring & ring, size_t polygon_id);

    LoggerPtr log;

    /** Sorted distinct coordinates of all vertices */
    VectorWithMemoryTracking<Coord> sorted_x;
    VectorWithMemoryTracking<Edge> all_edges;

    /** This edges_index_tree stores all slabs with edges efficiently, using segment tree algorithm.
      * edges_index_tree[i] node combines segments from edges_index_tree[i*2] and edges_index_tree[i*2+1].
      * Every polygon's edge covers a segment of x coordinates, and can be added to this tree by
      *  placing it into O(log n) nodes of this tree.
      */
    VectorWithMemoryTracking<VectorWithMemoryTracking<EdgeLine>> edges_index_tree;
};

template <class ReturnCell>
class ICell
{
public:
    virtual ~ICell() = default;
    [[nodiscard]] virtual const ReturnCell * find(Coord x, Coord y) const = 0;
    /** Bytes held by this cell and (recursively) its children, including capacity overhead. */
    [[nodiscard]] virtual size_t getBytesAllocated() const = 0;
};

/** This leaf cell implementation simply stores the indexes of the intersections.
  * As an additional optimization, if a polygon covers the cell completely its index is stored in
  * the first_covered field and all following polygon indexes are discarded,
  * since they won't ever be useful.
  */
class FinalCell : public ICell<FinalCell>
{
public:
    explicit FinalCell(const VectorWithMemoryTracking<size_t> & polygon_ids_, const VectorWithMemoryTracking<Polygon> &, const Box &, bool is_last_covered_);
    VectorWithMemoryTracking<size_t> polygon_ids;
    size_t first_covered = kNone;

    static constexpr size_t kNone = -1;

    [[nodiscard]] size_t getBytesAllocated() const override;

private:
    [[nodiscard]] const FinalCell * find(Coord x, Coord y) const override;
};

/** This leaf cell implementation intersects the given polygons with the cell's box and builds a
  * slab index for the result.
  * Since the intersections can produce multiple polygons a vector of corresponding ids is stored.
  * If the slab index returned the id x for a query the correct polygon id is corresponding_ids[x].
  * As an additional optimization, if a polygon covers the cell completely its index stored in the
  * first_covered field and all following polygons are not used for building the slab index.
  */
class FinalCellWithSlabs : public ICell<FinalCellWithSlabs>
{
public:
    explicit FinalCellWithSlabs(const VectorWithMemoryTracking<size_t> & polygon_ids_, const VectorWithMemoryTracking<Polygon> & polygons_, const Box & box_, bool is_last_covered_);

    SlabsPolygonIndex index;
    VectorWithMemoryTracking<size_t> corresponding_ids;
    size_t first_covered = kNone;

    static constexpr size_t kNone = -1;

    [[nodiscard]] size_t getBytesAllocated() const override;

private:
    [[nodiscard]] const FinalCellWithSlabs * find(Coord x, Coord y) const override;
};

template <class ReturnCell>
class DividedCell : public ICell<ReturnCell>
{
public:
    explicit DividedCell(VectorWithMemoryTracking<std::unique_ptr<ICell<ReturnCell>>> children_): children(std::move(children_)) {}

    /// Returns the child index along one side for `ratio` in [0, 1] and rescales `ratio` to that child.
    static int route(Coord & ratio)
    {
        ratio *= kSplit;
        auto bin = static_cast<int>(ratio);
        /// ratio is 1 for points very close to the max side.
        if (bin == kSplit)
            --bin;
        ratio -= static_cast<Coord>(bin);
        return bin;
    }

    [[nodiscard]] const ReturnCell * find(Coord x, Coord y) const override
    {
        auto x_bin = route(x);
        auto y_bin = route(y);
        return children[y_bin + x_bin * kSplit]->find(x, y);
    }

    [[nodiscard]] size_t getBytesAllocated() const override
    {
        size_t total = sizeof(*this) + children.capacity() * sizeof(std::unique_ptr<ICell<ReturnCell>>);
        for (const auto & child : children)
            if (child)
                total += child->getBytesAllocated();
        return total;
    }

    /** When a cell is split every side is split into kSplit pieces producing kSplit * kSplit equal smaller cells. */
    static constexpr size_t kSplit = 4;

private:
    VectorWithMemoryTracking<std::unique_ptr<ICell<ReturnCell>>> children;
};

/** A recursively built grid containing information about polygons intersecting each cell.
  * The starting cell is the bounding box of the given polygons which are passed by reference.
  * For every cell a vector of indices of intersecting polygons is calculated, in the order originally provided upon
  * construction. A cell is recursively split into kSplit * kSplit equal cells up to the point where the cell
  * intersects a small enough number of polygons or the maximum allowed depth is exceeded.
  * Both of these parameters are set in the constructor.
  * Once these conditions are fulfilled some index is built and stored in the leaf cells.
  * The ReturnCell class passed in the template parameter is responsible for this.
  */
template <class ReturnCell>
class GridRoot : public ICell<ReturnCell>
{
public:
    GridRoot(size_t min_intersections_, size_t max_depth_, const VectorWithMemoryTracking<Polygon> & polygons_):
            k_min_intersections(min_intersections_), k_max_depth(max_depth_), polygons(polygons_)
    {
        setBoundingBox();
        VectorWithMemoryTracking<size_t> order(polygons.size());
        iota(order.data(), order.size(), size_t(0));

        /// Single pool shared across the whole recursion. `makeCell` only schedules work up to `kMultiProcessingDepth`;
        /// constructing a pool per recursive call would create and destroy threads for the deeper levels that never
        /// enqueue anything.
        ThreadPool pool(
            CurrentMetrics::PolygonDictionaryThreads,
            CurrentMetrics::PolygonDictionaryThreadsActive,
            CurrentMetrics::PolygonDictionaryThreadsScheduled,
            128);

        root = makeCell(min_x, min_y, max_x, max_y, order, 0, pool);
    }

    /** Retrieves the cell containing a given point.
      * A null pointer is returned when the point falls outside the grid.
      */
    [[nodiscard]] const ReturnCell * find(Coord x, Coord y) const override
    {
        if (x < min_x || x >= max_x)
            return nullptr;
        if (y < min_y || y >= max_y)
            return nullptr;
        return root->find(toRatio(x, min_x, max_x), toRatio(y, min_y, max_y));
    }

    [[nodiscard]] size_t getBytesAllocated() const override
    {
        return root ? root->getBytesAllocated() : 0;
    }

    /** Until this depth is reached each row of cells is calculated concurrently in a new thread. */
    static constexpr size_t kMultiProcessingDepth = 2;

    /** A constant used to avoid errors with points falling on the boundaries of cells. */
    static constexpr Coord kEps = 1e-4f;

private:
    std::unique_ptr<ICell<ReturnCell>> root;
    Coord min_x = 0, min_y = 0;
    Coord max_x = 0, max_y = 0;
    const size_t k_min_intersections;
    const size_t k_max_depth;

    const VectorWithMemoryTracking<Polygon> & polygons;

    static Coord toRatio(Coord v, Coord lo, Coord hi) { return (v - lo) / (hi - lo); }

    /// `find` accepts no point when a side of the bounding box is zero.
    [[nodiscard]] bool hasZeroSide() const { return min_x == max_x || min_y == max_y; }

    /// The child index along one side that `find` picks for `v` in the `level`-th divided cell of its path (the root is 1).
    static size_t routedBin(Coord v, Coord lo, Coord hi, size_t level)
    {
        Coord ratio = toRatio(v, lo, hi);
        int bin = 0;
        for (size_t i = 0; i < level; ++i)
            bin = DividedCell<ReturnCell>::route(ratio);
        return static_cast<size_t>(bin);
    }

    /// bounds[i] is the smallest coordinate in [lo, hi) that `find` routes to a child >= i, so each child's box contains its points.
    static std::array<Coord, DividedCell<ReturnCell>::kSplit + 1> splitSide(Coord lo, Coord hi, Coord side_min, Coord side_max, size_t level)
    {
        std::array<Coord, DividedCell<ReturnCell>::kSplit + 1> bounds{};
        bounds[0] = lo;
        bounds[DividedCell<ReturnCell>::kSplit] = hi;
        for (size_t bin = 1; bin < DividedCell<ReturnCell>::kSplit; ++bin)
        {
            /// Binary search, `routedBin` is monotone in the coordinate.
            Coord below = bounds[bin - 1];
            Coord above = hi;
            if (routedBin(below, side_min, side_max, level) >= bin)
                above = below;
            while (below < above)
            {
                Coord mid = std::midpoint(below, above);
                if (mid == below || mid == above)
                    break;
                if (routedBin(mid, side_min, side_max, level) >= bin)
                    above = mid;
                else
                    below = mid;
            }
            bounds[bin] = above;
        }
        return bounds;
    }

    std::unique_ptr<ICell<ReturnCell>> makeCell(Coord current_min_x, Coord current_min_y, Coord current_max_x, Coord current_max_y, VectorWithMemoryTracking<size_t> possible_ids, size_t depth, ThreadPool & pool)
    {
        auto current_box = Box(Point(current_min_x, current_min_y), Point(current_max_x, current_max_y));
        const BoxF64 current_box_f64 = toBoxF64(current_box);
        PolygonF64 tmp_poly;
        bg::convert(current_box_f64, tmp_poly);
        std::erase_if(possible_ids, [&](const auto id)
        {
            return !bg::intersects(current_box_f64, polygons[id]);
        });
        int covered = 0;
#ifndef __clang_analyzer__ /// Triggers a warning in boost geometry.
        auto it = std::find_if(possible_ids.begin(), possible_ids.end(), [&](const auto id)
        {
            return bg::covered_by(tmp_poly, polygons[id]);
        });
        if (it != possible_ids.end())
        {
            possible_ids.erase(it + 1, possible_ids.end());
            covered = 1;
        }
#endif
        size_t intersections = possible_ids.size() - covered;
        if (intersections <= k_min_intersections || depth++ == k_max_depth || hasZeroSide())
            return std::make_unique<ReturnCell>(possible_ids, polygons, current_box, covered);
        if (!std::isfinite(max_x - min_x) || !std::isfinite(max_y - min_y))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "PolygonDictionary polygons are too far apart for a grid index: a side of their bounding box exceeds the Float32 range");
        /// `depth` is now this cell's level, the root's is 1.
        const auto x_bounds = splitSide(current_min_x, current_max_x, min_x, max_x, depth);
        const auto y_bounds = splitSide(current_min_y, current_max_y, min_y, max_y, depth);
        VectorWithMemoryTracking<std::unique_ptr<ICell<ReturnCell>>> children;
        children.resize(DividedCell<ReturnCell>::kSplit * DividedCell<ReturnCell>::kSplit);

        ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::POLYGON_DICT_LOAD);
        for (size_t i = 0; i < DividedCell<ReturnCell>::kSplit; ++i)
        {
            /// Capturing by reference is fine, all variables outlive runner
            auto handle_row = [this, &children, &x_bounds, &y_bounds, &possible_ids, &pool, depth, i]()
            {
                for (size_t j = 0; j < DividedCell<ReturnCell>::kSplit; ++j)
                {
                    children[i * DividedCell<ReturnCell>::kSplit + j] = makeCell(
                        x_bounds[i], y_bounds[j], x_bounds[i + 1], y_bounds[j + 1], possible_ids, depth, pool);
                }
            };
            if (depth <= kMultiProcessingDepth)
                runner.enqueueAndKeepTrack(std::move(handle_row));
            else
                handle_row();
        }
        runner.waitForAllToFinishAndRethrowFirstError();
        return std::make_unique<DividedCell<ReturnCell>>(std::move(children));
    }

    void setBoundingBox()
    {
        bool first = true;
        std::for_each(polygons.begin(), polygons.end(), [&](const auto & polygon)
        {
            bg::for_each_point(polygon, [&](const Point & point)
            {
                auto x = point.x();
                auto y = point.y();
                if (first || x < min_x)
                    min_x = x;
                if (first || x > max_x)
                    max_x = x;
                if (first || y < min_y)
                    min_y = y;
                if (first || y > max_y)
                    max_y = y;
                if (first)
                    first = false;
            });
        });
        max_x += kEps;
        max_y += kEps;
    }
};

}
