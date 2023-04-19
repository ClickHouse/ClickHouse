#pragma once

#include <base/types.h>
#include <Common/ThreadPool.h>
#include <Poco/Logger.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>

#include "PolygonDictionary.h"

#include <numeric>

namespace DB
{

namespace bg = boost::geometry;

using Coord = IPolygonDictionary::Coord;
using Point = IPolygonDictionary::Point;
using Polygon = IPolygonDictionary::Polygon;
using Ring = IPolygonDictionary::Ring;
using Box = bg::model::box<IPolygonDictionary::Point>;

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
    explicit SlabsPolygonIndex(const std::vector<Polygon> & polygons);

    /** Finds polygon id the same way as IPolygonIndex. */
    bool find(const Point & point, size_t & id) const;

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
    static std::vector<Coord> uniqueX(const std::vector<Polygon> & polygons);

    /** Builds index described above */
    void indexBuild(const std::vector<Polygon> & polygons);

    /** Auxiliary function for adding ring to the index */
    void indexAddRing(const Ring & ring, size_t polygon_id);

    Poco::Logger * log;

    /** Sorted distinct coordinates of all vertices */
    std::vector<Coord> sorted_x;
    std::vector<Edge> all_edges;

    /** This edges_index_tree stores all slabs with edges efficiently, using segment tree algorithm.
      * edges_index_tree[i] node combines segments from edges_index_tree[i*2] and edges_index_tree[i*2+1].
      * Every polygon's edge covers a segment of x coordinates, and can be added to this tree by
      *  placing it into O(log n) nodes of this tree.
      */
    std::vector<std::vector<EdgeLine>> edges_index_tree;
};

template <class ReturnCell>
class ICell
{
public:
    virtual ~ICell() = default;
    [[nodiscard]] virtual const ReturnCell * find(Coord x, Coord y) const = 0;
};

/** This leaf cell implementation simply stores the indexes of the intersections.
  * As an additional optimization, if a polygon covers the cell completely its index is stored in
  * the first_covered field and all following polygon indexes are discarded,
  * since they won't ever be useful.
  */
class FinalCell : public ICell<FinalCell>
{
public:
    explicit FinalCell(const std::vector<size_t> & polygon_ids_, const std::vector<Polygon> &, const Box &, bool is_last_covered_);
    std::vector<size_t> polygon_ids;
    size_t first_covered = kNone;

    static constexpr size_t kNone = -1;

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
    explicit FinalCellWithSlabs(const std::vector<size_t> & polygon_ids_, const std::vector<Polygon> & polygons_, const Box & box_, bool is_last_covered_);

    SlabsPolygonIndex index;
    std::vector<size_t> corresponding_ids;
    size_t first_covered = kNone;

    static constexpr size_t kNone = -1;

private:
    [[nodiscard]] const FinalCellWithSlabs * find(Coord x, Coord y) const override;
};

template <class ReturnCell>
class DividedCell : public ICell<ReturnCell>
{
public:
    explicit DividedCell(std::vector<std::unique_ptr<ICell<ReturnCell>>> children_): children(std::move(children_)) {}

    [[nodiscard]] const ReturnCell * find(Coord x, Coord y) const override
    {
        auto x_ratio = x * kSplit;
        auto y_ratio = y * kSplit;
        auto x_bin = static_cast<int>(x_ratio);
        auto y_bin = static_cast<int>(y_ratio);
        return children[y_bin + x_bin * kSplit]->find(x_ratio - x_bin, y_ratio - y_bin);
    }

    /** When a cell is split every side is split into kSplit pieces producing kSplit * kSplit equal smaller cells. */
    static constexpr size_t kSplit = 4;

private:
    std::vector<std::unique_ptr<ICell<ReturnCell>>> children;
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
    GridRoot(size_t min_intersections_, size_t max_depth_, const std::vector<Polygon> & polygons_):
            k_min_intersections(min_intersections_), k_max_depth(max_depth_), polygons(polygons_)
    {
        setBoundingBox();
        std::vector<size_t> order(polygons.size());
        std::iota(order.begin(), order.end(), 0);
        root = makeCell(min_x, min_y, max_x, max_y, order);
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
        return root->find((x - min_x) / (max_x - min_x), (y - min_y) / (max_y - min_y));
    }

    /** Until this depth is reached each row of cells is calculated concurrently in a new thread. */
    static constexpr size_t kMultiProcessingDepth = 2;

    /** A constant used to avoid errors with points falling on the boundaries of cells. */
    static constexpr Coord kEps = 1e-4;

private:
    std::unique_ptr<ICell<ReturnCell>> root = nullptr;
    Coord min_x = 0, min_y = 0;
    Coord max_x = 0, max_y = 0;
    const size_t k_min_intersections;
    const size_t k_max_depth;

    const std::vector<Polygon> & polygons;

    std::unique_ptr<ICell<ReturnCell>> makeCell(Coord current_min_x, Coord current_min_y, Coord current_max_x, Coord current_max_y, std::vector<size_t> possible_ids, size_t depth = 0)
    {
        auto current_box = Box(Point(current_min_x, current_min_y), Point(current_max_x, current_max_y));
        Polygon tmp_poly;
        bg::convert(current_box, tmp_poly);
        std::erase_if(possible_ids, [&](const auto id)
        {
            return !bg::intersects(current_box, polygons[id]);
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
        if (intersections <= k_min_intersections || depth++ == k_max_depth)
            return std::make_unique<ReturnCell>(possible_ids, polygons, current_box, covered);
        auto x_shift = (current_max_x - current_min_x) / DividedCell<ReturnCell>::kSplit;
        auto y_shift = (current_max_y - current_min_y) / DividedCell<ReturnCell>::kSplit;
        std::vector<std::unique_ptr<ICell<ReturnCell>>> children;
        children.resize(DividedCell<ReturnCell>::kSplit * DividedCell<ReturnCell>::kSplit);
        std::vector<ThreadFromGlobalPool> threads{};
        for (size_t i = 0; i < DividedCell<ReturnCell>::kSplit; current_min_x += x_shift, ++i)
        {
            auto handle_row = [this, &children, &y_shift, &x_shift, &possible_ids, &depth, i](Coord x, Coord y)
            {
                for (size_t j = 0; j < DividedCell<ReturnCell>::kSplit; y += y_shift, ++j)
                {
                    children[i * DividedCell<ReturnCell>::kSplit + j] = makeCell(x, y, x + x_shift, y + y_shift, possible_ids, depth);
                }
            };
            if (depth <= kMultiProcessingDepth)
                threads.emplace_back(handle_row, current_min_x, current_min_y);
            else
                handle_row(current_min_x, current_min_y);
        }
        for (auto & thread : threads)
            thread.join();
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
