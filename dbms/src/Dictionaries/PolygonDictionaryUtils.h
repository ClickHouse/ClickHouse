#pragma once

#include <Core/Types.h>
#include <Poco/Logger.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>

#include "PolygonDictionary.h"

namespace DB
{

namespace bg = boost::geometry;

using Point = IPolygonDictionary::Point;
using Polygon = IPolygonDictionary::Polygon;
using Box = bg::model::box<IPolygonDictionary::Point>;

class FinalCell;

class ICell
{
public:
    virtual ~ICell() = default;
    [[nodiscard]] virtual const FinalCell * find(Float64 x, Float64 y) const = 0;
};

class DividedCell : public ICell
{
public:
    explicit DividedCell(std::vector<std::unique_ptr<ICell>> children_);
    [[nodiscard]] const FinalCell * find(Float64 x, Float64 y) const override;

private:
    std::vector<std::unique_ptr<ICell>> children;
};

class FinalCell : public ICell
{
public:
    explicit FinalCell(std::vector<size_t> polygon_ids_, const std::vector<Polygon> & polygons_, const Box & box_);
    std::vector<size_t> polygon_ids;
    std::vector<uint8_t> is_covered_by;

private:
    [[nodiscard]] const FinalCell * find(Float64 x, Float64 y) const override;
};

/** A recursively built grid containing information about polygons intersecting each cell.
 *  The starting cell is the bounding box of the given polygons which are stored by reference.
 *  For every cell a vector of indices of intersecting polygons is stored, in the order originally provided upon
 *  construction. A cell is recursively split into kSplit * kSplit equal cells up to the point where the cell
 *  intersects a small enough number of polygons or the maximum allowed depth is exceeded.
 *  Both of these parameters are set in the constructor.
 */
class GridRoot : public ICell
{
public:
    GridRoot(size_t min_intersections_, size_t max_depth_, const std::vector<Polygon> & polygons_);
    /** Retrieves the cell containing a given point.
     *  A null pointer is returned when the point falls outside the grid.
     */
    [[nodiscard]] const FinalCell * find(Float64 x, Float64 y) const override;

    /** When a cell is split every side is split into kSplit pieces producing kSplit * kSplit equal smaller cells. */
    static constexpr size_t kSplit = 4;
    static constexpr size_t kMultiProcessingDepth = 2;

private:
    std::unique_ptr<ICell> root = nullptr;
    Float64 min_x = 0, min_y = 0;
    Float64 max_x = 0, max_y = 0;
    const size_t kMinIntersections;
    const size_t kMaxDepth;

    const std::vector<Polygon> & polygons;

    std::unique_ptr<ICell> makeCell(Float64 min_x, Float64 min_y, Float64 max_x, Float64 max_y, std::vector<size_t> intersecting_ids, size_t depth = 0);

    void setBoundingBox();
};

/** Generate edge indexes during its construction in
 *  the following way: sort all polygon's vertexes by x coordinate, and then store all interesting
 *  polygon edges for each adjacent x coordinates. For each query finds interesting edges and
 *  iterates over them, finding required polygon. If there is more than one any such polygon may be returned.
 */
class BucketsPolygonIndex
{
public:
    /** A two-dimensional point in Euclidean coordinates. */
    using Point = IPolygonDictionary::Point;
    /** A polygon in boost is a an outer ring of points with zero or more cut out inner rings. */
    using Polygon = IPolygonDictionary::Polygon;
    /** A ring in boost used for describing the polygons. */
    using Ring = IPolygonDictionary::Ring;

    /** Builds an index by splitting all edges with provided sorted x coordinates. */
    BucketsPolygonIndex(const std::vector<Polygon> & polygons, const std::vector<Float64> & splits);

    /** Builds an index by splitting all edges with all points x coordinates. */
    BucketsPolygonIndex(const std::vector<Polygon> & polygons);

    /** Finds polygon id the same way as IPolygonIndex. */
    bool find(const Point & point, size_t & id) const;

private:
    /** Returns unique x coordinates among all points. */
    std::vector<Float64> uniqueX(const std::vector<Polygon> & polygons);

    /** Builds indexes described above. */
    void indexBuild(const std::vector<Polygon> & polygons);

    /** Auxiliary function for adding ring to index */
    void indexAddRing(const Ring & ring, size_t polygon_id);

    /** Edge describes edge (adjacent points) of any polygon, and contains polygon's id.
     *  Invariant here is first point has x not greater than second point.
     */
    struct Edge
    {
        Point l;
        Point r;
        size_t polygon_id;
        size_t edge_id;

        static bool compare1(const Edge & a, const Edge & b);
        static bool compare2(const Edge & a, const Edge & b);
    };

    Poco::Logger * log;

    /** Sorted distinct coordinates of all vertexes. */
    std::vector<Float64> sorted_x;
    std::vector<Edge> all_edges;

    /** Edges from all polygons, classified by sorted_x borders.
     *  edges_index[i] stores all interesting edges in range ( sorted_x[i]; sorted_x[i + 1] ]
     *  That means edges_index.size() + 1 == sorted_x.size()
     * 
     *  std::vector<std::vector<Edge>> edges_index;
     */

    /** TODO: fix this and previous comments.
     *  This edges_index_tree stores the same info as edges_index, but more efficiently.
     *  To do that, edges_index_tree is actually a segment tree of segments between x coordinates.
     *  edges_index_tree.size() == edges_index.size() * 2 == n * 2, and as in usual segment tree,
     *  edges_index_tree[i] combines segments edges_index_tree[i*2] and edges_index_tree[i*2+1].
     *  Every polygon's edge covers a segment of x coordinates, and can be added to this tree by
     *  placing it into O(log n) vertexes of this tree.
     */
    std::vector<std::vector<size_t>> edges_index_tree;
};

/** Generate edge indexes during its construction in
 *  the following way: sort all polygon's vertexes by x coordinate, and then store all interesting
 *  polygon edges for each adjacent x coordinates. For each query finds interesting edges and
 *  iterates over them, finding required polygon. If there is more than one any such polygon may be returned.
 */
class BucketsSinglePolygonIndex
{
public:
    /** A two-dimensional point in Euclidean coordinates. */
    using Point = IPolygonDictionary::Point;
    /** A polygon in boost is a an outer ring of points with zero or more cut out inner rings. */
    using Polygon = IPolygonDictionary::Polygon;
    /** A ring in boost used for describing the polygons. */
    using Ring = IPolygonDictionary::Ring;

    /** Builds an index by splitting all edges with all points x coordinates. */
    BucketsSinglePolygonIndex(const Polygon & polygon);

    /** Finds polygon id the same way as IPolygonIndex. */
    bool find(const Point & point) const;

private:
    /** Returns unique x coordinates among all points. */
    std::vector<Float64> uniqueX(const Polygon & polygon);

    /** Builds indexes described above. */
    void indexBuild(const Polygon & polygon);

    /** Auxiliary function for adding ring to index */
    void indexAddRing(const Ring & ring);

    /** Edge describes edge (adjacent points) of any polygon, and contains polygon's id.
     *  Invariant here is first point has x not greater than second point.
     */
    struct Edge
    {
        Point l;
        Point r;
        size_t edge_id;

        static bool compare1(const Edge & a, const Edge & b);
        static bool compare2(const Edge & a, const Edge & b);
    };

    /** Sorted distinct coordinates of all vertexes. */
    std::vector<Float64> sorted_x;
    std::vector<Edge> all_edges;

    /** Edges from all polygons, classified by sorted_x borders.
     *  edges_index[i] stores all interesting edges in range ( sorted_x[i]; sorted_x[i + 1] ]
     *  That means edges_index.size() + 1 == sorted_x.size()
     *
     *  std::vector<std::vector<Edge>> edges_index;
     */

    /** TODO: fix this and previous comments.
     *  This edges_index_tree stores the same info as edges_index, but more efficiently.
     *  To do that, edges_index_tree is actually a segment tree of segments between x coordinates.
     *  edges_index_tree.size() == edges_index.size() * 2 == n * 2, and as in usual segment tree,
     *  edges_index_tree[i] combines segments edges_index_tree[i*2] and edges_index_tree[i*2+1].
     *  Every polygon's edge covers a segment of x coordinates, and can be added to this tree by
     *  placing it into O(log n) vertexes of this tree.
     */
    std::vector<std::vector<size_t>> edges_index_tree;
};

}
