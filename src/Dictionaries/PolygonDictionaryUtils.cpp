#include "PolygonDictionaryUtils.h"

#include <Common/ThreadPool.h>

#include <Common/logger_useful.h>

#include <algorithm>
#include <numeric>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

FinalCell::FinalCell(const std::vector<size_t> & polygon_ids_, const std::vector<Polygon> &, const Box &, bool is_last_covered_):
polygon_ids(polygon_ids_)
{
    if (is_last_covered_)
    {
        first_covered = polygon_ids.back();
        polygon_ids.pop_back();
    }
}

const FinalCell * FinalCell::find(Coord, Coord) const
{
    return this;
}

inline void shift(Point & point, Coord val)
{
    point.x(point.x() + val);
    point.y(point.y() + val);
}

FinalCellWithSlabs::FinalCellWithSlabs(const std::vector<size_t> & polygon_ids_, const std::vector<Polygon> & polygons_, const Box & box_, bool is_last_covered_)
{
    auto extended = box_;
    shift(extended.min_corner(), -GridRoot<FinalCellWithSlabs>::kEps);
    shift(extended.max_corner(), GridRoot<FinalCellWithSlabs>::kEps);
    Polygon tmp_poly;
    bg::convert(extended, tmp_poly);
    std::vector<Polygon> intersections;
    if (is_last_covered_)
        first_covered = polygon_ids_.back();
    for (size_t i = 0; i + is_last_covered_ < polygon_ids_.size(); ++i)
    {
        std::vector<Polygon> intersection;
        bg::intersection(tmp_poly, polygons_[polygon_ids_[i]], intersection);
        for (auto & polygon : intersection)
            intersections.emplace_back(std::move(polygon));
        while (corresponding_ids.size() < intersections.size())
            corresponding_ids.push_back(polygon_ids_[i]);
    }
    if (!intersections.empty())
        index = SlabsPolygonIndex{intersections};
}

const FinalCellWithSlabs * FinalCellWithSlabs::find(Coord, Coord) const
{
    return this;
}

SlabsPolygonIndex::SlabsPolygonIndex(
    const std::vector<Polygon> & polygons)
    : log(getLogger("SlabsPolygonIndex")),
      sorted_x(uniqueX(polygons))
{
    indexBuild(polygons);
}

std::vector<Coord> SlabsPolygonIndex::uniqueX(const std::vector<Polygon> & polygons)
{
    std::vector<Coord> all_x;
    for (const auto & poly : polygons)
    {
        for (const auto & point : poly.outer())
            all_x.push_back(point.x());

        for (const auto & inner : poly.inners())
            for (const auto & point : inner)
                all_x.push_back(point.x());
    }

    /** Making all_x sorted and distinct */
    ::sort(all_x.begin(), all_x.end());
    all_x.erase(std::unique(all_x.begin(), all_x.end()), all_x.end());

    return all_x;
}

void SlabsPolygonIndex::indexBuild(const std::vector<Polygon> & polygons)
{
    for (size_t i = 0; i < polygons.size(); ++i)
    {
        indexAddRing(polygons[i].outer(), i);

        for (const auto & inner : polygons[i].inners())
            indexAddRing(inner, i);
    }

    /** Sorting edges of (left_point, right_point, polygon_id) in that order */
    ::sort(all_edges.begin(), all_edges.end(), Edge::compareByLeftPoint);
    for (size_t i = 0; i != all_edges.size(); ++i)
        all_edges[i].edge_id = i;

    /** Total number of edges */
    size_t m = all_edges.size();

    /** Using custom comparator for fetching edges in right_point order, like in scanline */
    auto cmp = [](const Edge & a, const Edge & b)
    {
        return Edge::compareByRightPoint(a, b);
    };
    std::set<Edge, decltype(cmp)> interesting_edges(cmp);

    /** Size of index (number of different x coordinates) */
    size_t n = 0;
    if (!sorted_x.empty())
    {
        n = sorted_x.size() - 1;
    }
    edges_index_tree.resize(2 * n);

    /** Map of interesting edge ids to the index of left x, the index of right x */
    std::vector<size_t> edge_left(m, n), edge_right(m, n);

    size_t edges_it = 0;
    for (size_t l = 0, r = 1; r < sorted_x.size(); ++l, ++r)
    {
        const Coord lx = sorted_x[l];
        const Coord rx = sorted_x[r];

        /** Removing edges where right_point.x <= lx */
        while (!interesting_edges.empty() && interesting_edges.begin()->r.x() <= lx)
        {
            edge_right[interesting_edges.begin()->edge_id] = l;
            interesting_edges.erase(interesting_edges.begin());
        }

        /** Adding edges where left_point.x < rx */
        for (; edges_it < all_edges.size() && all_edges[edges_it].l.x() < rx; ++edges_it)
        {
            interesting_edges.insert(all_edges[edges_it]);
            edge_left[all_edges[edges_it].edge_id] = l;
        }
    }

    for (size_t i = 0; i != all_edges.size(); ++i)
    {
        size_t l = edge_left[i];
        size_t r = edge_right[i];
        if (l == n || sorted_x[l] != all_edges[i].l.x() || sorted_x[r] != all_edges[i].r.x())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Error occurred while building polygon index. Edge {}  is [{}, {}] but found [{}, {}]. l = {}, r = {}",
                i, all_edges[i].l.x(), all_edges[i].r.x(), sorted_x[l], sorted_x[r], l, r);
        }

        /** Adding [l, r) to the segment tree */
        for (l += n, r += n; l < r; l >>= 1, r >>= 1)
        {
            if (l & 1)
            {
                edges_index_tree[l++].emplace_back(all_edges[i]);
            }
            if (r & 1)
            {
                edges_index_tree[--r].emplace_back(all_edges[i]);
            }
        }
    }
}

void SlabsPolygonIndex::indexAddRing(const Ring & ring, size_t polygon_id)
{
    for (size_t i = 0, prev = ring.size() - 1; i < ring.size(); prev = i, ++i)
    {
        Point a = ring[prev];
        Point b = ring[i];

        /** Making a.x <= b.x */
        if (a.x() > b.x())
            std::swap(a, b);

        if (a.x() == b.x() && a.y() > b.y())
            std::swap(a, b);

        if (a.x() == b.x())
        {
            /** Vertical edge found, skipping for now */
            continue;
        }

        all_edges.emplace_back(a, b, polygon_id, 0);
    }
}

SlabsPolygonIndex::Edge::Edge(
    const Point & l_,
    const Point & r_,
    size_t polygon_id_,
    size_t edge_id_)
    : l(l_),
      r(r_),
      polygon_id(polygon_id_),
      edge_id(edge_id_)
{
    /** Calculating arguments of line equation.
      * Original equation of this edge is:
      * f(x) = l.y() + (r.y() - l.y()) / (r.x() - l.x()) * (x - l.x())
      */
    k = (r.y() - l.y()) / (r.x() - l.x());
    b = l.y() - k * l.x();
}

bool SlabsPolygonIndex::Edge::compareByLeftPoint(const Edge & a, const Edge & b)
{
    /** Comparing left point */
    if (a.l.x() != b.l.x())
        return a.l.x() < b.l.x();
    if (a.l.y() != b.l.y())
        return a.l.y() < b.l.y();

    /** Comparing right point */
    if (a.r.x() != b.r.x())
        return a.r.x() < b.r.x();
    if (a.r.y() != b.r.y())
        return a.r.y() < b.r.y();

    return a.polygon_id < b.polygon_id;
}

bool SlabsPolygonIndex::Edge::compareByRightPoint(const Edge & a, const Edge & b)
{
    /** Comparing right point */
    if (a.r.x() != b.r.x())
        return a.r.x() < b.r.x();
    if (a.r.y() != b.r.y())
        return a.r.y() < b.r.y();

    /** Comparing left point */
    if (a.l.x() != b.l.x())
        return a.l.x() < b.l.x();
    if (a.l.y() != b.l.y())
        return a.l.y() < b.l.y();

    if (a.polygon_id != b.polygon_id)
        return a.polygon_id < b.polygon_id;

    return a.edge_id < b.edge_id;
}

bool SlabsPolygonIndex::find(const Point & point, size_t & id) const
{
    /** Vertical line or nothing at all, no match here */
    if (sorted_x.size() < 2)
        return false;

    Coord x = point.x();
    Coord y = point.y();

    /** Not in bounding box */
    if (x < sorted_x.front() || x > sorted_x.back())
        return false;

    bool found = false;

    /** Point is considired inside when ray down from point crosses odd number of edges.
      * This vector will contain polygon ids of all crosses. Smallest id with odd number of
      * occurrences is the answer.
      */
    std::vector<size_t> intersections;
    intersections.reserve(10);

    /** Find position of the slab with binary search by sorted_x */
    size_t pos = std::upper_bound(sorted_x.begin() + 1, sorted_x.end() - 1, x) - sorted_x.begin() - 1;

    /** Jump to the leaf in segment tree */
    pos += edges_index_tree.size() / 2;
    do
    {
        /** Iterating over interesting edges */
        for (const auto & edge : edges_index_tree[pos])
        {
            /** Check if point lies above the edge */
            if (x * edge.k + edge.b <= y)
                intersections.emplace_back(edge.polygon_id);
        }
        pos >>= 1;
    } while (pos != 0);

    /** Sort all ids and find smallest with odd occurrences */
    ::sort(intersections.begin(), intersections.end());
    for (size_t i = 0; i < intersections.size(); i += 2)
    {
        if (i + 1 == intersections.size() || intersections[i] != intersections[i + 1])
        {
            found = true;
            id = intersections[i];
            break;
        }
    }

    return found;
}

}
