#include "PolygonDictionaryUtils.h"

#include <Common/ThreadPool.h>

#include <common/logger_useful.h>

#include <algorithm>
#include <thread>
#include <numeric>

namespace DB
{

FinalCell::FinalCell(const std::vector<size_t> & polygon_ids_, const std::vector<Polygon> & polygons_, const Box & box_)
{
    Polygon tmp_poly;
    bg::convert(box_, tmp_poly);
    for (const auto id : polygon_ids_)
    {
        if (bg::covered_by(tmp_poly, polygons_[id]))
        {
            first_covered = id;
            break;
        }
        polygon_ids.push_back(id);
    }
}

const FinalCell * FinalCell::find(Coord, Coord) const
{
    return this;
}

inline void shift(Point & point, Coord val) {
    point.x(point.x() + val);
    point.y(point.y() + val);
}

FinalCellWithSlabs::FinalCellWithSlabs(const std::vector<size_t> & polygon_ids_, const std::vector<Polygon> & polygons_, const Box & box_)
{
    auto extended = box_;
    shift(extended.min_corner(), -GridRoot<FinalCellWithSlabs>::kEps);
    shift(extended.max_corner(), GridRoot<FinalCellWithSlabs>::kEps);
    Polygon tmp_poly;
    bg::convert(extended, tmp_poly);
    std::vector<Polygon> intersections;
    for (const auto id : polygon_ids_)
    {
        if (bg::covered_by(tmp_poly, polygons_[id]))
        {
            first_covered = id;
            break;
        }
        std::vector<Polygon> intersection;
        bg::intersection(tmp_poly, polygons_[id], intersection);
        for (auto & polygon : intersection)
            intersections.emplace_back(std::move(polygon));
        while (corresponding_ids.size() < intersections.size())
            corresponding_ids.push_back(id);
    }
    index = BucketsPolygonIndex{intersections};
}

const FinalCellWithSlabs * FinalCellWithSlabs::find(Coord, Coord) const
{
    return this;
}

BucketsPolygonIndex::BucketsPolygonIndex(
    const std::vector<Polygon> & polygons,
    const std::vector<Coord> & splits)
    : log(&Logger::get("BucketsPolygonIndex")),
      sorted_x(splits)
{
    indexBuild(polygons);
}

BucketsPolygonIndex::BucketsPolygonIndex(
    const std::vector<Polygon> & polygons)
    : log(&Logger::get("BucketsPolygonIndex")),
      sorted_x(uniqueX(polygons))
{
    indexBuild(polygons);
}

std::vector<Coord> BucketsPolygonIndex::uniqueX(const std::vector<Polygon> & polygons)
{
    std::vector<Coord> all_x;
    for (size_t i = 0; i < polygons.size(); ++i)
    {
        for (auto & point : polygons[i].outer())
        {
            all_x.push_back(point.x());
        }

        for (auto & inner : polygons[i].inners())
        {
            for (auto & point : inner)
            {
                all_x.push_back(point.x());
            }
        }
    }

    /** making all_x sorted and distinct */
    std::sort(all_x.begin(), all_x.end());
    all_x.erase(std::unique(all_x.begin(), all_x.end()), all_x.end());

    LOG_TRACE(log, "Found " << all_x.size() << " unique x coordinates");

    return all_x;
}

void BucketsPolygonIndex::indexBuild(const std::vector<Polygon> & polygons)
{
    for (size_t i = 0; i < polygons.size(); ++i)
    {
        indexAddRing(polygons[i].outer(), i);

        for (auto & inner : polygons[i].inners())
        {
            indexAddRing(inner, i);
        }
    }

    /** sorting edges consisting of (left_point, right_point, polygon_id) in that order */
    std::sort(this->all_edges.begin(), this->all_edges.end(), Edge::compare1);
    for (size_t i = 0; i != this->all_edges.size(); ++i)
    {
        this->all_edges[i].edge_id = i;
    }
    
    /** total number of edges */
    size_t m = this->all_edges.size();

    LOG_TRACE(log, "Just sorted " << all_edges.size() << " edges from all " << polygons.size() << " polygons");

    /** using custom comparator for fetching edges in right_point order, like in scanline */
    auto cmp = [](const Edge & a, const Edge & b)
    {
        return Edge::compare2(a, b);
    };
    std::set<Edge, decltype(cmp)> interesting_edges(cmp);

    /** size of index (number of different x coordinates) */
    size_t n = 0;
    if (!this->sorted_x.empty())
    {
        n = this->sorted_x.size() - 1;
    }
    this->edges_index_tree.resize(2 * n);

    /** Map of interesting edge ids to the index of left x, the index of right x */
    std::vector<size_t> edge_left(m, n), edge_right(m, n);

    size_t total_index_edges = 0;
    size_t edges_it = 0;
    for (size_t l = 0, r = 1; r < this->sorted_x.size(); ++l, ++r)
    {
        const Coord lx = this->sorted_x[l];
        const Coord rx = this->sorted_x[r];

        /** removing edges where right_point.x <= lx */
        while (!interesting_edges.empty() && interesting_edges.begin()->r.x() <= lx)
        {
            edge_right[interesting_edges.begin()->edge_id] = l;
            interesting_edges.erase(interesting_edges.begin());
        }

        /** adding edges where left_point.x < rx */
        for (; edges_it < this->all_edges.size() && this->all_edges[edges_it].l.x() < rx; ++edges_it)
        {
            interesting_edges.insert(this->all_edges[edges_it]);
            edge_left[this->all_edges[edges_it].edge_id] = l;
        }

        if (l % 1000 == 0 || r + 1 == this->sorted_x.size())
        {
            LOG_TRACE(log, "Iteration " << r << "/" << this->sorted_x.size());
        }
    }

    for (size_t i = 0; i != this->all_edges.size(); i++)
    {
        size_t l = edge_left[i];
        size_t r = edge_right[i];
        if (l == n || sorted_x[l] != all_edges[i].l.x() || sorted_x[r] != all_edges[i].r.x())
        {
            LOG_ERROR(log, "Error occured while building polygon index. Edge " << i << " is ["
                << all_edges[i].l.x() << ";" << all_edges[i].r.x() << "] but found ["
                << sorted_x[l] << ";" << sorted_x[r] << "]. l=" << l << ", r=" << r);
            throw Poco::Exception("polygon index build error");
        }

        /** adding [l, r) to the segment tree */
        for (l += n, r += n; l < r; l >>= 1, r >>= 1)
        {
            if (l & 1)
            {
                this->edges_index_tree[l++].emplace_back(all_edges[i]);
                ++total_index_edges;
            }
            if (r & 1)
            {
                this->edges_index_tree[--r].emplace_back(all_edges[i]);
                ++total_index_edges;
            }
        }
    }

    LOG_TRACE(log, "Index is built, total_index_edges=" << total_index_edges);
}

void BucketsPolygonIndex::indexAddRing(const Ring & ring, size_t polygon_id)
{
    for (size_t i = 0, prev = ring.size() - 1; i < ring.size(); prev = i, ++i)
    {
        Point a = ring[prev];
        Point b = ring[i];

        /** making a.x <= b.x */
        if (a.x() > b.x())
        {
            std::swap(a, b);
        }

        if (a.x() == b.x() && a.y() > b.y())
        {
            std::swap(a, b);
        }

        if (a.x() == b.x())
        {
            /** vertical edge found, skipping for now */
            continue;
        }

        this->all_edges.emplace_back(a, b, polygon_id, 0);
    }
}

BucketsPolygonIndex::Edge::Edge(
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
     * Original equation is:
     * f(x) = l.y() + (r.y() - l.y()) / (r.x() - l.x()) * (x - l.x())
     */
    k = (r.y() - l.y()) / (r.x() - l.x());
    b = l.y() - k * l.x();
}

bool BucketsPolygonIndex::Edge::compare1(const Edge & a, const Edge & b)
{
    /** comparing left point */
    if (a.l.x() != b.l.x())
    {
        return a.l.x() < b.l.x();
    }
    if (a.l.y() != b.l.y())
    {
        return a.l.y() < b.l.y();
    }

    /** comparing right point */
    if (a.r.x() != b.r.x())
    {
        return a.r.x() < b.r.x();
    }
    if (a.r.y() != b.r.y())
    {
        return a.r.y() < b.r.y();
    }

    if (a.polygon_id != b.polygon_id)
    {
        return a.polygon_id < b.polygon_id;
    }

    return a.edge_id < b.edge_id;
}

bool BucketsPolygonIndex::Edge::compare2(const Edge & a, const Edge & b)
{
    /** comparing right point */
    if (a.r.x() != b.r.x())
    {
        return a.r.x() < b.r.x();
    }
    if (a.r.y() != b.r.y())
    {
        return a.r.y() < b.r.y();
    }

    /** comparing left point */
    if (a.l.x() != b.l.x())
    {
        return a.l.x() < b.l.x();
    }
    if (a.l.y() != b.l.y())
    {
        return a.l.y() < b.l.y();
    }

    if (a.polygon_id != b.polygon_id)
    {
        return a.polygon_id < b.polygon_id;
    }

    return a.edge_id < b.edge_id;
}

namespace
{
    inline void update_result(bool & found, size_t & id, const size_t & new_id)
    {
        if (!found || new_id < id)
        {
            found = true;
            id = new_id;
        }
    }
}

bool BucketsPolygonIndex::find(const Point & point, size_t & id) const
{
    /** TODO: maybe we should check for vertical line? */
    if (this->sorted_x.size() < 2)
    {
        return false;
    }

    Coord x = point.x();
    Coord y = point.y();

    if (x < this->sorted_x[0] || x > this->sorted_x.back())
    {
        return false;
    }

    bool found = false;

    /** point is considired inside when ray down from point crosses odd number of edges */
    std::vector<size_t> intersections;
    intersections.reserve(10);

    size_t pos = std::upper_bound(this->sorted_x.begin() + 1, this->sorted_x.end() - 1, x) - this->sorted_x.begin() - 1;

    /** Here we doing: pos += n */
    pos += this->edges_index_tree.size() / 2;
    do
    {
        /** iterating over interesting edges */
        for (const auto & edge : this->edges_index_tree[pos])
        {
            if (x * edge.k + edge.b <= y)
            {
                intersections.emplace_back(edge.polygon_id);
            }
        }
        pos >>= 1;
    } while (pos != 0);

    std::sort(intersections.begin(), intersections.end());
    for (size_t i = 0; i < intersections.size(); i += 2)
    {
        if (i + 1 == intersections.size() || intersections[i] != intersections[i + 1])
        {
            update_result(found, id, intersections[i]);
            break;
        }
    }

    return found;
}

}
