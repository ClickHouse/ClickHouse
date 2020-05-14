#include "PolygonDictionaryUtils.h"

#include <Common/ThreadPool.h>

#include <common/logger_useful.h>

#include <algorithm>
#include <thread>
#include <numeric>

namespace DB
{

FinalCell::FinalCell(std::vector<size_t> polygon_ids_, const std::vector<Polygon> & polygons_, const Box & box_):
polygon_ids(std::move(polygon_ids_))
{
    Polygon tmp_poly;
    bg::convert(box_, tmp_poly);
    std::transform(polygon_ids.begin(), polygon_ids.end(), std::back_inserter(is_covered_by), [&](const auto id)
    {
        return bg::covered_by(tmp_poly, polygons_[id]);
    });
}

const FinalCell * FinalCell::find(Coord, Coord) const
{
    return this;
}

DividedCell::DividedCell(std::vector<std::unique_ptr<ICell>> children_): children(std::move(children_)) {}

const FinalCell * DividedCell::find(Coord x, Coord y) const
{
    auto x_ratio = x * GridRoot::kSplit;
    auto y_ratio = y * GridRoot::kSplit;
    auto x_bin = static_cast<int>(x_ratio);
    auto y_bin = static_cast<int>(y_ratio);
    return children[y_bin + x_bin * GridRoot::kSplit]->find(x_ratio - x_bin, y_ratio - y_bin);
}

GridRoot::GridRoot(const size_t min_intersections_, const size_t max_depth_, const std::vector<Polygon> & polygons_):
kMinIntersections(min_intersections_), kMaxDepth(max_depth_), polygons(polygons_)
{
    setBoundingBox();
    std::vector<size_t> order(polygons.size());
    std::iota(order.begin(), order.end(), 0);
    root = makeCell(min_x, min_y, max_x, max_y, order);
}

const FinalCell * GridRoot::find(Coord x, Coord y) const
{
    if (x < min_x || x >= max_x)
        return nullptr;
    if (y < min_y || y >= max_y)
        return nullptr;
    return root->find((x - min_x) / (max_x - min_x), (y - min_y) / (max_y - min_y));
}

std::unique_ptr<ICell> GridRoot::makeCell(Coord current_min_x, Coord current_min_y, Coord current_max_x, Coord current_max_y, std::vector<size_t> possible_ids, size_t depth)
{
    auto current_box = Box(Point(current_min_x, current_min_y), Point(current_max_x, current_max_y));
    possible_ids.erase(std::remove_if(possible_ids.begin(), possible_ids.end(), [&](const auto id)
    {
        return !bg::intersects(current_box, polygons[id]);
    }), possible_ids.end());
    if (possible_ids.size() <= kMinIntersections || depth++ == kMaxDepth)
        return std::make_unique<FinalCell>(possible_ids, polygons, current_box);
    auto x_shift = (current_max_x - current_min_x) / kSplit;
    auto y_shift = (current_max_y - current_min_y) / kSplit;
    std::vector<std::unique_ptr<ICell>> children;
    children.resize(kSplit * kSplit);
    std::vector<ThreadFromGlobalPool> threads;
    for (size_t i = 0; i < kSplit; current_min_x += x_shift, ++i)
    {
        auto handle_row = [this, &children, &y_shift, &x_shift, &possible_ids, &depth, i](Coord x, Coord y)
        {
            for (size_t j = 0; j < kSplit; y += y_shift, ++j)
            {
                children[i * kSplit + j] = makeCell(x, y, x + x_shift, y + y_shift, possible_ids, depth);
            }
        };
        if (depth <= kMultiProcessingDepth)
            threads.emplace_back(handle_row, current_min_x, current_min_y);
        else
            handle_row(current_min_x, current_min_y);
    }
    for (auto & thread : threads)
        thread.join();
    return std::make_unique<DividedCell>(std::move(children));
}

void GridRoot::setBoundingBox()
{
    bool first = true;
    std::for_each(polygons.begin(), polygons.end(), [&](const auto & polygon)
    {
        bg::for_each_point(polygon, [&](const Point & point)
        {
            auto x = point.get<0>();
            auto y = point.get<1>();
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

        /** removing edges where right_point.x < lx */
        while (!interesting_edges.empty() && interesting_edges.begin()->r.x() < lx)
        {
            edge_right[interesting_edges.begin()->edge_id] = l;
            interesting_edges.erase(interesting_edges.begin());
        }

        /** adding edges where left_point.x <= rx */
        for (; edges_it < this->all_edges.size() && this->all_edges[edges_it].l.x() <= rx; ++edges_it)
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
        if (l == n)
        {
            LOG_TRACE(log, "Edge " << i << " is very sad");
            continue;
        }

        /** adding [l, r) to the segment tree */
        for (l += n, r += n; l < r; l >>= 1, r >>= 1)
        {
            if (l & 1)
            {
                this->edges_index_tree[l++].push_back(all_edges[i]);
                ++total_index_edges;
            }
            if (r & 1)
            {
                this->edges_index_tree[--r].push_back(all_edges[i]);
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

        // making a.x <= b.x
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

    return a.polygon_id < b.polygon_id;
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

    return a.polygon_id < b.polygon_id;
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
            const Point & l = edge.l;
            const Point & r = edge.r;
            size_t polygon_id = edge.polygon_id;

            /** check if point outside of edge's x bounds */
            if (x < l.x() || x >= r.x())
            {
                continue;
            }

            Coord edge_y = x * edge.k + edge.b;
            if (edge_y <= y)
            {
                intersections.emplace_back(polygon_id);
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

BucketsSinglePolygonIndex::BucketsSinglePolygonIndex(
        const Polygon & polygon)
        : sorted_x(uniqueX(polygon))
{
    indexBuild(polygon);
}

std::vector<Coord> BucketsSinglePolygonIndex::uniqueX(const Polygon & polygon)
{
    std::vector<Coord> all_x;

    for (auto & point : polygon.outer())
    {
        all_x.push_back(point.x());
    }

    for (auto & inner : polygon.inners())
    {
        for (auto & point : inner)
        {
            all_x.push_back(point.x());
        }
    }

    /** making all_x sorted and distinct */
    std::sort(all_x.begin(), all_x.end());
    all_x.erase(std::unique(all_x.begin(), all_x.end()), all_x.end());

    return all_x;
}

void BucketsSinglePolygonIndex::indexBuild(const Polygon & polygon)
{
    indexAddRing(polygon.outer());

    for (auto & inner : polygon.inners())
    {
        indexAddRing(inner);
    }

    /** sorting edges consisting of (left_point, right_point, polygon_id) in that order */
    std::sort(this->all_edges.begin(), this->all_edges.end(), Edge::compare1);
    for (size_t i = 0; i != this->all_edges.size(); ++i)
    {
        this->all_edges[i].edge_id = i;
    }

    /** total number of edges */
    size_t m = this->all_edges.size();

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

        /** removing edges where right_point.x < lx */
        while (!interesting_edges.empty() && interesting_edges.begin()->r.x() < lx)
        {
            edge_right[interesting_edges.begin()->edge_id] = l;
            interesting_edges.erase(interesting_edges.begin());
        }

        /** adding edges where left_point.x <= rx */
        for (; edges_it < this->all_edges.size() && this->all_edges[edges_it].l.x() <= rx; ++edges_it)
        {
            interesting_edges.insert(this->all_edges[edges_it]);
            edge_left[this->all_edges[edges_it].edge_id] = l;
        }
    }

    for (size_t i = 0; i != this->all_edges.size(); i++)
    {
        size_t l = edge_left[i];
        size_t r = edge_right[i];
        if (l == n)
        {
            continue;
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

}

void BucketsSinglePolygonIndex::indexAddRing(const Ring & ring)
{
    for (size_t i = 0, prev = ring.size() - 1; i < ring.size(); prev = i, ++i)
    {
        Point a = ring[prev];
        Point b = ring[i];

        // making a.x <= b.x
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

        this->all_edges.emplace_back(a, b, 0, 0);
    }
}

bool BucketsSinglePolygonIndex::find(const Point & point) const
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

    size_t pos = std::upper_bound(this->sorted_x.begin() + 1, this->sorted_x.end() - 1, x) - this->sorted_x.begin() - 1;

    size_t cnt = 0;
    /** Here we doing: pos += n */
    pos += this->edges_index_tree.size() / 2;
    do
    {
        /** iterating over interesting edges */
        for (const auto & edge : this->edges_index_tree[pos])
        {
            /** check if point outside of edge's x bounds */
            if (x < edge.l_x || x >= edge.r_x)
            {
                continue;
            }

            Coord edge_y = x * edge.k + edge.b;
            if (edge_y <= y)
            {
                ++cnt;
            }
        }
        pos >>= 1;
    } while (pos != 0);

    return cnt % 2 == 1;
}

}
