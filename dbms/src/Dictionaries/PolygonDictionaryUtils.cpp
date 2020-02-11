#include "PolygonDictionaryUtils.h"

#include <algorithm>
#include <numeric>

namespace DB
{

FinalCell::FinalCell(std::vector<size_t> polygon_ids_): polygon_ids(std::move(polygon_ids_)) {}

const ICell * FinalCell::find(Float64, Float64) const
{
    return this;
}

DividedCell::DividedCell(std::vector<std::unique_ptr<ICell> children_): children(std::move(children_)) {}

const ICell * DividedCell::find(Float64 x, Float64 y) const
{
    auto x_ratio = x * GridRoot::kSplit;
    auto y_ratio = y * GridRoot::kSplit;
    auto x_bin = static_cast<int>(x_ratio);
    auto y_bin = static_cast<int>(y_ratio);
    return children[x_bin + y_bin * GridRoot::kSplit]->find(x_ratio - x_bin, y_ratio - y_bin);
}

GridRoot::GridRoot(const std::vector<Polygon> & polygons_): box(getBoundingBox(polygons_)), polygons(polygons_)
{
    std::vector<size_t> ids(polygons.size());
    std::iota(ids.begin(), ids.end(), 0);
    root = makeCell(box, ids);
}

const ICell * GridRoot::find(Float64 x, Float64 y) const
{
    auto min_x = box.min_corner().get<0>();
    auto min_y = box.min_corner().get<1>();
    auto max_x = box.max_corner().get<0>();
    auto max_y = box.max_corner().get<1>();
    if (x < min_x || x >= max_x)
        return nullptr;
    if (y < min_y || y >= max_y)
        return nullptr;
    return root->find((x - min_x) / (max_x - min_x), (y - min_y) / (max_y - min_y));
}

std::unique_ptr<ICell> GridRoot::makeCell(const DB::Box & current_box, std::vector<size_t> possible_ids, size_t depth)
{
    ++depth;
    possible_ids.erase(std::remove_if(possible_ids.begin(), possible_ids.end(), [&](const auto & id) {
        return !bg::intersects(box, polygons[id]);
    }), possible_ids.end());
    if (possible_ids.size() <= kMinIntersections || depth == kMaxDepth)
        return std::make_unique<FinalCell>(possible_ids);
    auto min_x = current_box.min_corner().get<0>();
    auto min_y = current_box.min_corner().get<1>();
    auto x_shift = (current_box.max_corner().get<0>() - min_x) / kSplit;
    auto y_shift = (current_box.max_corner().get<1>() - min_y) / kSplit;
    std::vector<std::unique_ptr<ICell>> children;
    children.reserve(kSplit * kSplit);
    for (size_t i = 0; i < kSplit; min_x += x_shift, ++i)
    {
        for (size_t j = 0; j < kSplit; min_y += y_shift, ++j)
        {
            children.push_back(makeCell(Box(Point(min_x, min_y), Point(min_x + x_shift, min_y + y_shift)), possible_ids, depth + 1));
        }
    }
    return std::make_unique<DividedCell>(children);
}

Box GridRoot::getBoundingBox(const std::vector<Polygon> & polygons)
{
    bool first = true;
    Float64 min_x = 0, min_y = 0, max_x = 0, max_y = 0;
    std::for_each(polygons.begin(), polygons.end(), [&](const auto & polygon) {
        bg::for_each_point(polygon, [&](const Point & point) {
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
            first = false;
        });
    });
    return Box(Point(min_x, min_y), Point(max_x, max_y));
}

}