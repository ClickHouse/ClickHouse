#pragma once

#include <Core/Types.h>

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

class ICell
{
public:
    virtual ~ICell() = default;

    virtual const ICell * find(Float64 x, Float64 y) const = 0;
};

class DividedCell : public ICell
{
public:
    explicit DividedCell(std::vector<std::unique_ptr<ICell>> children_);
    [[nodiscard]] const ICell * find(Float64 x, Float64 y) const override;
private:
    std::vector<std::unique_ptr<ICell>> children;
};

class FinalCell : public ICell
{
public:
    explicit FinalCell(std::vector<size_t> polygon_ids_);

private:
    std::vector<size_t> polygon_ids;

    [[nodiscard]] const ICell * find(Float64 x, Float64 y) const override;
};

class GridRoot : public ICell
{
public:
    explicit GridRoot(const size_t min_intersections_, const size_t max_depth_, const std::vector<Polygon> & polygons_);
    [[nodiscard]] const ICell * find(Float64 x, Float64 y) const override;

    static constexpr size_t kSplit = 10;
private:
    std::unique_ptr<ICell> root = nullptr;
    Float64 min_x = 0, min_y = 0;
    Float64 max_x = 0, max_y = 0;
    const size_t kMinIntersections = 3;
    const size_t kMaxDepth = 3;

    const std::vector<Polygon> & polygons;

    std::unique_ptr<ICell> makeCell(Float64 min_x, Float64 min_y, Float64 max_x, Float64 max_y, std::vector<size_t> intersecting_ids, size_t depth = 0);

    void setBoundingBox();
};

}