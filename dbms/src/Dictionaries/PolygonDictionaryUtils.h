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
    DividedCell(std::vector<std::unique_ptr<ICell>> children_);
    const ICell * find(Float64 x, Float64 y) const override;
private:
    std::vector<std::unique_ptr<ICell>> children;
};

class FinalCell : public ICell
{
public:
    FinalCell(std::vector<size_t> polygon_ids_);

private:
    std::vector<size_t> polygon_ids;

    const ICell * find(Float64 x, Float64 y) const override;
};

class GridRoot : public ICell
{
public:
    GridRoot(const Box & box_, const std::vector<Polygon> & polygons_);
    void build();
    const ICell * find(Float64 x, Float64 y) const override;

    static constexpr size_t kSplit = 10;
private:
    std::unique_ptr<ICell> root = nullptr;
    Box box;
    const size_t kMinIntersections = 3;
    const size_t kMaxDepth = 3;

    const std::vector<Polygon> & polygons;

    std::unique_ptr<ICell> makeCell(const Box & box, std::vector<size_t> intersecting_ids, size_t depth = 0);
};

}