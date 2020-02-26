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
    explicit FinalCell(std::vector<size_t> polygon_ids_);
    std::vector<size_t> polygon_ids;

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
    /** Initializes and builds the grid, saving the intersecting polygons for each cell accordingly.
     *  The order of indexes is always a subsequence of the order specified in this function call.
     */
    void init(const std::vector<size_t> & order_);
    /** Retrieves the cell containing a given point.
     *  A null pointer is returned when the point falls outside the grid.
     */
    [[nodiscard]] const FinalCell * find(Float64 x, Float64 y) const override;

    /** When a cell is split every side is split into kSplit pieces producing kSplit * kSplit equal smaller cells. */
    static constexpr size_t kSplit = 4;
    static constexpr size_t kMultiProcessingDepth = 3;

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

}
