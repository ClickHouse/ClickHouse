#pragma once

#include <algorithm>
#include <cmath>
#include <limits>
#include <optional>
#include <string_view>
#include <type_traits>

namespace DB
{
    class ActionsDAG;
    class IColumn;
    class ColumnConst;
    class ColumnString;
    class ColumnTuple;
    class ColumnArray;
}

namespace DB
{

/// Accumulates a bounding box from (x, y) coordinate pairs.
/// Check `found` before using xmin/ymin/xmax/ymax.
struct BboxAccumulator
{
    double xmin = std::numeric_limits<double>::infinity();
    double ymin = std::numeric_limits<double>::infinity();
    double xmax = -std::numeric_limits<double>::infinity();
    double ymax = -std::numeric_limits<double>::infinity();
    bool found = false;

    void add(double x, double y)
    {
        if (!std::isfinite(x) || !std::isfinite(y)) return;
        xmin = std::min(xmin, x);
        ymin = std::min(ymin, y);
        xmax = std::max(xmax, x);
        ymax = std::max(ymax, y);
        found = true;
    }

    /// Iterate over a container whose elements have .x() and .y() methods (e.g. CartesianPoint).
    template <typename Container>
    void addAll(const Container & pts) { for (const auto & p : pts) add(p.x(), p.y()); }
};

/// Try to extract a bounding box from a constant column.
/// Handles: WKB-encoded String (via parseWKBFormat), CH native geometry
/// (ColumnTuple<Float64,Float64> for points, ColumnArray of tuples for collections).
/// Returns true and sets xmin/ymin/xmax/ymax on success.
bool tryExtractBboxFromColumn(
    const IColumn & col,
    double & xmin, double & ymin,
    double & xmax, double & ymax);

}

#include <Common/GeoBbox.impl.h>
