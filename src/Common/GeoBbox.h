#pragma once

#include <algorithm>
#include <cmath>
#include <limits>

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

}
