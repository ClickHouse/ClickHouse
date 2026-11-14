#pragma once

#include <Functions/geometry.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <optional>


namespace DB
{

/// Boost.Geometry compares coordinates with an epsilon scaled by `max(1, |coordinate|)` (see
/// `math::scaled_epsilon`), and its sectionalization widens boxes by a multiple of it. Two valid
/// boundaries a few ULPs apart can therefore still be reported as touching, and the validator then
/// rejects their concatenation. Shortcuts that skip the general overlay must only rely on gaps that
/// exceed this conservative margin, computed from the largest absolute coordinate involved.
inline Float64 geoSeparationMargin(Float64 magnitude)
{
    return 0x1p12 * std::numeric_limits<Float64>::epsilon() * std::max(1.0, std::abs(magnitude));
}

/// Whether an interval ending at `low_max` and an interval starting at `high_min` are separated
/// by more than `margin`.
inline bool geoSeparatedBy(Float64 low_max, Float64 high_min, Float64 margin)
{
    return high_min - low_max > margin;
}

/// Exact bounds of a nondegenerate axis-aligned rectangle, with no constructed coordinates.
struct GeoRectangle
{
    Float64 min_x;
    Float64 min_y;
    Float64 max_x;
    Float64 max_y;

    bool contains(const GeoRectangle & other) const
    {
        return min_x <= other.min_x && min_y <= other.min_y && max_x >= other.max_x && max_y >= other.max_y;
    }

    Float64 magnitude() const
    {
        return std::max({std::abs(min_x), std::abs(min_y), std::abs(max_x), std::abs(max_y)});
    }

    CartesianPolygon polygon() const
    {
        CartesianPolygon result;
        result.outer() = {{min_x, min_y}, {min_x, max_y}, {max_x, max_y}, {max_x, min_y}, {min_x, min_y}};
        return result;
    }
};


/// Recognize the actual ring, not just its envelope. Reject approximate closure, repeated
/// corners, diagonal edges and nonfinite coordinates before granting any geometric shortcut.
inline std::optional<GeoRectangle> tryGetGeoRectangle(const CartesianRing & ring)
{
    if (ring.size() != 5 || ring.front().get<0>() != ring.back().get<0>() || ring.front().get<1>() != ring.back().get<1>())
        return std::nullopt;

    GeoRectangle bounds{ring[0].get<0>(), ring[0].get<1>(), ring[0].get<0>(), ring[0].get<1>()};
    for (size_t i = 0; i < 4; ++i)
    {
        const auto & point = ring[i];
        const auto & next = ring[i + 1];
        if (!std::isfinite(point.get<0>()) || !std::isfinite(point.get<1>())
            || ((point.get<0>() == next.get<0>()) == (point.get<1>() == next.get<1>())))
            return std::nullopt;
        bounds.min_x = std::min(bounds.min_x, point.get<0>());
        bounds.min_y = std::min(bounds.min_y, point.get<1>());
        bounds.max_x = std::max(bounds.max_x, point.get<0>());
        bounds.max_y = std::max(bounds.max_y, point.get<1>());
    }

    if (!(bounds.min_x < bounds.max_x && bounds.min_y < bounds.max_y))
        return std::nullopt;

    unsigned corners = 0;
    for (size_t i = 0; i < 4; ++i)
    {
        const auto & point = ring[i];
        if ((point.get<0>() != bounds.min_x && point.get<0>() != bounds.max_x)
            || (point.get<1>() != bounds.min_y && point.get<1>() != bounds.max_y))
            return std::nullopt;
        corners |= 1U << ((point.get<0>() == bounds.max_x ? 1 : 0) + (point.get<1>() == bounds.max_y ? 2 : 0));
    }
    if (corners != 15)
        return std::nullopt;
    return bounds;
}

}
