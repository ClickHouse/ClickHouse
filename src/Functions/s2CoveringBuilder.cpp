#include "config.h"

#if USE_S2_GEOMETRY

#include <Functions/s2CoveringBuilder.h>

#include <boost/geometry.hpp>

#include <s2/s2cell_id.h>
#include <s2/s2cell_union.h>
#include <s2/s2error.h>
#include <s2/s2latlng.h>
#include <s2/s2latlng_rect.h>
#include <s2/s2loop.h>
#include <s2/s2polygon.h>
#include <s2/s2polyline.h>
#include <s2/s2region_coverer.h>

namespace DB
{

/// ── Coordinate conversion helpers ───────────────────────────────────────────

std::optional<S2Point> boostPointToS2Point(const SphericalPoint & point)
{
    const double lon = boost::geometry::get<0>(point);
    const double lat = boost::geometry::get<1>(point);

    if (!std::isfinite(lon) || !std::isfinite(lat))
        return std::nullopt;

    S2LatLng lat_lng = S2LatLng::FromDegrees(lat, lon);
    if (!lat_lng.is_valid())
        return std::nullopt;

    return lat_lng.ToPoint();
}

/// Explicit instantiation for the two types used: SphericalRing and SphericalLineString.
/// Both are boost point sequences that share the same underlying model.
template <typename PointRange>
std::optional<std::vector<S2Point>> convertBoostPointsToS2Points(const PointRange & points, bool remove_closing_vertex)
{
    if (points.empty())
        return std::nullopt;

    std::vector<S2Point> result;
    result.reserve(points.size());

    for (const auto & vertex : points)
    {
        auto s2point = boostPointToS2Point(vertex);
        if (!s2point)
            return std::nullopt;
        result.push_back(*s2point);
    }

    /// Remove closing vertex if it duplicates the first (boost rings are Closed=true).
    if (remove_closing_vertex && result.size() >= 2 && result.front() == result.back())
        result.pop_back();

    return result;
}

/// Explicit template instantiations for the types used by callers.
template std::optional<std::vector<S2Point>> convertBoostPointsToS2Points(const SphericalRing & points, bool remove_closing_vertex);
template std::optional<std::vector<S2Point>> convertBoostPointsToS2Points(const SphericalLineString & points, bool remove_closing_vertex);

/// ── Native S2 type conversion helpers ───────────────────────────────────────

std::unique_ptr<S2Polyline> tryConvertLineStringToS2Polyline(const SphericalLineString & linestring)
{
    auto points_opt = convertBoostPointsToS2Points(linestring, /*remove_closing_vertex=*/false);
    if (!points_opt || points_opt->size() < 2)
        return nullptr;

    auto polyline = std::make_unique<S2Polyline>();
    polyline->set_s2debug_override(S2Debug::DISABLE);
    polyline->Init(*points_opt);

    S2Error error;
    if (polyline->FindValidationError(&error))
        return nullptr;

    return polyline;
}

std::unique_ptr<S2Loop> tryConvertRingToS2Loop(const SphericalRing & ring)
{
    auto points_opt = convertBoostPointsToS2Points(ring, /*remove_closing_vertex=*/true);
    if (!points_opt || points_opt->size() < 3)
        return nullptr;

    auto loop = std::make_unique<S2Loop>();
    loop->set_s2debug_override(S2Debug::DISABLE);
    loop->Init(*points_opt);

    /// Normalize ensures the loop covers ≤ half the sphere, fixing the
    /// CW→CCW conversion issue for convex / small-area rings.
    loop->Normalize();

    S2Error error;
    if (loop->FindValidationError(&error))
        return nullptr;

    return loop;
}

std::unique_ptr<S2Polygon> tryConvertPolygonToS2Polygon(const SphericalPolygon & polygon)
{
    SphericalPolygon corrected = polygon;
    boost::geometry::correct(corrected);

    /// Convert outer ring.
    auto outer_loop = tryConvertRingToS2Loop(corrected.outer());
    if (!outer_loop)
        return nullptr;

    std::vector<std::unique_ptr<S2Loop>> loops;

    /// The outer loop must be CCW (Normalize already ensured ≤ 2π).
    /// For InitOriented, interior is on the left side of edges, so CCW = shell.
    loops.push_back(std::move(outer_loop));

    /// Convert inner rings (holes). After boost::geometry::correct, inners
    /// are CCW (boost convention for holes with ClockWise=true outer).
    /// For S2Polygon::InitOriented, holes must be CW (interior on left = hole),
    /// so we Invert each inner loop after Normalize.
    for (const auto & inner : corrected.inners())
    {
        auto inner_loop = tryConvertRingToS2Loop(inner);
        if (!inner_loop)
            return nullptr;
        /// Normalize made it CCW (small area). Invert to CW for a hole.
        inner_loop->Invert();
        loops.push_back(std::move(inner_loop));
    }

    auto s2polygon = std::make_unique<S2Polygon>();
    s2polygon->set_s2debug_override(S2Debug::DISABLE);
    s2polygon->InitOriented(std::move(loops));

    S2Error error;
    if (s2polygon->FindValidationError(&error))
        return nullptr;

    return s2polygon;
}

/// ── Bounding-box helpers (fallback only) ────────────────────────────────────

namespace
{

/// Expand rect to include a single point. Returns false for invalid coordinates.
bool addPointToRect(const SphericalPoint & point, S2LatLngRect & rect, bool & initialized)
{
    const double lon = boost::geometry::get<0>(point);
    const double lat = boost::geometry::get<1>(point);

    if (!std::isfinite(lon) || !std::isfinite(lat))
        return false;

    S2LatLng lat_lng = S2LatLng::FromDegrees(lat, lon);
    if (!lat_lng.is_valid())
        return false;

    if (!initialized)
    {
        rect = S2LatLngRect(lat_lng, lat_lng);
        initialized = true;
    }
    else
    {
        rect.AddPoint(lat_lng.ToPoint());
    }

    return true;
}

/// Expand the bounding box to include all vertices of a point sequence.
template <typename PointRange>
bool addPointsToRect(const PointRange & points, S2LatLngRect & rect, bool & initialized)
{
    for (const auto & point : points)
    {
        if (!addPointToRect(point, rect, initialized))
            return false;
    }
    return true;
}

}

/// ── S2RegionCoverer wrapper ─────────────────────────────────────────────────

std::optional<S2CellUnion> getCovering(const S2Region & region, int max_cells, int min_level, int max_level)
{
    S2RegionCoverer::Options options;
    options.set_max_cells(max_cells);
    options.set_min_level(min_level);
    options.set_max_level(max_level);

    S2RegionCoverer coverer(options);
    S2CellUnion result = coverer.GetCovering(region);
    result.Normalize();

    if (result.empty())
        return std::nullopt;

    return result;
}

/// ── Build S2 coverings for each geometry type ──────────────────────────────

std::optional<S2CellUnion> buildPointCovering(const SphericalPoint & point, int max_level)
{
    auto s2point = boostPointToS2Point(point);
    if (!s2point)
        return std::nullopt;

    S2CellId cell_id(*s2point);
    if (!cell_id.is_valid())
        return std::nullopt;

    cell_id = cell_id.parent(max_level);

    S2CellUnion result;
    result.Init({cell_id});
    return result;
}

std::optional<S2CellUnion> buildLineStringCovering(
    const SphericalLineString & linestring, int max_cells, int min_level, int max_level)
{
    auto polyline = tryConvertLineStringToS2Polyline(linestring);
    if (polyline)
    {
        if (auto result = getCovering(*polyline, max_cells, min_level, max_level))
            return result;
    }

    /// Fallback to bounding box.
    S2LatLngRect rect;
    bool initialized = false;
    if (!addPointsToRect(linestring, rect, initialized) || !initialized || !rect.is_valid())
        return std::nullopt;
    return getCovering(rect, max_cells, min_level, max_level);
}

std::optional<S2CellUnion> buildMultiLineStringCovering(
    const SphericalMultiLineString & multi, int max_cells, int min_level, int max_level)
{
    S2RegionCoverer::Options options;
    options.set_max_cells(max_cells);
    options.set_min_level(min_level);
    options.set_max_level(max_level);
    S2RegionCoverer coverer(options);

    S2CellUnion combined;
    bool all_converted = true;

    for (const auto & linestring : multi)
    {
        auto polyline = tryConvertLineStringToS2Polyline(linestring);
        if (!polyline)
        {
            all_converted = false;
            break;
        }
        S2CellUnion sub = coverer.GetCovering(*polyline);
        combined = combined.empty() ? std::move(sub) : combined.Union(sub);
    }

    if (all_converted)
    {
        combined.Normalize();
        if (!combined.empty())
            return combined;
    }

    /// Fallback to bounding box.
    S2LatLngRect rect;
    bool initialized = false;
    for (const auto & linestring : multi)
    {
        if (!addPointsToRect(linestring, rect, initialized))
            return std::nullopt;
    }
    if (!initialized || !rect.is_valid())
        return std::nullopt;
    return getCovering(rect, max_cells, min_level, max_level);
}

std::optional<S2CellUnion> buildRingCovering(
    const SphericalRing & ring, int max_cells, int min_level, int max_level)
{
    /// Wrap the ring as a single-loop S2Polygon for a tight covering.
    auto loop = tryConvertRingToS2Loop(ring);
    if (loop)
    {
        auto s2polygon = std::make_unique<S2Polygon>();
        s2polygon->set_s2debug_override(S2Debug::DISABLE);
        s2polygon->Init(std::move(loop));

        S2Error error;
        if (!s2polygon->FindValidationError(&error))
        {
            if (auto result = getCovering(*s2polygon, max_cells, min_level, max_level))
                return result;
        }
    }

    /// Fallback to bounding box.
    S2LatLngRect rect;
    bool initialized = false;
    if (!addPointsToRect(ring, rect, initialized) || !initialized || !rect.is_valid())
        return std::nullopt;
    return getCovering(rect, max_cells, min_level, max_level);
}

std::optional<S2CellUnion> buildPolygonCovering(
    const SphericalPolygon & polygon, int max_cells, int min_level, int max_level)
{
    auto s2polygon = tryConvertPolygonToS2Polygon(polygon);
    if (s2polygon)
    {
        if (auto result = getCovering(*s2polygon, max_cells, min_level, max_level))
            return result;
    }

    /// Fallback to bounding box (outer ring only — inners are contained).
    SphericalPolygon corrected = polygon;
    boost::geometry::correct(corrected);
    S2LatLngRect rect;
    bool initialized = false;
    if (!addPointsToRect(corrected.outer(), rect, initialized) || !initialized || !rect.is_valid())
        return std::nullopt;
    return getCovering(rect, max_cells, min_level, max_level);
}

std::optional<S2CellUnion> buildMultiPolygonCovering(
    const SphericalMultiPolygon & multi, int max_cells, int min_level, int max_level)
{
    S2CellUnion combined;
    bool all_converted = true;

    for (const auto & poly : multi)
    {
        auto s2polygon = tryConvertPolygonToS2Polygon(poly);
        if (!s2polygon)
        {
            all_converted = false;
            break;
        }
        auto sub = getCovering(*s2polygon, max_cells, min_level, max_level);
        if (!sub)
        {
            all_converted = false;
            break;
        }
        combined = combined.empty() ? std::move(*sub) : combined.Union(*sub);
    }

    if (all_converted)
    {
        combined.Normalize();
        if (!combined.empty())
            return combined;
    }

    /// Fallback to bounding box.
    S2LatLngRect rect;
    bool initialized = false;
    for (const auto & poly : multi)
    {
        SphericalPolygon corrected = poly;
        boost::geometry::correct(corrected);
        if (!addPointsToRect(corrected.outer(), rect, initialized))
            return std::nullopt;
    }
    if (!initialized || !rect.is_valid())
        return std::nullopt;
    return getCovering(rect, max_cells, min_level, max_level);
}

}

#endif
