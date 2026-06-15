#pragma once

/// Shared S2 covering computation helpers.
///
/// These functions convert boost::geometry types (Spherical*) to S2 types
/// and build S2CellUnion coverings. They are used by both `geoToS2Cells`
/// (the SQL function) and `ProjectionIndexS2` (the S2 projection index).
///
/// Performance note: these functions are NOT on any hot path — the hot path
/// is the per-row binary search in `s2CoveringIntersects`. These helpers
/// run once per geometry during index build or covering computation.

#include "config.h"

#if USE_S2_GEOMETRY

#include <Functions/geometryConverters.h>

#include <s2/s2cell_union.h>

#include <memory>
#include <optional>

class S2Loop;
class S2Polygon;
class S2Polyline;

namespace DB
{

/// Options for S2 covering computation.
struct S2CoveringOptions
{
    int max_cells = 8;
    int min_level = 16;
    int max_level = 20;
};

/// ── Coordinate conversion helpers ───────────────────────────────────────────

/// Convert a boost (lon, lat) point in degrees to an S2Point on the unit sphere.
/// Returns nullopt for invalid coordinates (NaN, out-of-range lat/lng).
std::optional<S2Point> boostPointToS2Point(const SphericalPoint & point);

/// Convert a boost point range (Ring or LineString) to a vector of S2Points.
/// If `remove_closing_vertex` is true and the last vertex equals the first,
/// the closing vertex is removed (S2Loop does not want it).
template <typename PointRange>
std::optional<std::vector<S2Point>> convertBoostPointsToS2Points(const PointRange & points, bool remove_closing_vertex);

/// ── Native S2 type conversion helpers ───────────────────────────────────────

/// Try to convert a boost linestring to an S2Polyline. Returns nullptr on failure.
std::unique_ptr<S2Polyline> tryConvertLineStringToS2Polyline(const SphericalLineString & linestring);

/// Try to convert a boost ring to an S2Loop. Returns nullptr on failure.
/// The resulting loop is normalized to enclose the smaller region (≤ 2π sr).
std::unique_ptr<S2Loop> tryConvertRingToS2Loop(const SphericalRing & ring);

/// Try to convert a boost polygon (outer + inner rings) to an S2Polygon.
/// Returns nullptr on failure.
std::unique_ptr<S2Polygon> tryConvertPolygonToS2Polygon(const SphericalPolygon & polygon);

/// ── S2RegionCoverer wrapper ─────────────────────────────────────────────────

/// Cover an S2 region using the given parameters. Returns nullopt if the covering is empty.
std::optional<S2CellUnion> getCovering(const S2Region & region, int max_cells, int min_level, int max_level);

/// ── Build S2 coverings for each geometry type ──────────────────────────────
///
/// Strategy per geometry kind:
///   Point           → single S2CellId at max_level (no coverer needed)
///   LineString      → native S2Polyline covering (tight); bounding-box fallback
///   MultiLineString → native S2Polyline per segment, union; bounding-box fallback
///   Ring            → S2Loop → S2Polygon covering (tight); bounding-box fallback
///   Polygon         → S2Polygon covering (tight); bounding-box fallback
///   MultiPolygon    → S2Polygon per element, union; bounding-box fallback

std::optional<S2CellUnion> buildPointCovering(const SphericalPoint & point, int max_level);

std::optional<S2CellUnion> buildLineStringCovering(
    const SphericalLineString & linestring, int max_cells, int min_level, int max_level);

std::optional<S2CellUnion> buildMultiLineStringCovering(
    const SphericalMultiLineString & multi, int max_cells, int min_level, int max_level);

std::optional<S2CellUnion> buildRingCovering(
    const SphericalRing & ring, int max_cells, int min_level, int max_level);

std::optional<S2CellUnion> buildPolygonCovering(
    const SphericalPolygon & polygon, int max_cells, int min_level, int max_level);

std::optional<S2CellUnion> buildMultiPolygonCovering(
    const SphericalMultiPolygon & multi, int max_cells, int min_level, int max_level);

/// Convenience overloads taking S2CoveringOptions.

inline std::optional<S2CellUnion> buildPointCovering(const SphericalPoint & point, const S2CoveringOptions & opts)
{
    return buildPointCovering(point, opts.max_level);
}

inline std::optional<S2CellUnion> buildLineStringCovering(const SphericalLineString & ls, const S2CoveringOptions & opts)
{
    return buildLineStringCovering(ls, opts.max_cells, opts.min_level, opts.max_level);
}

inline std::optional<S2CellUnion> buildMultiLineStringCovering(const SphericalMultiLineString & m, const S2CoveringOptions & opts)
{
    return buildMultiLineStringCovering(m, opts.max_cells, opts.min_level, opts.max_level);
}

inline std::optional<S2CellUnion> buildRingCovering(const SphericalRing & r, const S2CoveringOptions & opts)
{
    return buildRingCovering(r, opts.max_cells, opts.min_level, opts.max_level);
}

inline std::optional<S2CellUnion> buildPolygonCovering(const SphericalPolygon & p, const S2CoveringOptions & opts)
{
    return buildPolygonCovering(p, opts.max_cells, opts.min_level, opts.max_level);
}

inline std::optional<S2CellUnion> buildMultiPolygonCovering(const SphericalMultiPolygon & m, const S2CoveringOptions & opts)
{
    return buildMultiPolygonCovering(m, opts.max_cells, opts.min_level, opts.max_level);
}

}

#endif
