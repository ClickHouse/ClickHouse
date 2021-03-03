#pragma once

#include <Core/Types.h>

#include <boost/geometry/geometries/geometries.hpp>
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include "common/types.h"

namespace DB
{

template <typename Point>
using Ring = boost::geometry::model::ring<Point>;

template <typename Point>
using Polygon = boost::geometry::model::polygon<Point>;

template <typename Point>
using MultiPolygon = boost::geometry::model::multi_polygon<Polygon<Point>>;

using CartesianPoint = boost::geometry::model::d2::point_xy<Float64>;
using CartesianRing = Ring<CartesianPoint>;
using CartesianPolygon = Polygon<CartesianPoint>;
using CartesianMultiPolygon = MultiPolygon<CartesianPoint>;

/// Latitude, longitude
using SphericalPoint = boost::geometry::model::point<Float64, 2, boost::geometry::cs::spherical_equatorial<boost::geometry::degree>>;
using SphericalRing = Ring<SphericalPoint>;
using SphericalPolygon = Polygon<SphericalPoint>;
using SphericalMultiPolygon = MultiPolygon<SphericalPoint>;

}
