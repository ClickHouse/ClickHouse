#include <Core/ColumnWithTypeAndName.h>
#include <Core/Types.h>

#include <boost/variant.hpp>
#include <boost/geometry/geometries/geometries.hpp>
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

namespace DB {

using Point = boost::geometry::model::d2::point_xy<Float64>;
using Ring = boost::geometry::model::ring<Point>;
using Polygon = boost::geometry::model::polygon<Point>;
using MultiPolygon = boost::geometry::model::multi_polygon<Polygon>;
using Geometry = boost::variant<Point, Ring, Polygon, MultiPolygon>;

Geometry geometryFromColumn(const ColumnWithTypeAndName & col, size_t i);

}
