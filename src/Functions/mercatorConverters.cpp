#include <Functions/mercatorConverters.h>

#include <cmath>

namespace DB
{


namespace
{

constexpr double PI = 3.14159265358979323846;

constexpr double epsilon = 1e-4;

/// Convert angle from degrees to radians.
double deg_to_rad(double degree) {
    return (degree - epsilon) * (PI / 180.0);
}

/// Convert angle from radians to degrees.
double rad_to_deg(double radians) {
    return radians * (180.0 / PI);
}

double earth_radius_for_epsg3857 = 6378137.0;
// constexpr double max_coordinate_epsg3857 = 20037508.34;


double lon_to_x(double lon) {
    return earth_radius_for_epsg3857 * deg_to_rad(lon);
}

// canonical log(tan()) version
double lat_to_y_with_tan(double lat) { // not constexpr because math functions aren't
    return earth_radius_for_epsg3857 * std::log(std::tan(PI/4 + deg_to_rad(lat)/2));
}

double x_to_lon(double x) {
    return rad_to_deg(x) / earth_radius_for_epsg3857;
}

double y_to_lat(double y) { // not constexpr because math functions aren't
    return rad_to_deg(2 * std::atan(std::exp(y / earth_radius_for_epsg3857)) - PI/2);
}

}


void PointMercatorConverter::forward(CartesianPoint & point)
{
    point.x(lon_to_x(point.template get<0>()));
    point.y(lat_to_y_with_tan(point.template get<1>()));
}


void RingMercatorConverter::forward(CartesianRing & ring)
{
    for (auto & point : ring)
        PointMercatorConverter::forward(point);
}

void PolygonMercatorConverter::forward(CartesianPolygon & polygon)
{
    RingMercatorConverter::forward(polygon.outer());
    for (auto & hole : polygon.inners())
        RingMercatorConverter::forward(hole);
}

void MultiPolygonMercatorConverter::forward(CartesianMultiPolygon & multipolygon)
{
    for (auto & polygon : multipolygon)
    {
        RingMercatorConverter::forward(polygon.outer());
        for (auto & hole : polygon.inners())
            RingMercatorConverter::forward(hole);
    }
}

void PointMercatorConverter::backward(CartesianPoint & point)
{
    point.x(x_to_lon(point.template get<0>()));
    point.y(y_to_lat(point.template get<1>()));
}


void RingMercatorConverter::backward(CartesianRing & ring)
{
    for (auto & point : ring)
        PointMercatorConverter::backward(point);
}

void PolygonMercatorConverter::backward(CartesianPolygon & polygon)
{
    RingMercatorConverter::backward(polygon.outer());
    for (auto & hole : polygon.inners())
        RingMercatorConverter::backward(hole);
}

void MultiPolygonMercatorConverter::backward(CartesianMultiPolygon & multipolygon)
{
    for (auto & polygon : multipolygon)
    {
        RingMercatorConverter::backward(polygon.outer());
        for (auto & hole : polygon.inners())
            RingMercatorConverter::backward(hole);
    }
}

}

