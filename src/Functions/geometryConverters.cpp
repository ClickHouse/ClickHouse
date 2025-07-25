#include <Functions/geometryConverters.h>

namespace DB
{

SphericalPointInRadians toRadianPoint(const SphericalPoint & degree_point)
{
    Float64 lon = get<0>(degree_point) * M_PI / 180.0;
    Float64 lat = get<1>(degree_point) * M_PI / 180.0;

    return SphericalPointInRadians(lon, lat);
}

LatLng toH3LatLng(const SphericalPointInRadians & point)
{
    LatLng result;
    result.lat = point.get<1>();
    result.lng = point.get<0>();
    return result;
}

}
