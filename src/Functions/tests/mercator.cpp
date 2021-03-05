#include <iostream>
#include <map>

#include <Functions/geometryConverters.h>


constexpr double PI = 3.14159265358979323846;

/// Convert angle from degrees to radians.
inline constexpr double deg_to_rad(double degree) noexcept {
    return degree * (PI / 180.0);
}

/// Convert angle from radians to degrees.
inline constexpr double rad_to_deg(double radians) noexcept {
    return radians * (180.0 / PI);
}

constexpr double earth_radius_for_epsg3857 = 6378137.0;
// constexpr double max_coordinate_epsg3857 = 20037508.34;

constexpr inline double lon_to_x(double lon) noexcept {
    return earth_radius_for_epsg3857 * deg_to_rad(lon);
}

// canonical log(tan()) version
inline double lat_to_y_with_tan(double lat) { // not constexpr because math functions aren't
    return earth_radius_for_epsg3857 * std::log(std::tan(PI/4 + deg_to_rad(lat)/2));
}

constexpr inline double x_to_lon(double x) {
    return rad_to_deg(x) / earth_radius_for_epsg3857;
}

inline double y_to_lat(double y) { // not constexpr because math functions aren't
    return rad_to_deg(2 * std::atan(std::exp(y / earth_radius_for_epsg3857)) - PI/2);
}

/// POLYGON((4.346693 50.858306, 4.367945 50.852455, 4.366227 50.840809, 4.344961 50.833264, 4.338074 50.848677, 4.346693 50.858306))
/// POLYGON((25.0010 136.9987, 17.7500 142.5000, 11.3733 142.5917))


/// POLYGON((4.3613577 50.8651821, 4.349556 50.8535879, 4.3602419 50.8435626, 4.3830299 50.8428851, 4.3904543 50.8564867, 4.3613148 50.8651279))
/// POLYGON((4.346693 50.858306, 4.367945 50.852455, 4.366227 50.840809, 4.344961 50.833264, 4.338074 50.848677, 4.346693 50.858306))

void mercator(DB::CartesianPolygon & polygon)
{
    for (auto & point : polygon.outer())
    {
        point.x(lon_to_x(point.x()));
        point.y(lat_to_y_with_tan(point.y()));
    }
}

void reverseMercator(DB::CartesianMultiPolygon & multi_polygon)
{
    for (auto & polygon : multi_polygon)
    {
        for (auto & point : polygon.outer())
        {
            point.x(x_to_lon(point.x()));
            point.y(y_to_lat(point.y()));
        }
    }
}


void printMultiPolygon(DB::CartesianMultiPolygon & multi_polygon)
{
    std::cout << "--------------" << std::endl;
    for (auto & polygon : multi_polygon)
    {
        for (auto & point : polygon.outer())
        {
            std::cout << point.x() << ' ' << point.y() << std::endl;
        }
    }
    std::cout << "--------------" << std::endl;
}

void test1()
{
    DB::CartesianPolygon green, blue;
    boost::geometry::read_wkt(
        "POLYGON((4.346693 50.858306, 4.367945 50.852455, 4.366227 50.840809, 4.344961 50.833264, 4.338074 50.848677, 4.346693 50.858306))", green);

    boost::geometry::read_wkt(
        "POLYGON((25.0010 136.9987, 17.7500 142.5000, 11.3733 142.5917))", blue);

    mercator(green);
    mercator(blue);

    DB::CartesianMultiPolygon output;
    boost::geometry::intersection(green, blue, output);

    reverseMercator(output);

    printMultiPolygon(output);
}


// 4.3666052904432435, 50.84337386140151
// 4.3602419 50.8435626
// 4.349556 50.8535879 
// 4.3526804582393535 50.856658100365976
// 4.367945 50.852455
// 4.3666052904432435 50.84337386140151


void test2()
{
    DB::CartesianPolygon green, blue;
    boost::geometry::read_wkt(
        "POLYGON((4.3613577 50.8651821, 4.349556 50.8535879, 4.3602419 50.8435626, 4.3830299 50.8428851, 4.3904543 50.8564867, 4.3613148 50.8651279))", green);

    boost::geometry::read_wkt(
        "POLYGON((4.346693 50.858306, 4.367945 50.852455, 4.366227 50.840809, 4.344961 50.833264, 4.338074 50.848677, 4.346693 50.858306))", blue);

    boost::geometry::correct(green);
    boost::geometry::correct(blue);

    mercator(green);
    mercator(blue);

    DB::CartesianMultiPolygon output;
    boost::geometry::intersection(green, blue, output);

    reverseMercator(output);

    boost::geometry::correct(output);

    printMultiPolygon(output);
}


void test3()
{
    DB::CartesianPolygon green, blue;
    boost::geometry::read_wkt(
        "POLYGON((4.3613577 50.8651821, 4.349556 50.8535879, 4.3602419 50.8435626, 4.3830299 50.8428851, 4.3904543 50.8564867, 4.3613148 50.8651279))", green);

    boost::geometry::read_wkt(
        "POLYGON((4.3613577 50.8651821, 4.349556 50.8535879, 4.3602419 50.8435626, 4.3830299 50.8428851, 4.3904543 50.8564867, 4.3613148 50.8651279))", blue);

    boost::geometry::correct(green);
    boost::geometry::correct(blue);

    mercator(green);
    mercator(blue);

    DB::CartesianMultiPolygon output;
    boost::geometry::intersection(green, blue, output);

    reverseMercator(output);

    boost::geometry::correct(output);

    printMultiPolygon(output);
}

int main(int argc, char ** argv)
{
    (void) argc;
    (void) argv;
    test1();
    test2();
    test3();
    return 0;
}
