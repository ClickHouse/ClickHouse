#pragma once

#include <base/types.h>

#include <string>
#include <string_view>


namespace DB
{

/** Universal Transverse Mercator (UTM) coordinate conversions on the WGS84 ellipsoid,
  * and the Military Grid Reference System (MGRS) built on top of UTM.
  *
  * The forward/reverse transverse Mercator series is the truncated Snyder series, accurate to
  * roughly a millimetre within a UTM zone and degrading away from the central meridian.
  * UTM is only defined for latitudes in [-80, 84]; the polar caps use the separate UPS system.
  */

struct UTMCoordinate
{
    Float64 easting = 0;    /// meters; includes the 500000 m false easting
    Float64 northing = 0;   /// meters; includes the 10000000 m false northing on the southern hemisphere
    UInt8 zone = 0;         /// 1..60
    char band = 0;          /// MGRS latitude band letter 'C'..'X', or 0 when the latitude is out of the UTM domain
};

/// Returns the MGRS latitude band letter for a latitude in degrees, or 0 if the latitude is outside [-80, 84].
char utmLatitudeBand(Float64 latitude);

/// Converts WGS84 (longitude, latitude) in degrees to UTM.
/// If `forced_zone` is in [1, 60] it is used as the zone; otherwise (0) the zone is selected automatically
/// from the longitude, applying the standard Norway and Svalbard exceptions.
/// The latitude must be in [-80, 84] and the longitude in [-180, 180]; the caller validates beforehand.
UTMCoordinate wgs84ToUTM(Float64 longitude, Float64 latitude, UInt8 forced_zone = 0);

/// Converts UTM back to WGS84 (longitude, latitude) in degrees. `is_north` selects the hemisphere.
void utmToWGS84(Float64 easting, Float64 northing, UInt8 zone, bool is_north, Float64 & longitude, Float64 & latitude);

/// Encodes a point as an MGRS string with `precision` digits (0..5) for each of easting and northing:
/// precision 5 -> 1 m, 4 -> 10 m, 3 -> 100 m, 2 -> 1 km, 1 -> 10 km, 0 -> 100 km (grid square only).
std::string mgrsEncode(Float64 longitude, Float64 latitude, UInt8 precision);

struct MGRSCoordinate
{
    Float64 longitude = 0;
    Float64 latitude = 0;
};

/// Decodes an MGRS string into the WGS84 coordinates of the centre of the referenced grid square.
/// Whitespace is ignored and letters are case-insensitive. Throws on malformed input.
MGRSCoordinate mgrsDecode(std::string_view mgrs);

}
