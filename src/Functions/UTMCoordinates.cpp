#include <Functions/UTMCoordinates.h>

#include <Common/Exception.h>

#include <algorithm>
#include <cctype>
#include <cmath>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// WGS84 ellipsoid and UTM constants.
constexpr Float64 WGS84_A = 6378137.0;                      /// semi-major axis, meters
constexpr Float64 WGS84_ECC_SQ = 0.0066943799901413165;    /// first eccentricity squared
constexpr Float64 UTM_SCALE = 0.9996;                       /// central scale factor k0
constexpr Float64 FALSE_EASTING = 500000.0;
constexpr Float64 FALSE_NORTHING = 10000000.0;

constexpr Float64 DEG_TO_RAD = M_PI / 180.0;
constexpr Float64 RAD_TO_DEG = 180.0 / M_PI;

/// MGRS latitude bands from 80°S (omitting 'I' and 'O'); 'X' additionally covers 72°..84°.
constexpr std::string_view BAND_LETTERS = "CDEFGHJKLMNPQRSTUVWX";

/// MGRS 100km square lettering.
constexpr int NUM_100K_SETS = 6;
constexpr std::string_view SET_ORIGIN_COLUMN_LETTERS = "AJSAJS";
constexpr std::string_view SET_ORIGIN_ROW_LETTERS = "AFAFAF";

constexpr int A_ASCII = 'A';
constexpr int I_ASCII = 'I';
constexpr int O_ASCII = 'O';
constexpr int V_ASCII = 'V';
constexpr int Z_ASCII = 'Z';

int get100kSetForZone(int zone)
{
    int set = zone % NUM_100K_SETS;
    if (set == 0)
        set = NUM_100K_SETS;
    return set;
}

/// Builds the two-letter 100km grid square identifier, skipping the unused 'I' and 'O' letters.
/// Ported from the proj4js/mgrs reference implementation.
std::string getLetter100kID(int column, int row, int set)
{
    const int index = set - 1;
    const int col_origin = SET_ORIGIN_COLUMN_LETTERS[index];
    const int row_origin = SET_ORIGIN_ROW_LETTERS[index];

    int col = col_origin + column - 1;
    int row_letter = row_origin + row;
    bool rollover = false;

    if (col > Z_ASCII)
    {
        col = col - Z_ASCII + A_ASCII - 1;
        rollover = true;
    }

    if (col == I_ASCII || (col_origin < I_ASCII && col > I_ASCII) || ((col > I_ASCII || col_origin < I_ASCII) && rollover))
        ++col;
    if (col == O_ASCII || (col_origin < O_ASCII && col > O_ASCII) || ((col > O_ASCII || col_origin < O_ASCII) && rollover))
    {
        ++col;
        if (col == I_ASCII)
            ++col;
    }
    if (col > Z_ASCII)
        col = col - Z_ASCII + A_ASCII - 1;

    if (row_letter > V_ASCII)
    {
        row_letter = row_letter - V_ASCII + A_ASCII - 1;
        rollover = true;
    }
    else
        rollover = false;

    if (row_letter == I_ASCII || (row_origin < I_ASCII && row_letter > I_ASCII)
        || ((row_letter > I_ASCII || row_origin < I_ASCII) && rollover))
        ++row_letter;
    if (row_letter == O_ASCII || (row_origin < O_ASCII && row_letter > O_ASCII)
        || ((row_letter > O_ASCII || row_origin < O_ASCII) && rollover))
    {
        ++row_letter;
        if (row_letter == I_ASCII)
            ++row_letter;
    }
    if (row_letter > V_ASCII)
        row_letter = row_letter - V_ASCII + A_ASCII - 1;

    std::string result;
    result += static_cast<char>(col);
    result += static_cast<char>(row_letter);
    return result;
}

std::string get100kID(Float64 easting, Float64 northing, int zone)
{
    const int set = get100kSetForZone(zone);
    const int set_column = static_cast<int>(std::floor(easting / 100000.0));
    const int set_row = static_cast<int>(std::floor(northing / 100000.0)) % 20;
    return getLetter100kID(set_column, set_row, set);
}

/// The minimum northing of each latitude band; used to recover which 2000km northing cycle a value lies in.
Float64 getMinNorthing(char band)
{
    switch (band)
    {
        case 'C': return 1100000.0;
        case 'D': return 2000000.0;
        case 'E': return 2800000.0;
        case 'F': return 3700000.0;
        case 'G': return 4600000.0;
        case 'H': return 5500000.0;
        case 'J': return 6400000.0;
        case 'K': return 7300000.0;
        case 'L': return 8200000.0;
        case 'M': return 9100000.0;
        case 'N': return 0.0;
        case 'P': return 800000.0;
        case 'Q': return 1700000.0;
        case 'R': return 2600000.0;
        case 'S': return 3500000.0;
        case 'T': return 4400000.0;
        case 'U': return 5300000.0;
        case 'V': return 6200000.0;
        case 'W': return 7000000.0;
        case 'X': return 7900000.0;
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid MGRS latitude band '{}'", String(1, band));
    }
}

Float64 getEastingFromChar(char letter, int set)
{
    int cur = SET_ORIGIN_COLUMN_LETTERS[set - 1];
    Float64 easting = 100000.0;
    bool rewound = false;
    while (cur != static_cast<int>(letter))
    {
        ++cur;
        if (cur == I_ASCII)
            ++cur;
        if (cur == O_ASCII)
            ++cur;
        if (cur > Z_ASCII)
        {
            if (rewound)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid MGRS 100km column letter '{}'", String(1, letter));
            cur = A_ASCII;
            rewound = true;
        }
        easting += 100000.0;
    }

    /// Only the first eight 100km columns (eastings 100000..800000) occur within a UTM zone; later
    /// letters of the set cannot be produced by the encoder and would decode far outside the zone.
    if (easting > 800000.0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MGRS 100km column letter '{}' is out of range for the zone", String(1, letter));

    return easting;
}

Float64 getNorthingFromChar(char letter, int set)
{
    if (letter > 'V')
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid MGRS 100km row letter '{}'", String(1, letter));

    int cur = SET_ORIGIN_ROW_LETTERS[set - 1];
    Float64 northing = 0.0;
    bool rewound = false;
    while (cur != static_cast<int>(letter))
    {
        ++cur;
        if (cur == I_ASCII)
            ++cur;
        if (cur == O_ASCII)
            ++cur;
        if (cur > V_ASCII)
        {
            if (rewound)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid MGRS 100km row letter '{}'", String(1, letter));
            cur = A_ASCII;
            rewound = true;
        }
        northing += 100000.0;
    }
    return northing;
}

}

char utmLatitudeBand(Float64 latitude)
{
    if (latitude >= 72.0 && latitude <= 84.0)
        return 'X';
    if (latitude >= -80.0 && latitude < 72.0)
        return BAND_LETTERS[static_cast<size_t>(std::floor((latitude + 80.0) / 8.0))];
    return 0;
}

UTMCoordinate wgs84ToUTM(Float64 longitude, Float64 latitude, UInt8 forced_zone)
{
    const Float64 lat_rad = latitude * DEG_TO_RAD;
    const Float64 lon_rad = longitude * DEG_TO_RAD;

    int zone = 0;
    if (forced_zone >= 1 && forced_zone <= 60)
    {
        zone = forced_zone;
    }
    else
    {
        /// std::clamp guards the longitude == 180 boundary, which would otherwise give zone 61.
        zone = std::clamp(static_cast<int>(std::floor((longitude + 180.0) / 6.0)) + 1, 1, 60);

        /// Western Norway: zone 32 is widened to cover the coast.
        if (latitude >= 56.0 && latitude < 64.0 && longitude >= 3.0 && longitude < 12.0)
            zone = 32;

        /// Svalbard: zones 31, 33, 35, 37 are widened and the even zones removed.
        /// The upper bound is inclusive to match the public latitude domain (up to 84°).
        if (latitude >= 72.0 && latitude <= 84.0)
        {
            if (longitude >= 0.0 && longitude < 9.0)
                zone = 31;
            else if (longitude >= 9.0 && longitude < 21.0)
                zone = 33;
            else if (longitude >= 21.0 && longitude < 33.0)
                zone = 35;
            else if (longitude >= 33.0 && longitude < 42.0)
                zone = 37;
        }
    }

    const Float64 lon_origin_rad = ((zone - 1) * 6.0 - 180.0 + 3.0) * DEG_TO_RAD;
    const Float64 ecc_prime_sq = WGS84_ECC_SQ / (1.0 - WGS84_ECC_SQ);

    const Float64 sin_lat = std::sin(lat_rad);
    const Float64 cos_lat = std::cos(lat_rad);
    const Float64 tan_lat = std::tan(lat_rad);

    const Float64 n = WGS84_A / std::sqrt(1.0 - WGS84_ECC_SQ * sin_lat * sin_lat);
    const Float64 t = tan_lat * tan_lat;
    const Float64 c = ecc_prime_sq * cos_lat * cos_lat;
    const Float64 a = cos_lat * (lon_rad - lon_origin_rad);

    const Float64 e2 = WGS84_ECC_SQ;
    const Float64 m = WGS84_A
        * ((1.0 - e2 / 4.0 - 3.0 * e2 * e2 / 64.0 - 5.0 * e2 * e2 * e2 / 256.0) * lat_rad
           - (3.0 * e2 / 8.0 + 3.0 * e2 * e2 / 32.0 + 45.0 * e2 * e2 * e2 / 1024.0) * std::sin(2.0 * lat_rad)
           + (15.0 * e2 * e2 / 256.0 + 45.0 * e2 * e2 * e2 / 1024.0) * std::sin(4.0 * lat_rad)
           - (35.0 * e2 * e2 * e2 / 3072.0) * std::sin(6.0 * lat_rad));

    UTMCoordinate result;
    result.zone = static_cast<UInt8>(zone);
    result.band = utmLatitudeBand(latitude);

    result.easting = UTM_SCALE * n
            * (a + (1.0 - t + c) * a * a * a / 6.0
               + (5.0 - 18.0 * t + t * t + 72.0 * c - 58.0 * ecc_prime_sq) * a * a * a * a * a / 120.0)
        + FALSE_EASTING;

    result.northing = UTM_SCALE
        * (m
           + n * tan_lat
               * (a * a / 2.0 + (5.0 - t + 9.0 * c + 4.0 * c * c) * a * a * a * a / 24.0
                  + (61.0 - 58.0 * t + t * t + 600.0 * c - 330.0 * ecc_prime_sq) * a * a * a * a * a * a / 720.0));

    if (latitude < 0.0)
        result.northing += FALSE_NORTHING;

    return result;
}

void utmToWGS84(Float64 easting, Float64 northing, UInt8 zone, bool is_north, Float64 & longitude, Float64 & latitude)
{
    const Float64 e2 = WGS84_ECC_SQ;
    const Float64 e1 = (1.0 - std::sqrt(1.0 - e2)) / (1.0 + std::sqrt(1.0 - e2));

    const Float64 x = easting - FALSE_EASTING;
    Float64 y = northing;
    if (!is_north)
        y -= FALSE_NORTHING;

    const Float64 lon_origin = (zone - 1) * 6.0 - 180.0 + 3.0;
    const Float64 ecc_prime_sq = e2 / (1.0 - e2);

    const Float64 m = y / UTM_SCALE;
    const Float64 mu = m / (WGS84_A * (1.0 - e2 / 4.0 - 3.0 * e2 * e2 / 64.0 - 5.0 * e2 * e2 * e2 / 256.0));

    const Float64 phi1 = mu + (3.0 * e1 / 2.0 - 27.0 * e1 * e1 * e1 / 32.0) * std::sin(2.0 * mu)
        + (21.0 * e1 * e1 / 16.0 - 55.0 * e1 * e1 * e1 * e1 / 32.0) * std::sin(4.0 * mu)
        + (151.0 * e1 * e1 * e1 / 96.0) * std::sin(6.0 * mu);

    const Float64 sin_phi1 = std::sin(phi1);
    const Float64 cos_phi1 = std::cos(phi1);
    const Float64 tan_phi1 = std::tan(phi1);

    const Float64 n1 = WGS84_A / std::sqrt(1.0 - e2 * sin_phi1 * sin_phi1);
    const Float64 t1 = tan_phi1 * tan_phi1;
    const Float64 c1 = ecc_prime_sq * cos_phi1 * cos_phi1;
    const Float64 r1 = WGS84_A * (1.0 - e2) / std::pow(1.0 - e2 * sin_phi1 * sin_phi1, 1.5);
    const Float64 d = x / (n1 * UTM_SCALE);

    const Float64 lat_rad = phi1
        - (n1 * tan_phi1 / r1)
            * (d * d / 2.0 - (5.0 + 3.0 * t1 + 10.0 * c1 - 4.0 * c1 * c1 - 9.0 * ecc_prime_sq) * d * d * d * d / 24.0
               + (61.0 + 90.0 * t1 + 298.0 * c1 + 45.0 * t1 * t1 - 252.0 * ecc_prime_sq - 3.0 * c1 * c1) * d * d * d * d * d * d
                   / 720.0);
    latitude = lat_rad * RAD_TO_DEG;

    const Float64 lon_rad = (d - (1.0 + 2.0 * t1 + c1) * d * d * d / 6.0
                             + (5.0 - 2.0 * c1 + 28.0 * t1 - 3.0 * c1 * c1 + 8.0 * ecc_prime_sq + 24.0 * t1 * t1) * d * d * d * d
                                 * d / 120.0)
        / cos_phi1;
    longitude = lon_origin + lon_rad * RAD_TO_DEG;
}

std::string mgrsEncode(Float64 longitude, Float64 latitude, UInt8 precision)
{
    precision = std::min<UInt8>(precision, 5);

    const UTMCoordinate utm = wgs84ToUTM(longitude, latitude);

    const Int64 easting_int = static_cast<Int64>(std::floor(utm.easting));
    const Int64 northing_int = static_cast<Int64>(std::floor(utm.northing));

    std::string result = std::to_string(utm.zone);
    result += utm.band;
    result += get100kID(static_cast<Float64>(easting_int), static_cast<Float64>(northing_int), utm.zone);

    if (precision > 0)
    {
        /// The within-square coordinate is a 5-digit number (0..99999); take its `precision` most significant digits.
        auto most_significant_digits = [&](Int64 value) -> std::string
        {
            Int64 within = value % 100000;
            if (within < 0)
                within += 100000;
            char buf[5];
            for (int i = 4; i >= 0; --i)
            {
                buf[i] = static_cast<char>('0' + within % 10);
                within /= 10;
            }
            return std::string(buf, buf + precision);
        };

        result += most_significant_digits(easting_int);
        result += most_significant_digits(northing_int);
    }

    return result;
}

MGRSCoordinate mgrsDecode(std::string_view mgrs)
{
    std::string clean;
    clean.reserve(mgrs.size());
    for (char c : mgrs)
    {
        if (std::isspace(static_cast<unsigned char>(c)))
            continue;
        clean += static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }

    size_t i = 0;
    std::string zone_digits;
    while (i < clean.size() && std::isdigit(static_cast<unsigned char>(clean[i])))
    {
        zone_digits += clean[i++];
        /// The zone designator is one or two digits; bail out before std::stoi can overflow on a long prefix.
        if (zone_digits.size() > 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid MGRS string '{}': zone designator must be one or two digits", String(mgrs));
    }

    if (zone_digits.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid MGRS string '{}': missing zone number", String(mgrs));

    const int zone = std::stoi(zone_digits);
    if (zone < 1 || zone > 60)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid MGRS zone number {} in string '{}'", zone, String(mgrs));

    /// The latitude band letter and the two 100km square letters.
    if (i + 3 > clean.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid MGRS string '{}': too short", String(mgrs));

    const char band = clean[i++];
    if (band < 'C' || band > 'X' || band == 'I' || band == 'O')
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid MGRS latitude band '{}' in string '{}'", String(1, band), String(mgrs));

    /// In the X band (72°..84°N) the even zones 32, 34, 36 do not exist; they are absorbed by the widened Svalbard zones.
    if (band == 'X' && (zone == 32 || zone == 34 || zone == 36))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MGRS zone {} does not exist in the X band in string '{}'", zone, String(mgrs));

    const char column_letter = clean[i++];
    const char row_letter = clean[i++];

    const int set = get100kSetForZone(zone);
    Float64 easting = getEastingFromChar(column_letter, set);
    Float64 northing = getNorthingFromChar(row_letter, set);

    /// Shift the northing into the correct 2000km cycle for the band.
    const Float64 min_northing = getMinNorthing(band);
    while (northing < min_northing)
        northing += 2000000.0;

    const size_t remainder = clean.size() - i;
    if (remainder % 2 != 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid MGRS string '{}': odd number of location digits", String(mgrs));

    const size_t per = remainder / 2;
    if (per > 5)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Invalid MGRS string '{}': more than five digits per coordinate", String(mgrs));

    /// The size of the referenced square: 100 km with no digits, down to 1 m with five digits.
    Float64 cell_size = 100000.0;
    if (per > 0)
    {
        const std::string_view east_digits(clean.data() + i, per);
        const std::string_view north_digits(clean.data() + i + per, per);
        for (char c : clean.substr(i))
            if (!std::isdigit(static_cast<unsigned char>(c)))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid MGRS string '{}': non-digit in location", String(mgrs));

        cell_size = 100000.0 / std::pow(10.0, static_cast<Float64>(per));
        easting += std::stod(String(east_digits)) * cell_size;
        northing += std::stod(String(north_digits)) * cell_size;
    }

    /// Return the centre of the referenced square. This is consistent with geohashDecode and, being half a
    /// cell away from the edges, re-encodes back to the same MGRS string without boundary rounding issues.
    easting += cell_size / 2.0;
    northing += cell_size / 2.0;

    const bool is_north = band >= 'N';

    MGRSCoordinate result;
    utmToWGS84(easting, northing, static_cast<UInt8>(zone), is_north, result.longitude, result.latitude);

    /// Reject squares whose decoded latitude does not fall within the declared band. A row letter that does
    /// not occur in the band (e.g. '1LAB') otherwise decodes, after the northing roll-over, to a point a whole
    /// 2000km cycle away in a different band. The tolerance covers cells that straddle a band boundary (a 100km
    /// cell reaches at most ~0.5° beyond it), while a misplaced square lands a full cycle (~18°) away.
    const size_t band_index = BAND_LETTERS.find(band);
    const Float64 band_min_lat = -80.0 + 8.0 * static_cast<Float64>(band_index);
    const Float64 band_max_lat = (band == 'X') ? 84.0 : band_min_lat + 8.0;
    if (result.latitude < band_min_lat - 1.0 || result.latitude > band_max_lat + 1.0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "MGRS 100km square does not intersect latitude band '{}' in string '{}'", String(1, band), String(mgrs));

    return result;
}

}
