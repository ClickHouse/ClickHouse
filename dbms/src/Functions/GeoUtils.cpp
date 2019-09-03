#include <Core/Types.h>
#include <Functions/GeoUtils.h>

namespace
{

using namespace DB;

const char geohash_base32_encode_lookup_table[32] = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm',
    'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
    'y', 'z',
};

// TODO: this could be halved by excluding 128-255 range.
const UInt8 geohash_base32_decode_lookup_table[256] = {
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    4,    5,    6,    7,    8,    9,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 10,   11,   12,   13,   14,   15,   16,   0xFF, 17,   18,   0xFF, 19,   20,   0xFF,
    21,   22,   23,   24,   25,   26,   27,   28,   29,   30,   31,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
};

const size_t BITS_PER_SYMBOL = 5;
const size_t MAX_PRECISION = 12;
const size_t MAX_BITS = MAX_PRECISION * BITS_PER_SYMBOL * 1.5;
const Float64 LON_MIN = -180;
const Float64 LON_MAX = 180;
const Float64 LAT_MIN = -90;
const Float64 LAT_MAX = 90;

using Encoded = std::array<UInt8, MAX_BITS>;

enum CoordType
{
    LATITUDE,
    LONGITUDE,
};

inline UInt8 singleCoordBitsPrecision(UInt8 precision, CoordType type)
{
    // Single coordinate occupies only half of the total bits.
    const UInt8 bits = (precision * BITS_PER_SYMBOL) / 2;
    if (precision & 0x1 && type == LONGITUDE)
    {
        return bits + 1;
    }

    return bits;
}

inline Encoded encodeCoordinate(Float64 coord, Float64 min, Float64 max, UInt8 bits)
{
    Encoded result;
    result.fill(0);

    for (size_t i = 0; i < bits; ++i)
    {
        const Float64 mid = (max + min) / 2;
        if (coord >= mid)
        {
            result[i] = 1;
            min = mid;
        }
        else
        {
            result[i] = 0;
            max = mid;
        }
    }

    return result;
}

inline Float64 decodeCoordinate(const Encoded & coord, Float64 min, Float64 max, UInt8 bits)
{
    Float64 mid = (max + min) / 2;
    for (size_t i = 0; i < bits; ++i)
    {
        const auto c = coord[i];
        if (c == 1)
        {
            min = mid;
        }
        else
        {
            max = mid;
        }

        mid = (max + min) / 2;
    }

    return mid;
}

inline Encoded merge(const Encoded & encodedLon, const Encoded & encodedLat, UInt8 precision)
{
    Encoded result;
    result.fill(0);

    const auto bits = (precision * BITS_PER_SYMBOL) / 2;
    UInt8 i = 0;
    for (; i < bits; ++i)
    {
        result[i * 2 + 0] = encodedLon[i];
        result[i * 2 + 1] = encodedLat[i];
    }
    // in case of even precision, add last bit of longitude
    if (precision & 0x1)
    {
        result[i * 2] = encodedLon[i];
    }

    return result;
}

inline std::tuple<Encoded, Encoded> split(const Encoded & combined, UInt8 precision)
{
    Encoded lat, lon;
    lat.fill(0);
    lon.fill(0);

    UInt8 i = 0;
    for (; i < precision * BITS_PER_SYMBOL - 1; i += 2)
    {
        // longitude is even bits
        lon[i/2] = combined[i];
        lat[i/2] = combined[i + 1];
    }
    // precision is even, read the last bit as lat.
    if (precision & 0x1)
    {
        lon[i/2] = combined[precision * BITS_PER_SYMBOL - 1];
    }

    return std::tie(lon, lat);
}

inline void base32Encode(const Encoded & binary, UInt8 precision, char * out)
{
    extern const char geohash_base32_encode_lookup_table[32];

    for (UInt8 i = 0; i < precision * BITS_PER_SYMBOL; i += BITS_PER_SYMBOL)
    {
        UInt8 v = binary[i];
        v <<= 1;
        v |= binary[i + 1];
        v <<= 1;
        v |= binary[i + 2];
        v <<= 1;
        v |= binary[i + 3];
        v <<= 1;
        v |= binary[i + 4];

        assert(v < 32);

        *out = geohash_base32_encode_lookup_table[v];
        ++out;
    }
}

inline Encoded base32Decode(const char * encoded_string, size_t encoded_length)
{
    extern const UInt8 geohash_base32_decode_lookup_table[256];

    Encoded result;

    for (size_t i = 0; i < encoded_length; ++i)
    {
        const UInt8 c = static_cast<UInt8>(encoded_string[i]);
        const UInt8 decoded = geohash_base32_decode_lookup_table[c] & 0x1F;
        result[i * 5 + 4] = (decoded >> 0) & 0x01;
        result[i * 5 + 3] = (decoded >> 1) & 0x01;
        result[i * 5 + 2] = (decoded >> 2) & 0x01;
        result[i * 5 + 1] = (decoded >> 3) & 0x01;
        result[i * 5 + 0] = (decoded >> 4) & 0x01;
    }

    return result;
}

inline Float64 getMaxSpan(CoordType type)
{
    if (type == LONGITUDE)
    {
        return LON_MAX - LON_MIN;
    }

    return LAT_MAX - LAT_MIN;
}

inline Float64 getSpan(UInt8 precision, CoordType type)
{
    const auto bits = singleCoordBitsPrecision(precision, type);
    // since every bit of precision divides span by 2, divide max span by 2^bits.
    return ldexp(getMaxSpan(type), -1 * bits);
}

inline UInt8 geohashPrecision(UInt8 precision)
{
    if (precision == 0 || precision > MAX_PRECISION)
    {
        precision = MAX_PRECISION;
    }

    return precision;
}

inline size_t geohashEncodeImpl(Float64 longitude, Float64 latitude, UInt8 precision, char * out)
{
    const Encoded combined = merge(
                encodeCoordinate(longitude, LON_MIN, LON_MAX, singleCoordBitsPrecision(precision, LONGITUDE)),
                encodeCoordinate(latitude, LAT_MIN, LAT_MAX, singleCoordBitsPrecision(precision, LATITUDE)),
                precision);

    base32Encode(combined, precision, out);

    return precision;
}

}

namespace DB
{

namespace ErrorCodes
{
extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace GeoUtils
{

size_t geohashEncode(Float64 longitude, Float64 latitude, UInt8 precision, char * out)
{
    precision = geohashPrecision(precision);
    return geohashEncodeImpl(longitude, latitude, precision, out);
}

void geohashDecode(const char * encoded_string, size_t encoded_len, Float64 * longitude, Float64 * latitude)
{
    const UInt8 precision = std::min(encoded_len, static_cast<size_t>(MAX_PRECISION));
    if (precision == 0)
    {
        return;
    }

    Encoded lat_encoded, lon_encoded;
    std::tie(lon_encoded, lat_encoded) = split(base32Decode(encoded_string, precision), precision);

    *longitude = decodeCoordinate(lon_encoded, LON_MIN, LON_MAX, singleCoordBitsPrecision(precision, LONGITUDE));
    *latitude = decodeCoordinate(lat_encoded, LAT_MIN, LAT_MAX, singleCoordBitsPrecision(precision, LATITUDE));
}

GeohashesInBoxPreparedArgs geohashesInBoxPrepare(const Float64 longitude_min,
                                              const Float64 latitude_min,
                                              const Float64 longitude_max,
                                              const Float64 latitude_max,
                                              UInt8 precision)
{
    precision = geohashPrecision(precision);

    if (longitude_max < longitude_min || latitude_max < latitude_min)
    {
        return {};
    }

    const auto lon_step = getSpan(precision, LONGITUDE);
    const auto lat_step = getSpan(precision, LATITUDE);

    // align max to the right(or up) border of geohash grid cell to ensure that cell is in result.
    Float64 lon_min = floor(longitude_min / lon_step) * lon_step;
    Float64 lat_min = floor(latitude_min / lat_step) * lat_step;
    Float64 lon_max = ceil(longitude_max / lon_step) * lon_step;
    Float64 lat_max = ceil(latitude_max / lat_step) * lat_step;

    const auto lon_span = lon_max - lon_min;
    const auto lat_span = lat_max - lat_min;
    // in case of a very small (or zero) span, produce at least 1 item.
    const auto items_count = std::max(size_t{1}, static_cast<size_t>(ceil(lon_span/lon_step * lat_span/lat_step)));

    return GeohashesInBoxPreparedArgs{
            items_count,
            precision,
            lon_min,
            lat_min,
            lon_max,
            lat_max,
            lon_step,
            lat_step
    };
}

UInt64 geohashesInBox(const GeohashesInBoxPreparedArgs & args, char * out)
{
    if (args.items_count == 0
            || args.precision == 0
            || args.precision > MAX_PRECISION
            || args.latitude_min > args.latitude_max
            || args.longitude_min > args.longitude_max
            || args.longitude_step <= 0
            || args.latitude_step <= 0)
    {
        return 0;
    }

    UInt64 items = 0;
    for (auto lon = args.longitude_min; lon < args.longitude_max; lon += args.longitude_step)
    {
        for (auto lat = args.latitude_min; lat < args.latitude_max; lat += args.latitude_step)
        {
            assert(items <= args.items_count);

            size_t l = geohashEncodeImpl(lon, lat, args.precision, out);
            out += l;
            *out = '\0';
            ++out;

            ++items;
        }
    }

    if (items == 0)
    {
        size_t l = geohashEncodeImpl(args.longitude_min, args.latitude_min, args.precision, out);
        out += l;
        *out = '\0';
        ++out;

        ++items;
    }

    return items;
}

}

}
