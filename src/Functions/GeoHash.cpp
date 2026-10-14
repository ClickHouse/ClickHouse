#include <array>
#include <base/defines.h>
#include <cmath>
#include <Functions/GeoHash.h>


namespace DB
{

namespace
{

const char geohash_base32_encode_lookup_table[32] = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm',
    'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
    'y', 'z',
};

// TODO: this could be halved by excluding 128-255 range.
const uint8_t geohash_base32_decode_lookup_table[256] = {
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
const size_t MAX_BITS = (MAX_PRECISION * BITS_PER_SYMBOL * 3) / 2;
const Float64 LON_MIN = -180;
const Float64 LON_MAX = 180;
const Float64 LAT_MIN = -90;
const Float64 LAT_MAX = 90;

using Encoded = std::array<uint8_t, MAX_BITS>;

enum CoordType
{
    LATITUDE,
    LONGITUDE,
};

inline uint8_t singleCoordBitsPrecision(uint8_t precision, CoordType type)
{
    // Single coordinate occupies only half of the total bits.
    const uint8_t bits = (precision * BITS_PER_SYMBOL) / 2;
    if (precision & 0x1 && type == LONGITUDE)
    {
        return bits + 1;
    }

    return bits;
}

inline Encoded encodeCoordinate(Float64 coord, Float64 min, Float64 max, uint8_t bits)
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

/// Drop the odd bits and pack the even ones down, so `0b_d_c_b_a` becomes `0b dcba`.
inline UInt64 compactEvenBits(UInt64 x)
{
    x &= 0x5555555555555555ULL;
    x = (x | (x >> 1)) & 0x3333333333333333ULL;
    x = (x | (x >> 2)) & 0x0f0f0f0f0f0f0f0fULL;
    x = (x | (x >> 4)) & 0x00ff00ff00ff00ffULL;
    x = (x | (x >> 8)) & 0x0000ffff0000ffffULL;
    x = (x | (x >> 16)) & 0x00000000ffffffffULL;
    return x;
}

/// `coord` holds `bits` bits, most significant first.
inline Float64 decodeCoordinate(UInt64 coord, uint8_t bits, Float64 min, Float64 max)
{
    Float64 mid = (max + min) / 2;
    for (uint8_t i = 0; i < bits; ++i)
    {
        if ((coord >> (bits - 1 - i)) & 1)
            min = mid;
        else
            max = mid;

        mid = (max + min) / 2;
    }

    return mid;
}

inline Encoded merge(const Encoded & encodedLon, const Encoded & encodedLat, uint8_t precision)
{
    Encoded result;
    result.fill(0);

    uint8_t bits = (precision * BITS_PER_SYMBOL) / 2;
    chassert(bits < 255);
    uint8_t i = 0;
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

inline void base32Encode(const Encoded & binary, uint8_t precision, char * out)
{
    extern const char geohash_base32_encode_lookup_table[32];

    for (size_t i = 0; i < precision * BITS_PER_SYMBOL; i += BITS_PER_SYMBOL)
    {
        uint8_t v = binary[i];
        v <<= 1;
        v |= binary[i + 1];
        v <<= 1;
        v |= binary[i + 2];
        v <<= 1;
        v |= binary[i + 3];
        v <<= 1;
        v |= binary[i + 4];

        chassert(v < 32);

        *out = geohash_base32_encode_lookup_table[v];
        ++out;
    }
}

inline Float64 getMaxSpan(CoordType type)
{
    if (type == LONGITUDE)
    {
        return LON_MAX - LON_MIN;
    }

    return LAT_MAX - LAT_MIN;
}

inline Float64 getSpan(uint8_t precision, CoordType type)
{
    const auto bits = singleCoordBitsPrecision(precision, type);
    // since every bit of precision divides span by 2, divide max span by 2^bits.
    return ldexp(getMaxSpan(type), -1 * bits);
}

inline uint8_t geohashPrecision(uint8_t precision)
{
    if (precision == 0 || precision > MAX_PRECISION)
        precision = MAX_PRECISION;

    return precision;
}

inline size_t geohashEncodeImpl(Float64 longitude, Float64 latitude, uint8_t precision, char * out)
{
    const Encoded combined = merge(
                encodeCoordinate(longitude, LON_MIN, LON_MAX, singleCoordBitsPrecision(precision, LONGITUDE)),
                encodeCoordinate(latitude, LAT_MIN, LAT_MAX, singleCoordBitsPrecision(precision, LATITUDE)),
                precision);

    base32Encode(combined, precision, out);

    return precision;
}

}


size_t geohashEncode(Float64 longitude, Float64 latitude, uint8_t precision, char * out)
{
    precision = geohashPrecision(precision);
    return geohashEncodeImpl(longitude, latitude, precision, out);
}

void geohashDecode(const char * encoded_string, size_t encoded_len, Float64 * longitude, Float64 * latitude)
{
    const uint8_t precision = static_cast<uint8_t>(std::min(encoded_len, MAX_PRECISION));
    if (precision == 0)
    {
        // Empty string is converted to (0, 0)
        *longitude = 0;
        *latitude = 0;
        return;
    }

    /// At most 12 symbols of 5 bits, so the whole geohash fits in one word. That also avoids a local
    /// buffer, which would put a `-fstack-protector-strong` canary on this per-row function.
    UInt64 bits = 0;
    for (size_t i = 0; i < precision; ++i)
    {
        const uint8_t c = static_cast<uint8_t>(encoded_string[i]);
        bits = (bits << BITS_PER_SYMBOL) | (geohash_base32_decode_lookup_table[c] & 0x1F);
    }

    /// Longitude takes the even bit positions counting from the most significant, latitude the odd ones.
    /// Counting from the least significant instead, which is what `compactEvenBits` works on, swaps the
    /// two whenever the total number of bits is even.
    const uint8_t total_bits = static_cast<uint8_t>(precision * BITS_PER_SYMBOL);
    const UInt64 even = compactEvenBits(bits);
    const UInt64 odd = compactEvenBits(bits >> 1);
    const UInt64 lon_encoded = (total_bits & 1) ? even : odd;
    const UInt64 lat_encoded = (total_bits & 1) ? odd : even;

    *longitude = decodeCoordinate(lon_encoded, singleCoordBitsPrecision(precision, LONGITUDE), LON_MIN, LON_MAX);
    *latitude = decodeCoordinate(lat_encoded, singleCoordBitsPrecision(precision, LATITUDE), LAT_MIN, LAT_MAX);
}

GeohashesInBoxPreparedArgs geohashesInBoxPrepare(
    Float64 longitude_min,
    Float64 latitude_min,
    Float64 longitude_max,
    Float64 latitude_max,
    uint8_t precision)
{
    precision = geohashPrecision(precision);

    if (longitude_max < longitude_min
        || latitude_max < latitude_min
        || std::isnan(longitude_min)
        || std::isnan(longitude_max)
        || std::isnan(latitude_min)
        || std::isnan(latitude_max))
    {
        return {};
    }

    auto saturate = [](Float64 & value, Float64 min, Float64 max)
    {
        if (value < min)
            value = min;
        else if (value > max)
            value = max;
    };

    saturate(longitude_min, LON_MIN, LON_MAX);
    saturate(longitude_max, LON_MIN, LON_MAX);
    saturate(latitude_min, LAT_MIN, LAT_MAX);
    saturate(latitude_max, LAT_MIN, LAT_MAX);

    Float64 lon_step = getSpan(precision, LONGITUDE);
    Float64 lat_step = getSpan(precision, LATITUDE);

    /// Align max to the right (or up) border of geohash grid cell to ensure that cell is in result.
    Float64 lon_min = floor(longitude_min / lon_step) * lon_step;
    Float64 lat_min = floor(latitude_min / lat_step) * lat_step;
    Float64 lon_max = ceil(longitude_max / lon_step) * lon_step;
    Float64 lat_max = ceil(latitude_max / lat_step) * lat_step;

    UInt32 lon_items = static_cast<UInt32>((lon_max - lon_min) / lon_step);
    UInt32 lat_items = static_cast<UInt32>((lat_max - lat_min) / lat_step);

    return GeohashesInBoxPreparedArgs
    {
        std::max<UInt64>(1, static_cast<UInt64>(lon_items) * lat_items),
        lon_items,
        lat_items,
        lon_min,
        lat_min,
        lon_step,
        lat_step,
        precision
    };
}

UInt64 geohashesInBox(const GeohashesInBoxPreparedArgs & args, char * out)
{
    if (args.precision == 0
        || args.precision > MAX_PRECISION
        || args.longitude_step <= 0
        || args.latitude_step <= 0)
    {
        return 0;
    }

    UInt64 items = 0;
    /// A zero item count on either axis makes the loops below produce nothing, while the other
    /// axis can still be ~2^32 wide. Skip them and let the fallback emit the single geohash.
    if (args.longitude_items != 0 && args.latitude_items != 0)
    {
        for (size_t i = 0; i < args.longitude_items; ++i)
        {
            for (size_t j = 0; j < args.latitude_items; ++j)
            {
                size_t length = geohashEncodeImpl(
                    args.longitude_min + args.longitude_step * static_cast<Float64>(i),
                    args.latitude_min + args.latitude_step * static_cast<Float64>(j),
                    args.precision,
                    out);

                out += length;
                ++items;
            }
        }
    }

    if (items == 0)
    {
        geohashEncodeImpl(args.longitude_min, args.latitude_min, args.precision, out);
        ++items;
    }

    return items;
}

}
