#pragma once

#include <map>
#include <vector>
#include <common/types.h>


namespace DB
{

size_t geohashEncode(Float64 longitude, Float64 latitude, uint8_t precision, char * out);

void geohashDecode(const char * encoded_string, size_t encoded_len, Float64 * longitude, Float64 * latitude);

std::vector<std::pair<Float64, Float64>> geohashCoverBox(
    Float64 longitude_min,
    Float64 latitude_min,
    Float64 longitude_max,
    Float64 latitude_max,
    uint8_t precision,
    UInt32 max_items = 0);

struct GeohashesInBoxPreparedArgs
{
    UInt64 items_count = 0;

    UInt32 longitude_items = 0;
    UInt32 latitude_items = 0;

    Float64 longitude_min = 0.0;
    Float64 latitude_min = 0.0;

    Float64 longitude_step = 0.0;
    Float64 latitude_step = 0.0;

    uint8_t precision = 0;
};

GeohashesInBoxPreparedArgs geohashesInBoxPrepare(
    const Float64 longitude_min,
    const Float64 latitude_min,
    Float64 longitude_max,
    Float64 latitude_max,
    uint8_t precision);

UInt64 geohashesInBox(const GeohashesInBoxPreparedArgs & args, char * out);

}
