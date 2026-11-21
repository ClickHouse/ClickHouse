#pragma once

#include <base/types.h>
#include <Functions/CancellationBudget.h>


namespace DB
{

size_t geohashEncode(Float64 longitude, Float64 latitude, uint8_t precision, char * out);

void geohashDecode(const char * encoded_string, size_t encoded_len, Float64 * longitude, Float64 * latitude);

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
    Float64 longitude_min,
    Float64 latitude_min,
    Float64 longitude_max,
    Float64 latitude_max,
    uint8_t precision);

/// Writes the geohashes covering the prepared box to `out`, charging `budget` for every one of them so that
/// the expansion of a single box can observe a timeout or `KILL QUERY` while it runs. A box holding no
/// geohashes is charged once, so the cost of a row is counted whether or not it produces anything.
UInt64 geohashesInBox(const GeohashesInBoxPreparedArgs & args, char * out, CancellationBudget & budget);

}
