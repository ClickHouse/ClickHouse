#pragma once

#include <IO/DoubleConverter.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <locale>

namespace DB
{

/// `boost::geometry::wkt` writes coordinates with `operator<<`, which uses 6 significant digits by default.
/// This facet writes them with the shortest representation that round-trips, as the text output of `Float64` does.
class ShortestFloatNumPut : public std::num_put<char>
{
protected:
    iter_type do_put(iter_type out, std::ios_base & /*str*/, char_type /*fill*/, double value) const override
    {
        DoubleConverter<false>::BufferType buffer;
        size_t length = writeFloatTextFastPath(value, buffer, false);
        return std::copy(buffer, buffer + length, out);
    }
};

}
