#pragma once

#include <Core/Types.h>

#include <pcg_random.hpp>


namespace DB
{

/// Slow random string. Useful for random names and things like this. Not for generating data.
String getRandomASCIIString(size_t length);
String getRandomASCIIString(size_t length, pcg64 & rng);

}
