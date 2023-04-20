#pragma once

#include <city.h>

#include <span>

namespace DB
{
CityHash_v1_0_2::uint128 CalculateCityHash128InLittleEndian(std::span<char> buffer);
}
