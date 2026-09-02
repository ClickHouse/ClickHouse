#pragma once

#include <base/defines.h>
#include <base/types.h>

#include <fmt/format.h>

template <>
struct fmt::formatter<Int8> : fmt::formatter<int8_t>
{
};


namespace std
{
std::string to_string(Int8 v); /// NOLINT (cert-dcl58-cpp)
}
