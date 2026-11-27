#pragma once

#include <Common/ErrorCodes.h>

#include <span>
#include <string_view>
#include <utility>


namespace DB::ErrorCodes
{

namespace List
{

/// Defined in ErrorCodes.cpp with the list of error codes.

/// Error codes in the order of their declaration.
std::span<const ErrorCode> codes();

/// The name of every declared error code, `names()[i]` being the name of `codes()[i]`.
std::span<const std::string_view> names();

/// Pairs of (error code, its position in `codes()` and `names()`), sorted by the error code, to look codes up.
std::span<const std::pair<ErrorCode, size_t>> sortedCodes();

}

}
