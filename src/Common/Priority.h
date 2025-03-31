#pragma once

#include <base/types.h>

/// Common type for priority values.
/// Separate type (rather than `Int64` is used just to avoid implicit conversion errors and to default-initialize
struct Priority
{
    Int64 value = 0; /// Note that lower value means higher priority.
    constexpr operator Int64() const { return value; } /// NOLINT
};
