#pragma once

#include <base/types.h>

/// Common type for priority values.
/// Separate type (rather than `Int64` is used just to avoid implicit conversion errors and to default-initialize
struct Priority
{
    using Value = Int64;
    Value value = 0; /// Note that lower value means higher priority.
    constexpr operator Value() const { return value; } /// NOLINT
};
