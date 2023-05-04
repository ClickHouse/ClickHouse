#pragma once
#include <cstddef>

constexpr size_t KiB = 1024;
constexpr size_t MiB = 1024 * KiB;
constexpr size_t GiB = 1024 * MiB;

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-identifier"

// NOLINTBEGIN(google-runtime-int)
constexpr size_t operator"" _KiB(unsigned long long val) { return val * KiB; }
constexpr size_t operator"" _MiB(unsigned long long val) { return val * MiB; }
constexpr size_t operator"" _GiB(unsigned long long val) { return val * GiB; }
// NOLINTEND(google-runtime-int)

#pragma clang diagnostic pop
