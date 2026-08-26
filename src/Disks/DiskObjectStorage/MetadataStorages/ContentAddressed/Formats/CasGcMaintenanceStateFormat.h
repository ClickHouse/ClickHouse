#pragma once
#include <base/types.h>
#include <cstddef>
#include <string_view>

namespace DB::Cas
{

constexpr size_t kMaxGcMaintenanceCursorBytes = 64 * 1024;

/// Durable, leak-only progress for namespace maintenance. It has no GC authority fields.
struct GcMaintenanceState
{
    String janitor_cursor;
    bool operator==(const GcMaintenanceState &) const = default;
};

String encodeGcMaintenanceState(const GcMaintenanceState & state);
GcMaintenanceState decodeGcMaintenanceState(std::string_view data);

}
