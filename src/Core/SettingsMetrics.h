#pragma once

#include <Common/CurrentMetrics.h>

namespace CurrentMetrics
{
extern const Metric SettingsObjects; // NOLINT: metric, not a setting declaration.
extern const Metric SettingsImplementations; // NOLINT: metric, not a setting declaration.
extern const Metric SettingsDenseData; // NOLINT: metric, not a setting declaration.
extern const Metric SettingsSnapshotStates; // NOLINT: metric, not a setting declaration.
extern const Metric SettingsSnapshotChunks; // NOLINT: metric, not a setting declaration.
extern const Metric SettingsStructuralMemoryBytes; // NOLINT: metric, not a setting declaration.
}

namespace DB
{

/// Count each owned allocation once, independently of the number or thread of its owners.
/// These are requested structural bytes, not allocator usable sizes or dynamic field payloads.
enum class SettingsAllocationKind
{
    Implementation,
    DenseImplementation,
    DenseData,
    SnapshotState,
    SnapshotChunk,
};

inline CurrentMetrics::Metric settingsAllocationMetric(SettingsAllocationKind kind)
{
    switch (kind)
    {
        case SettingsAllocationKind::Implementation:
        case SettingsAllocationKind::DenseImplementation: return CurrentMetrics::SettingsImplementations;
        case SettingsAllocationKind::DenseData: return CurrentMetrics::SettingsDenseData;
        case SettingsAllocationKind::SnapshotState: return CurrentMetrics::SettingsSnapshotStates;
        case SettingsAllocationKind::SnapshotChunk: return CurrentMetrics::SettingsSnapshotChunks;
    }
    UNREACHABLE();
}

inline void accountSettingsStructuralBytes(Int64 bytes)
{
    CurrentMetrics::add(CurrentMetrics::SettingsStructuralMemoryBytes, bytes);
}

inline void accountSettingsObject(Int64 bytes, Int64 count)
{
    CurrentMetrics::add(CurrentMetrics::SettingsObjects, count);
    accountSettingsStructuralBytes(bytes);
}

inline void accountSettingsAllocation(SettingsAllocationKind kind, Int64 bytes, Int64 count)
{
    accountSettingsStructuralBytes(bytes);
    CurrentMetrics::add(settingsAllocationMetric(kind), count);
    if (kind == SettingsAllocationKind::DenseImplementation)
        CurrentMetrics::add(CurrentMetrics::SettingsDenseData, count);
}

}
