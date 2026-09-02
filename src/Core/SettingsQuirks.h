#pragma once

#include <Common/Logger.h>
#include <base/types.h>
#include <base/unit.h>

namespace DB
{

struct Settings;

/// A read buffer never needs to be larger than this. An out-of-range value would be passed
/// straight to the allocator when constructing a read buffer, tripping its size guard with a
/// `LOGICAL_ERROR` "Too large size passed to allocator". The read buffer size settings are
/// clamped to this value by `doSettingsSanityCheckClamp`, but code that can run on a context
/// where the clamp is not applied (`ApplicationType::CLIENT`) has to clamp at the consumption
/// site as well.
inline constexpr UInt64 MAX_SANE_READ_BUFFER_SIZE = 256 * 1024 * 1024; /// 256 MiB

/// A writer of temporary files never needs a larger buffer than this. `temporary_files_buffer_size`
/// is clamped to it by `doSettingsSanityCheckClamp`.
inline constexpr UInt64 MAX_TEMPORARY_FILES_BUFFER_SIZE = 1_GiB;

/// Bounds for the Confluent Schema Registry client. `doSettingsSanityCheckClamp` applies them to
/// the settings, and `getFormatSettings` applies them again on the way into `FormatSettings`,
/// because the clamp does not run for `ApplicationType::CLIENT`.
inline constexpr UInt64 MAX_SCHEMA_REGISTRY_TIMEOUT_SECONDS = 599;
inline constexpr UInt64 MAX_SCHEMA_REGISTRY_RETRIES = 20;
inline constexpr UInt64 MAX_SCHEMA_REGISTRY_INITIAL_BACKOFF_MS = 60000;

/// Clamp a `temporary_files_buffer_size` that arrived in a serialized query plan, which does not
/// go through `doSettingsSanityCheckClamp`. Warns when the value is reduced.
UInt64 clampTemporaryFilesBufferSize(UInt64 buffer_size);

/// Update some settings defaults to avoid some known issues.
void applySettingsQuirks(Settings & settings, LoggerPtr log = nullptr);

/// When make_distributed_plan is enabled, adjust the settings that control features distributed
/// query plans do not support yet. Applied whenever settings changes are applied to a context, so
/// that every context driving analysis, planning or task execution sees the adjusted values.
void adjustSettingsForMakeDistributedPlan(Settings & settings);

/// Verify that some settings have sane values. Alters the value to a reasonable one if not
void doSettingsSanityCheckClamp(Settings & settings, LoggerPtr log);
}
