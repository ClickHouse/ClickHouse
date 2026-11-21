#pragma once

#include <Common/Logger.h>
#include <base/types.h>

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

/// Update some settings defaults to avoid some known issues.
void applySettingsQuirks(Settings & settings, LoggerPtr log = nullptr);

/// Verify that some settings have sane values. Alters the value to a reasonable one if not
void doSettingsSanityCheckClamp(Settings & settings, LoggerPtr log);
}
