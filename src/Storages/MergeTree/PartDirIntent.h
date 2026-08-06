#pragma once

#include <cstdint>

namespace DB
{

/// Whether a data part object is constructed over an existing directory or over a fresh one that is
/// about to be written. Stated explicitly at every construction site (no default), so a write path
/// cannot silently seed in-memory state from a stale leftover of an interrupted operation.
enum class PartDirIntent : uint8_t
{
    /// Directory contents are authoritative: probe the mark type and `txn_version.txt`, seed the
    /// packed archive reader.
    OpenExisting,
    /// Directory is guaranteed clean by the claim (`MergeTreeData::claimTemporaryPartDirectory`):
    /// granularity comes from the settings, nothing is read from disk.
    CreateFresh,
    /// Existing part whose metadata arrives out of band (e.g. from another replica) instead of being
    /// probed. Reads nothing, so the part is not usable until the caller populates the storage and
    /// part state from its own source; unlike `CreateFresh` the directory may exist.
    OpenExistingWithoutProbing,
};

}
