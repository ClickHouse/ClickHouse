#pragma once

#include <cstdint>

namespace DB
{

/// Declares whether a data part object is constructed over an existing directory whose contents are
/// authoritative, or over a fresh directory that is about to be written. Every construction site must
/// state its intent explicitly (there is no default): probing a directory that is not authoritative,
/// e.g. a stale leftover of an interrupted operation, would seed in-memory state from garbage.
enum class PartDirIntent : uint8_t
{
    /// Loading or attaching a part whose directory contents are authoritative:
    /// probe the filesystem for the mark type, seed the packed archive reader.
    OpenExisting,
    /// Writing a brand-new part into a directory guaranteed clean by the temporary
    /// directory claim (`MergeTreeData::claimTemporaryPartDirectory`): initialize
    /// granularity from settings, read nothing from disk.
    CreateFresh,
};

}
