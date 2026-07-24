#pragma once

#include <cstdint>

namespace DB
{

/// Declares whether a data part object is constructed over an existing directory whose contents are
/// authoritative, or over a fresh directory that is about to be written. Every construction site must
/// state its intent explicitly (there is no default): a write path that wrongly used `OpenExisting`
/// could seed in-memory state (mark type, packed archive index, transaction metadata) from a stale
/// leftover of an interrupted operation. Write paths also remove such leftovers before construction
/// (see `MergeTreeData::claimTemporaryPartDirectory`), so this explicit intent is a second, independent
/// layer of protection.
enum class PartDirIntent : uint8_t
{
    /// Loading or attaching a part whose directory contents are authoritative: probe the filesystem
    /// for the mark type, the transaction version metadata file, seed the packed archive reader.
    OpenExisting,
    /// Writing a brand-new part into a directory guaranteed clean by the temporary
    /// directory claim (`MergeTreeData::claimTemporaryPartDirectory`): initialize
    /// granularity from settings, read nothing from disk.
    CreateFresh,
    /// Constructing a part object for an existing part whose metadata arrives out of band instead of
    /// being probed from disk (e.g. deserialized from another replica). Construction reads nothing and
    /// initializes nothing from the directory, so the object is NOT usable as built: the caller must
    /// supply the part format itself and then populate the storage and part state (packed archive
    /// index, granularity, checksums, transaction metadata) from its out-of-band source before the
    /// part is used; otherwise the first read fails (e.g. a packed file read throws). Unlike
    /// `CreateFresh`, the directory may legitimately exist, so the clean-directory assertion does
    /// not apply.
    OpenExistingWithoutProbing,
};

}
