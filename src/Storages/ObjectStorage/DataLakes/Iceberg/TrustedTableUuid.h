#pragma once
#include "config.h"

#if USE_AVRO

#include <mutex>
#include <optional>
#include <base/defines.h>
#include <base/types.h>
#include <Common/SharedLockGuard.h>
#include <Common/SharedMutex.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>

namespace DB::Iceberg
{

/// The `table-uuid` that is trusted enough to be used as a metadata *content* cache key.
///
/// The UUID exists in that key to tell apart two different tables that occupy the same storage
/// path at different times, so it may not be pinned to the value observed when the table was
/// opened: an external writer can drop and recreate the table at the same root with a new
/// `table-uuid`, and a reused `IcebergMetadata` object would then read the previous table's
/// `metadata.json` out of the cache. `IcebergMetadata::update` refreshes the value here.
///
/// The cell is shared (via `std::shared_ptr`) by every copy of `PersistentTableComponents`
/// describing the same table, and is guarded by a `SharedMutex`, so a refresh is observed by all
/// of them and the "thread-safe or immutable" contract of that struct holds.
class TrustedTableUuid
{
public:
    explicit TrustedTableUuid(std::optional<String> uuid_)
        : uuid(std::move(uuid_))
    {
    }

    std::optional<String> get() const
    {
        SharedLockGuard lock(mutex);
        return uuid;
    }

    /// The `table-uuid` under which `metadata_file_path` may be used as a metadata *content*
    /// cache key, or `std::nullopt` when it may not be.
    ///
    /// A `TableStateSnapshot` pins one metadata file from analysis through execution, and the
    /// content read for it at execution time must be the content of the incarnation that was
    /// analyzed. The trusted UUID is shared and mutable, so by then a concurrent query may have
    /// moved it to a table that replaced the analyzed one in place - and a replacement can put a
    /// different file at the very same path. Probing the cache with the moved UUID would then
    /// return the wrong table's metadata, so the file is only cache-keyed while it is still the
    /// one this cell has validated; otherwise the caller reads it from storage.
    std::optional<String> getForValidatedFile(const String & metadata_file_path) const
    {
        SharedLockGuard lock(mutex);
        if (!last_validated.has_value() || last_validated->path != metadata_file_path)
            return std::nullopt;
        return uuid;
    }

    /// Commit the `table-uuid` that was just read from the metadata file at
    /// {`metadata_version`, `metadata_file_path`}, and record that file as validated.
    ///
    /// Returns true if the trusted value actually changed, i.e. the table was replaced in place.
    /// A replacement resets the watermark to the recreated table's own version, which is normally
    /// lower: keeping the previous table's higher watermark would force an uncached read on every
    /// query until the new table caught up with it.
    bool commitValidated(
        std::optional<String> new_uuid,
        Int32 metadata_version,
        const String & metadata_file_path,
        const std::optional<MetadataFileIdentity> & identity)
    {
        std::lock_guard lock(mutex);
        bool changed = uuid != new_uuid;
        uuid = std::move(new_uuid);
        if (changed)
            last_validated = ValidatedMetadataFile{metadata_version, metadata_file_path, identity};
        else
            advanceWatermark(metadata_version, metadata_file_path, identity);
        return changed;
    }

    /// Whether the `table-uuid` of the selected metadata file has to be re-read from storage
    /// before it can be trusted as a content cache key.
    ///
    /// Iceberg writers advance the metadata version strictly, so a selected version that is
    /// strictly greater than the last validated one cannot have been produced by a replacement
    /// that restarted the numbering, and the extra uncached read is skipped. A version that does
    /// not advance is the signature of a possible in-place replacement - a recreated table
    /// restarts the numbering, and the `<V>-<random-uuid>.metadata.json` naming even lets it
    /// reuse a version number under a different path.
    ///
    /// The steady state - the same query re-selecting the very same file, over and over, with no
    /// writer in sight - must stay free, or the metadata content cache would be defeated on every
    /// read. So a non-advancing version is still trusted when the selected file is byte-for-byte
    /// the one that was validated: same path, and the same size and modification time as the
    /// listing reported back then. Replacing a table in place rewrites `metadata.json`, which
    /// changes that identity. A file whose identity the storage cannot report is always
    /// revalidated rather than assumed unchanged.
    ///
    /// A table with no `table-uuid` at all is never content-cached under a UUID key, so there is
    /// nothing to revalidate.
    bool needsRevalidation(
        Int32 metadata_version, const String & metadata_file_path, const std::optional<MetadataFileIdentity> & identity) const
    {
        SharedLockGuard lock(mutex);
        if (!uuid.has_value())
            return false;
        if (!last_validated.has_value())
            return true;
        if (metadata_version > last_validated->version)
            return false;
        return !identity.has_value() || !last_validated->identity.has_value() || metadata_file_path != last_validated->path
            || *identity != *last_validated->identity;
    }

    /// Record the metadata file whose own `table-uuid` is trusted, either because it was just
    /// read from that file or because the selected version advanced strictly past the previously
    /// validated one, which no replacement restarting the numbering can do.
    ///
    /// The watermark only ever moves forward here: two concurrent `update` calls can observe
    /// different metadata files, and the older observation must not undo the newer one by
    /// lowering the watermark and letting an already-seen version pass unchecked. Only a
    /// confirmed replacement, through `commitValidated`, moves it back.
    void markValidated(Int32 metadata_version, const String & metadata_file_path, const std::optional<MetadataFileIdentity> & identity)
    {
        std::lock_guard lock(mutex);
        advanceWatermark(metadata_version, metadata_file_path, identity);
    }

private:
    void advanceWatermark(
        Int32 metadata_version, const String & metadata_file_path, const std::optional<MetadataFileIdentity> & identity) TSA_REQUIRES(mutex)
    {
        if (!last_validated.has_value() || last_validated->version <= metadata_version)
            last_validated = ValidatedMetadataFile{metadata_version, metadata_file_path, identity};
    }

    struct ValidatedMetadataFile
    {
        Int32 version;
        String path;
        std::optional<MetadataFileIdentity> identity;
    };

    mutable SharedMutex mutex;
    std::optional<String> uuid TSA_GUARDED_BY(mutex);
    std::optional<ValidatedMetadataFile> last_validated TSA_GUARDED_BY(mutex);
};

using TrustedTableUuidPtr = std::shared_ptr<TrustedTableUuid>;

}

#endif
