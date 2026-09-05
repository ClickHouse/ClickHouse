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

    /// How many times an in-place replacement has been observed at this table root. Every
    /// `TableStateSnapshot` records the value in effect when it was pinned, so that a later
    /// reopen of the pinned metadata file can tell whether it is still looking at the same
    /// incarnation of the table.
    UInt64 getIncarnation() const
    {
        SharedLockGuard lock(mutex);
        return incarnation;
    }

    /// The `table-uuid` under which a file pinned during `pinned_incarnation` may be used as a
    /// metadata *content* cache key, or `std::nullopt` when it may not be.
    ///
    /// A `TableStateSnapshot` pins one metadata file from analysis through execution, and the
    /// content read for it at execution time must be the content of the incarnation that was
    /// analyzed. The trusted UUID is shared and mutable, so by then a concurrent query may have
    /// moved it to a table that replaced the analyzed one in place - and a replacement may reuse
    /// the very same metadata file path, so the path proves nothing. Only an unchanged
    /// incarnation does: while it holds, the trusted UUID is still the one that was in effect at
    /// the pin. Otherwise the caller reads the file from storage instead of probing the cache
    /// with a UUID that may belong to another table.
    ///
    /// A pin that carries no incarnation - one deserialized on another server, whose cell tracks
    /// its own incarnations - is never cache-keyed.
    std::optional<String> getForPinnedIncarnation(std::optional<UInt64> pinned_incarnation) const
    {
        SharedLockGuard lock(mutex);
        if (pinned_incarnation != incarnation)
            return std::nullopt;
        return uuid;
    }

    /// Commit the `table-uuid` that was just read from the metadata file at
    /// {`metadata_version`, `metadata_file_path`}, and record that file as validated.
    ///
    /// Returns true if the trusted value actually changed, i.e. the table was replaced in place.
    bool commitValidated(
        std::optional<String> new_uuid,
        Int32 metadata_version,
        const String & metadata_file_path,
        const std::optional<MetadataFileIdentity> & identity,
        std::optional<UInt64> content_token)
    {
        std::lock_guard lock(mutex);
        bool changed = uuid != new_uuid;
        /// A table with no `table-uuid` - possible only in format-version 1 - carries no identity
        /// of its own, so a replacement can only be seen in the metadata file. A recreated table
        /// restarts the version numbering and rewrites `metadata.json`, so a version that does not
        /// advance past the last validated one, reached through a different file or through the
        /// same path with a different identity, is the replacement token for these tables.
        /// A storage that cannot report the identity of an unchanged path - HDFS synthesizes a
        /// weak etag, and the identity is then absent - leaves the content of the file as the only
        /// proof, and the caller has just read it: a metadata file is immutable, so the same path
        /// answering with a different content token is another table that took the path over.
        if (!changed && !uuid.has_value() && !new_uuid.has_value() && last_validated.has_value())
        {
            changed = metadata_version <= last_validated->version
                && (metadata_file_path != last_validated->path || identity != last_validated->identity);

            if (!changed && metadata_file_path == last_validated->path && last_validated->content_token.has_value()
                && content_token.has_value())
                changed = *content_token != *last_validated->content_token;
        }
        uuid = std::move(new_uuid);
        if (changed)
            ++incarnation;
        last_validated = ValidatedMetadataFile{metadata_version, metadata_file_path, identity, content_token};
        return changed;
    }

    /// Whether the `table-uuid` of the selected metadata file has to be re-read from storage
    /// before it can be trusted as a content cache key.
    ///
    /// Only one thing proves that the selected file still describes the table whose `table-uuid`
    /// is trusted: it being byte-for-byte the file that was validated last - the same path,
    /// carrying the identity the listing reported back then. Replacing a table in place rewrites
    /// `metadata.json`, which changes that identity, and a file whose identity the storage cannot
    /// report is revalidated rather than assumed unchanged.
    ///
    /// A higher version number proves nothing: a table recreated at the same root may start above
    /// every version validated so far, and trusting the version alone would leave the previous
    /// table's `table-uuid` recorded for a file that belongs to the replacement - after which
    /// every statement would reopen that file, see the other UUID and throw, with nothing left to
    /// re-read it. Any file other than the one validated last is therefore re-read; that is the
    /// same read the new version needs anyway.
    ///
    /// A table with no `table-uuid` at all is never content-cached under a UUID key, but it is
    /// still revalidated: `commitValidated` detects its replacement from the metadata file that
    /// comes back, and it can only do so when the file is actually re-read.
    bool needsRevalidation(const String & metadata_file_path, const std::optional<MetadataFileIdentity> & identity) const
    {
        SharedLockGuard lock(mutex);
        if (!last_validated.has_value())
            return true;
        return !identity.has_value() || !last_validated->identity.has_value() || metadata_file_path != last_validated->path
            || *identity != *last_validated->identity;
    }


    /// Record the metadata file whose own `table-uuid` is trusted, because it was just read from
    /// that file or because it is the unchanged file that was validated before.
    ///
    /// The content token of an unchanged file is the token that was recorded for it, so recording
    /// the same file again keeps it: the caller that took this path did not read the content and
    /// has no token to offer.
    void markValidated(Int32 metadata_version, const String & metadata_file_path, const std::optional<MetadataFileIdentity> & identity)
    {
        std::lock_guard lock(mutex);
        std::optional<UInt64> content_token;
        if (last_validated.has_value() && last_validated->path == metadata_file_path && last_validated->identity == identity)
            content_token = last_validated->content_token;
        last_validated = ValidatedMetadataFile{metadata_version, metadata_file_path, identity, content_token};
    }

    struct ValidatedMetadataFile
    {
        Int32 version;
        String path;
        std::optional<MetadataFileIdentity> identity;
        /// See `computeMetadataContentToken`. Absent for a file whose content was never read here.
        std::optional<UInt64> content_token;
    };

    /// The metadata file that was validated last, so that a statement about to publish can read
    /// that very file again and see whether it still describes the table it validated.
    /// See `checkStorageStillHoldsValidatedTable`.
    std::optional<ValidatedMetadataFile> getValidatedFile() const
    {
        SharedLockGuard lock(mutex);
        return last_validated;
    }

private:

    mutable SharedMutex mutex;
    std::optional<String> uuid TSA_GUARDED_BY(mutex);
    UInt64 incarnation TSA_GUARDED_BY(mutex) = 0;
    /// The exact metadata file that was validated last, whichever version it carries.
    std::optional<ValidatedMetadataFile> last_validated TSA_GUARDED_BY(mutex);
};

using TrustedTableUuidPtr = std::shared_ptr<TrustedTableUuid>;

}

#endif
