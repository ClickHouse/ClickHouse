#pragma once
#include "config.h"

#if USE_AVRO

#include <mutex>
#include <optional>
#include <base/defines.h>
#include <base/types.h>
#include <Common/SharedLockGuard.h>
#include <Common/SharedMutex.h>

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

    /// Returns true if the value actually changed, i.e. the table was replaced in place.
    bool set(std::optional<String> new_uuid)
    {
        std::lock_guard lock(mutex);
        if (uuid == new_uuid)
            return false;
        uuid = std::move(new_uuid);
        return true;
    }

    /// Whether the `table-uuid` of the selected metadata file has to be re-read from storage
    /// before it can be trusted as a content cache key.
    ///
    /// Iceberg writers advance the metadata version strictly, so a selected version that is
    /// strictly greater than the last validated one cannot have been produced by a replacement
    /// that restarted the numbering, and the extra uncached read is skipped. A version that does
    /// not advance is the signature of a possible in-place replacement - a recreated table
    /// restarts the numbering, and the `<V>-<random-uuid>.metadata.json` naming even lets it
    /// reuse a version number under a different path - and is revalidated.
    ///
    /// A table with no `table-uuid` at all is never content-cached under a UUID key, so there is
    /// nothing to revalidate.
    bool needsRevalidation(Int32 metadata_version, const String & /*metadata_file_path*/) const
    {
        SharedLockGuard lock(mutex);
        if (!uuid.has_value())
            return false;
        if (!last_validated.has_value())
            return true;
        return metadata_version <= last_validated->version;
    }

    /// Record the metadata file whose own `table-uuid` was just confirmed to be the trusted one.
    void markValidated(Int32 metadata_version, const String & metadata_file_path)
    {
        std::lock_guard lock(mutex);
        last_validated = ValidatedMetadataFile{metadata_version, metadata_file_path};
    }

private:
    struct ValidatedMetadataFile
    {
        Int32 version;
        String path;
    };

    mutable SharedMutex mutex;
    std::optional<String> uuid TSA_GUARDED_BY(mutex);
    std::optional<ValidatedMetadataFile> last_validated TSA_GUARDED_BY(mutex);
};

using TrustedTableUuidPtr = std::shared_ptr<TrustedTableUuid>;

}

#endif
