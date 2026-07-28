#pragma once

#include "config.h"

#if USE_LANCE

#include <Storages/ObjectStorage/DataLakes/Lance/LanceWrapper.h>
#include <Interpreters/Context_fwd.h>

#include <mutex>
#include <optional>
#include <unordered_map>

namespace DB::Lance
{

/// Query-scoped registry: at most one open DatasetHandle per DatasetIdentity.
/// Attached to the query Context (via kitchen_sink) so analysis and execution share handles.
class QuerySession
{
public:
    static std::shared_ptr<QuerySession> get(const ContextPtr & context);

    /// Returns an existing open handle or opens once. Thread-safe.
    DatasetHandle getOrOpen(const DatasetOptions & options);

    /// Record the version pinned for this identity. Second pin with a different version is an error.
    void pinVersion(const String & identity_key, UInt64 version);

    std::optional<UInt64> getPinnedVersion(const String & identity_key) const;

    /// Require a previously opened handle for the pinned version.
    DatasetHandle getPinned(const DatasetOptions & options, UInt64 pinned_version);

    size_t openCount() const;

    void setReuseEnabled(bool enabled) { reuse_enabled = enabled; }
    bool getReuseEnabled() const { return reuse_enabled; }

    /// When true, LanceMetadata::iterate produces a single full-dataset pack
    /// (LIMIT pushdown, count() fast path, ordered reads). Set from
    /// ReadFromObjectStorageStep before createFileIterator.
    void setForceSingleFragmentPack(bool enabled) { force_single_fragment_pack = enabled; }
    bool getForceSingleFragmentPack() const { return force_single_fragment_pack; }

private:
    mutable std::mutex mutex;
    std::unordered_map<String, DatasetHandle> open_datasets;
    std::unordered_map<String, UInt64> pinned_versions;
    size_t open_count = 0;
    bool reuse_enabled = true;
    bool force_single_fragment_pack = false;
};

}

#endif
