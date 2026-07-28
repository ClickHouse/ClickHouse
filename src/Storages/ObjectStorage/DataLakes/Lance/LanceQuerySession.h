#pragma once

#include "config.h"

#if USE_LANCE

#include <Common/StopToken.h>
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
    QuerySession();

    static std::shared_ptr<QuerySession> get(const ContextPtr & context);

    /// Returns an existing open handle or opens once. Thread-safe.
    DatasetHandle getOrOpen(const DatasetOptions & options);

    /// Record the immutable snapshot pinned for this identity.
    void pinSnapshot(const String & identity_key, const TableStateSnapshot & snapshot);

    std::optional<TableStateSnapshot> getPinnedSnapshot(const String & identity_key) const;

    /// Require a previously opened handle for the pinned snapshot.
    DatasetHandle getPinned(const DatasetOptions & options, const TableStateSnapshot & pinned_snapshot);

    size_t openCount() const;
    const CancelHandlePtr & getCancelHandle() const { return cancel_handle; }

    void setReuseEnabled(bool enabled) { reuse_enabled = enabled; }
    bool getReuseEnabled() const { return reuse_enabled; }

    /// When true, LanceMetadata::iterate produces a single full-dataset pack
    /// (LIMIT pushdown and ordered reads). Set from
    /// ReadFromObjectStorageStep before createFileIterator.
    void setForceSingleFragmentPack(bool enabled) { force_single_fragment_pack = enabled; }
    bool getForceSingleFragmentPack() const { return force_single_fragment_pack; }

private:
    void bindToQueryCancellation(const ContextPtr & context);

    mutable std::mutex mutex;
    std::unordered_map<String, DatasetHandle> open_datasets;
    std::unordered_map<String, TableStateSnapshot> pinned_snapshots;
    size_t open_count = 0;
    bool reuse_enabled = true;
    bool force_single_fragment_pack = false;
    CancelHandlePtr cancel_handle;
    std::unique_ptr<StopCallback> query_cancel_callback;
};

}

#endif
