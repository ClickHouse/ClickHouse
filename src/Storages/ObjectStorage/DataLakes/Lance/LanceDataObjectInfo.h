#pragma once

#include "config.h"

#if USE_LANCE

#include <Storages/ObjectStorage/DataLakes/Lance/LanceTableStateSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceWrapper.h>
#include <Storages/ObjectStorage/IObjectIterator.h>

#include <vector>

namespace DB
{
class ReadBuffer;
class WriteBuffer;
}

namespace DB::Lance
{

/// Cluster-function task payload for one fragment pack.
/// Credentials are never serialized; workers rebuild DatasetHandle from local configuration.
struct LanceObjectSerializableInfo
{
    TableStateSnapshot snapshot;
    std::vector<UInt64> fragment_ids;
    size_t pack_index = 0;
    size_t pack_count = 1;

    void serializeForClusterFunctionProtocol(WriteBuffer & out, size_t protocol_version) const;
    void deserializeForClusterFunctionProtocol(ReadBuffer & in, size_t protocol_version);

private:
    void checkVersion(size_t protocol_version) const;
};

}

namespace DB
{

/// Synthetic ObjectInfo for a Lance dataset (or one fragment pack).
/// Path is not a real object key; it is only a stable task identifier for scheduling.
struct LanceDatasetObjectInfo final : public ObjectInfo
{
    LanceDatasetObjectInfo(
        String synthetic_path_,
        Lance::TableStateSnapshot snapshot_,
        Lance::DatasetHandle dataset_,
        std::vector<UInt64> fragment_ids_,
        size_t pack_index_ = 0,
        size_t pack_count_ = 1);

    /// Reconstruct from a cluster task without a live DatasetHandle.
    explicit LanceDatasetObjectInfo(const RelativePathWithMetadata & path_, const Lance::LanceObjectSerializableInfo & info_);

    Lance::LanceObjectSerializableInfo toSerializableInfo() const;

    const Lance::TableStateSnapshot snapshot;
    /// Empty handle is valid on workers after cluster deserialization; open via session/snapshot.
    const Lance::DatasetHandle dataset;
    /// Empty means full-table scan (force_single_pack / compatibility path).
    const std::vector<UInt64> fragment_ids;
    const size_t pack_index = 0;
    const size_t pack_count = 1;

private:
    static ObjectMetadata createDatasetObjectMetadata();
};

}

#endif
