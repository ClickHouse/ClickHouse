#pragma once

#include "config.h"

#if USE_AVRO

#include <map>
#include <optional>

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>

namespace DB::Iceberg
{

/// The data manifests of a manifest list that hold a single partition of the spec `partition_spec_id`, according to their
/// summaries in the manifest list, grouped by that partition.
class ManifestsByPartition
{
public:
    ManifestsByPartition(const ManifestFileCacheKeys & parent_manifests, Int32 partition_spec_id, const DataTypes & partition_types);

    /// The manifests that hold only the partition `partition_value`, newest first.
    const ManifestFileCacheKeys & get(const Row & partition_value) const;

private:
    /// The serialized value of each partition field, or unset for null.
    using SinglePartition = std::vector<std::optional<String>>;

    DataTypes partition_types;
    std::map<SinglePartition, ManifestFileCacheKeys> manifests;
};

}

#endif
