#pragma once
#include "config.h"

#if USE_AVRO

#include <Interpreters/ActionsDAG.h>
#include <Poco/JSON/Array.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>

namespace DB::Iceberg
{

class ManifestListPruner
{
public:
    /// A condition is built only for the specs of `manifest_list_entries` that can be pruned: `partition_specs`
    /// keeps every spec the table ever had, most of them unused by the snapshot.
    /// `require_ready_sets` makes `IN` conditions whose sets are not built yet unknown instead of building them.
    ManifestListPruner(
        const IcebergSchemaProcessor & schema_processor_,
        Int32 current_schema_id_,
        Int32 partition_schema_id_,
        const Poco::JSON::Array::Ptr & partition_specs,
        const DB::ManifestFileCacheKeys & manifest_list_entries,
        const DB::ActionsDAG * filter_dag,
        DB::ContextPtr context,
        bool require_ready_sets = false);

    [[nodiscard]] bool canBePruned(Int32 partition_spec_id, const PartitionFieldSummaries & partition_summaries) const;

private:
    struct SpecCondition
    {
        DB::KeyDescription partition_key;
        DB::KeyCondition condition;
        std::vector<Int32TruncateWraparound> int32_truncate_wraparounds;
    };

    std::unordered_map<Int32, SpecCondition> conditions_by_spec_id;
};

}

#endif
