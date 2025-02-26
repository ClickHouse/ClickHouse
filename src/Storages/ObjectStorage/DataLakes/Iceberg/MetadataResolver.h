#pragma once
#include "config.h"

#if USE_AVRO

#    include <Core/Types.h>
#    include <Disks/ObjectStorages/IObjectStorage.h>
#    include <Interpreters/Context_fwd.h>
#    include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#    include <Storages/ObjectStorage/StorageObjectStorage.h>

#    include <Poco/JSON/Array.h>
#    include <Poco/JSON/Object.h>
#    include <Poco/JSON/Parser.h>

#    include "Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h"
#    include "Storages/ObjectStorage/DataLakes/Iceberg/PartitionPruning.h"
#    include "Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h"
#    include "Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h"

#    include <unordered_map>

namespace DB
{

struct IcebergTableState
{
    Int32 current_metadata_version;
    Int32 format_version;
    Int32 current_schema_id;
    std::optional<Iceberg::IcebergSnapshot> current_snapshot;
};


class MetadataResolver
{
public:
    std::optional<IcebergTableState> getSchemaVersionByFileIfOutdated(const ContextPtr & local_context) const;


    Int64 getRelevantSnapshotId(const Poco::JSON::Object::Ptr & metadata)
    {
        auto configuration_ptr = configuration.lock();

        Int64 snapshot_id = -1;
        Int32 schema_id = -1;

        std::optional<String> manifest_list_file;

        bool timestamp_changed = local_context->getSettingsRef()[Settings::iceberg_query_at_timestamp_ms].changed;
        bool snapshot_id_changed = local_context->getSettingsRef()[Settings::iceberg_snapshot_id].changed;
        if (timestamp_changed && snapshot_id_changed)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg snapshot id and timestamp cannot be changed simultaneously");
        }
        if (timestamp_changed)
        {
            Int64 closest_timestamp = 0;
            Int64 query_timestamp = local_context->getSettingsRef()[Settings::iceberg_timestamp_ms];
            for (size_t i = 0; i < snapshots->size(); ++i)
            {
                const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
                Int64 snapshot_timestamp = snapshot->getValue<Int64>("timestamp-ms");
                if (snapshot_timestamp <= query_timestamp && snapshot_timestamp > closest_timestamp)
                {
                    closest_timestamp = snapshot_timestamp;
                    const auto path = snapshot->getValue<String>("manifest-list");
                    manifest_list_file = std::filesystem::path(path) / "metadata" / std::filesystem::path(path).filename();
                    snapshot_id = snapshot->getValue<Int64>("snapshot-id");
                }
            }
        }
        else if (snapshot_id_changed)
        {
            snapshot_id = local_context->getSettingsRef()[Settings::iceberg_snapshot_id];
            return std::make_tuple(schema_id, snapshot_id);
        }
    }

private:
    const ObjectStoragePtr object_storage;
    const std::string path;
};


}

#endif
