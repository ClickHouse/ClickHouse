#pragma once

#include <Interpreters/StorageID.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/StorageSnapshot.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

class Exception;

class ExportPartTask;

struct MergeTreePartExportManifest
{
    using FileAlreadyExistsPolicy = MergeTreePartExportFileAlreadyExistsPolicy;

    using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

    MergeTreePartExportManifest(
        const StorageID & destination_storage_id_,
        const DataPartPtr & data_part_,
        const String & transaction_id_,
        FileAlreadyExistsPolicy file_already_exists_policy_,
        const FormatSettings & format_settings_,
        const StorageMetadataPtr & metadata_snapshot_)
        : destination_storage_id(destination_storage_id_),
          data_part(data_part_),
          transaction_id(transaction_id_),
          file_already_exists_policy(file_already_exists_policy_),
          format_settings(format_settings_),
          metadata_snapshot(metadata_snapshot_),
          create_time(time(nullptr)) {}

    StorageID destination_storage_id;
    DataPartPtr data_part;
    /// Used for killing the export.
    String transaction_id;
    FileAlreadyExistsPolicy file_already_exists_policy;
    FormatSettings format_settings;

    /// Metadata snapshot captured at the time of query validation to prevent race conditions with mutations
    /// Otherwise the export could fail if the schema changes between validation and execution
    StorageMetadataPtr metadata_snapshot;

    time_t create_time;
    mutable bool in_progress = false;
    mutable std::shared_ptr<ExportPartTask> task = nullptr;

    bool operator<(const MergeTreePartExportManifest & rhs) const 
    {
        // Lexicographic comparison: first compare destination storage, then part name
        auto lhs_storage = destination_storage_id.getQualifiedName();
        auto rhs_storage = rhs.destination_storage_id.getQualifiedName();
        
        if (lhs_storage != rhs_storage)
            return lhs_storage < rhs_storage;
            
        return data_part->name < rhs.data_part->name;
    }

    bool operator==(const MergeTreePartExportManifest & rhs) const 
    {
        return destination_storage_id.getQualifiedName() == rhs.destination_storage_id.getQualifiedName()
            && data_part->name == rhs.data_part->name;
    }
};

}
