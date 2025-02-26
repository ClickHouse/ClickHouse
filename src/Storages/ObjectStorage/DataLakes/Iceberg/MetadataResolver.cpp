



void IcebergMetadata::updateState(const ContextPtr & local_context)
{
    bool timestamp_changed = local_context->getSettingsRef()[Settings::iceberg_query_at_timestamp_ms].changed;
    if (!timestamp_changed)
    {
        Int64 query_timestamp = local_context->getSettingsRef()[Settings::iceberg_query_at_timestamp_ms];
        // Use current snapshot
        snapshot_id = metadata->getValue<Int64>("current-snapshot-id");
        for (size_t i = 0; i < snapshots->size(); ++i)
        {
            const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
            if (snapshot->getValue<Int64>("snapshot-id") == snapshot_id)
            {
                const auto path = snapshot->getValue<String>("manifest-list");
                manifest_list_file
                    = std::filesystem::path(configuration_ptr->getPath()) / "metadata" / std::filesystem::path(path).filename();
                schema_id = snapshot->getValue<Int32>("schema-id");
                snapshot_id = snapshot->getValue<Int64>("snapshot-id");

                break;
            }
        }
    }
    else
    {
        // Find the most recent snapshot at or before the query timestamp
        Int64 closest_timestamp = 0;
        for (size_t i = 0; i < snapshots->size(); ++i)
        {
            const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
            Int64 snapshot_timestamp = snapshot->getValue<Int64>("timestamp-ms");

            // The spec doesn't say these have to be ordered, so we do an exhaustive search just to be safe
            if (snapshot_timestamp <= query_timestamp && snapshot_timestamp > closest_timestamp)
            {
                closest_timestamp = snapshot_timestamp;
                const auto path = snapshot->getValue<String>("manifest-list");
                manifest_list_file
                    = std::filesystem::path(configuration_ptr->getPath()) / "metadata" / std::filesystem::path(path).filename();
                schema_id = snapshot->getValue<Int32>("schema-id");
                snapshot_id = snapshot->getValue<Int64>("snapshot-id");
            }
        }

        if (manifest_list_file.empty() || schema_id == -1 || snapshot_id == -1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No Iceberg snapshot found at or before timestamp {}", query_timestamp);
        }
    }
    auto manifest_list_file = getRelevantManifestList(metadata_object);
    if (manifest_list_file && (!current_snapshot.has_value() || (manifest_list_file.value() != current_snapshot->getName())))
    {
        current_snapshot = getSnapshot(manifest_list_file.value());
        cached_files_for_current_snapshot = std::nullopt;
    }
    current_schema_id = parseTableSchema(metadata_object, schema_processor, log);
}


    std::optional<Int32> getSchemaVersionByFileIfOutdated(const ContextPtr & local_context) const 