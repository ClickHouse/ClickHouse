


std::vector<ParsedDataFileInfo> IcebergMetadata::getDataFiles(
    const ActionsDAG * filter_dag, ContextPtr local_context, const std::vector<ManifestFileEntry> & position_delete_files) const
{
    return getFilesImpl<ParsedDataFileInfo>(
        filter_dag,
        FileContentType::DATA,
        local_context,
        [this, &position_delete_files](const ManifestFileEntry & entry)
        { return ParsedDataFileInfo{this->configuration.lock(), entry, position_delete_files}; });
}

std::vector<Iceberg::ManifestFileEntry>
IcebergMetadata::getPositionDeleteFiles(const ActionsDAG * filter_dag, ContextPtr local_context) const
{
    return getFilesImpl<ManifestFileEntry>(
        filter_dag,
        FileContentType::POSITION_DELETE,
        local_context,
        // In the current design we can't avoid storing ManifestFileEntry in RAM explicitly for position deletes
        [](const ManifestFileEntry & entry) { return entry; });
}



ParsedDataFileInfo::ParsedDataFileInfo(
    StorageObjectStorageConfigurationPtr configuration_,
    Iceberg::ManifestFileEntry data_object_,
    const std::vector<Iceberg::ManifestFileEntry> & position_deletes_objects_)
    : data_object_file_path_key(data_object_.file_path_key)
    , data_object_file_path(data_object_.file_path)
{
    ///Object in position_deletes_objects_ are sorted by common_partition_specification, partition_key_value and added_sequence_number.
    /// It is done to have an invariant that position deletes objects which corresponds
    /// to the data object form a subsegment in a position_deletes_objects_ vector.
    /// We need to take all position deletes objects which has the same partition schema and value and has added_sequence_number
    /// greater than or equal to the data object added_sequence_number (https://iceberg.apache.org/spec/#scan-planning)
    /// ManifestFileEntry has comparator by default which helps to do that.
    auto beg_it = std::lower_bound(position_deletes_objects_.begin(), position_deletes_objects_.end(), data_object_);
    auto end_it = std::upper_bound(
        position_deletes_objects_.begin(),
        position_deletes_objects_.end(),
        data_object_,
        [](const Iceberg::ManifestFileEntry & lhs, const Iceberg::ManifestFileEntry & rhs)
        {
            return std::tie(lhs.common_partition_specification, lhs.partition_key_value)
                < std::tie(rhs.common_partition_specification, rhs.partition_key_value);
        });
    if (beg_it - position_deletes_objects_.begin() > end_it - position_deletes_objects_.begin())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Position deletes objects are not sorted by common_partition_specification and partition_key_value, "
            "beginning: {}, end: {}, position_deletes_objects size: {}",
            beg_it - position_deletes_objects_.begin(),
            end_it - position_deletes_objects_.begin(),
            position_deletes_objects_.size());
    }
    position_deletes_objects = std::span<const Iceberg::ManifestFileEntry>{beg_it, end_it};
    if (!position_deletes_objects.empty() && configuration_->format != "Parquet")
    {
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "Position deletes are only supported for data files of Parquet format in Iceberg, but got {}",
            configuration_->format);
    }
}