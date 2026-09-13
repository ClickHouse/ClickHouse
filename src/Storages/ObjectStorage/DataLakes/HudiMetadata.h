#pragma once

#include <Interpreters/Context_fwd.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Core/Types.h>

#include <mutex>

namespace DB
{

class HudiMetadata final : public IDataLakeMetadata, private WithContext
{
public:
    static constexpr auto name = "Hudi";

    const char * getName() const override { return name; }

    HudiMetadata(ObjectStoragePtr object_storage_, StorageObjectStorageConfigurationPtr configuration_, ContextPtr context_);

    NamesAndTypesList getTableSchema(ContextPtr /*local_context*/) const override { return {}; }

    bool operator ==(const IDataLakeMetadata & other) const override
    {
        const auto * hudi_metadata = dynamic_cast<const HudiMetadata *>(&other);
        if (!hudi_metadata)
            return false;

        /// Both file lists are read under their own object's mutex, and never both at once, so a
        /// comparison of an object with itself does not deadlock either.
        auto this_data_files = getDataFilesIfListed();
        auto other_data_files = hudi_metadata->getDataFilesIfListed();

        return !this_data_files.empty() && !other_data_files.empty() && this_data_files == other_data_files;
    }

    static void createInitial(
        const ObjectStoragePtr & /*object_storage*/,
        const StorageObjectStorageConfigurationWeakPtr & /*configuration*/,
        const ContextPtr & /*local_context*/,
        const std::optional<ColumnsDescription> & /*columns*/,
        ASTPtr /*partition_by*/,
        ASTPtr /*order_by*/,
        bool /*if_not_exists*/,
        std::shared_ptr<DataLake::ICatalog> /*catalog*/,
        const StorageID & /*table_id_*/)
    {
    }

    static DataLakeMetadataPtr create(
        ObjectStoragePtr object_storage,
        StorageObjectStorageConfigurationWeakPtr configuration,
        ContextPtr local_context)
    {
        return std::make_unique<HudiMetadata>(object_storage, configuration.lock(), local_context);
    }

protected:
    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        FileProgressCallback callback,
        size_t list_batch_size,
        StorageMetadataPtr storage_metadata_snapshot,
        ContextPtr context) const override;

private:
    const ObjectStoragePtr object_storage;
    const String table_path;
    const String format;

    /** One metadata object is shared by the queries that run on the table (see
      * `DataLakeConfiguration::update`), and this list is filled on first use, so every access to it
      * is synchronized. Listing the data files of a table takes a listing of the object storage, and
      * the mutex is held across it, so that a second query waits for the first list instead of
      * making its own.
      */
    mutable std::mutex data_files_mutex;
    mutable Strings data_files TSA_GUARDED_BY(data_files_mutex);

    Strings getDataFilesImpl() const;
    Strings getDataFiles(const ActionsDAG * filter_dag) const;
    /// The listed data files, or an empty list when nothing has listed them yet.
    Strings getDataFilesIfListed() const;
};

}
