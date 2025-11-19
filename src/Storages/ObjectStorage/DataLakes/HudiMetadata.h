#pragma once

#include <Interpreters/Context_fwd.h>
#include <Disks/ObjectStorages/IObjectStorage_fwd.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Core/Types.h>

namespace DB
{

class HudiMetadata final : public IDataLakeMetadata, private WithContext
{
public:
    static constexpr auto name = "Hudi";

    const char * getName() const override { return name; }

    HudiMetadata(ObjectStoragePtr object_storage_, StorageObjectStorageConfigurationWeakPtr configuration_, ContextPtr context_);

    NamesAndTypesList getTableSchema(ContextPtr /*local_context*/) const override { return {}; }

    bool operator ==(const IDataLakeMetadata & other) const override
    {
        const auto * hudi_metadata = dynamic_cast<const HudiMetadata *>(&other);
        return hudi_metadata
            && !data_files.empty() && !hudi_metadata->data_files.empty()
            && data_files == hudi_metadata->data_files;
    }

    static void createInitial(
        const ObjectStoragePtr & /*object_storage*/,
        const StorageObjectStorageConfigurationWeakPtr & /*configuration*/,
        const ContextPtr & /*local_context*/,
        const std::optional<ColumnsDescription> & /*columns*/,
        ASTPtr /*partition_by*/,
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
        return std::make_unique<HudiMetadata>(object_storage, configuration, local_context);
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
    const StorageObjectStorageConfigurationWeakPtr configuration;
    mutable Strings data_files;

    Strings getDataFilesImpl() const;
    Strings getDataFiles(const ActionsDAG * filter_dag) const;
};

}
