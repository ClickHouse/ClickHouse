#pragma once

#include <Interpreters/Context_fwd.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Core/Types.h>

namespace DataLake { class ICatalog; }

namespace DB
{

class HudiMetadata final : public IDataLakeMetadata, private WithContext
{
public:
    static constexpr auto name = "Hudi";

    const char * getName() const override { return name; }

    HudiMetadata(ObjectStoragePtr object_storage_, ObjectStorageConnectionConfigurationPtr configuration_, ContextPtr context_);

    NamesAndTypesList getTableSchema(ContextPtr /*local_context*/) const override { return {}; }

    bool operator ==(const IDataLakeMetadata & other) const override
    {
        const auto * hudi_metadata = dynamic_cast<const HudiMetadata *>(&other);
        return hudi_metadata
            && !data_files.empty() && !hudi_metadata->data_files.empty()
            && data_files == hudi_metadata->data_files;
    }

    static DataLakeMetadataPtr
    create(ObjectStoragePtr object_storage, ObjectStorageConnectionConfigurationWeakPtr configuration, ContextPtr local_context)
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
    mutable Strings data_files;

    Strings getDataFilesImpl() const;
    Strings getDataFiles(const ActionsDAG * filter_dag) const;
};

}
