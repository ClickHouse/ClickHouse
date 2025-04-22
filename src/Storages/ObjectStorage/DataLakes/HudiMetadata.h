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
    using ConfigurationObserverPtr = StorageObjectStorage::ConfigurationObserverPtr;

    static constexpr auto name = "Hudi";

    HudiMetadata(ObjectStoragePtr object_storage_, ConfigurationObserverPtr configuration_, ContextPtr context_);

    NamesAndTypesList getTableSchema() const override { return {}; }

    bool operator ==(const IDataLakeMetadata & other) const override
    {
        const auto * hudi_metadata = dynamic_cast<const HudiMetadata *>(&other);
        return hudi_metadata
            && !data_files.empty() && !hudi_metadata->data_files.empty()
            && data_files == hudi_metadata->data_files;
    }

    static DataLakeMetadataPtr create(
        ObjectStoragePtr object_storage,
        ConfigurationObserverPtr configuration,
        ContextPtr local_context)
    {
        return std::make_unique<HudiMetadata>(object_storage, configuration, local_context);
    }

protected:
    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        FileProgressCallback callback,
        size_t list_batch_size) const override;

private:
    const ObjectStoragePtr object_storage;
    const ConfigurationObserverPtr configuration;
    mutable Strings data_files;

    Strings getDataFilesImpl() const;
    Strings getDataFiles(const ActionsDAG * filter_dag) const;
};

}
