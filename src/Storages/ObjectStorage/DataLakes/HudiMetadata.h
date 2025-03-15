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
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;

    static constexpr auto name = "Hudi";

    HudiMetadata(ObjectStoragePtr object_storage_, ConfigurationObserverPtr configuration_, ContextPtr context_);

    Strings getDataFiles() const override;

    NamesAndTypesList getTableSchema() const override { return {}; }

    bool operator ==(const IDataLakeMetadata & other) const override
    {
        const auto * hudi_metadata = dynamic_cast<const HudiMetadata *>(&other);
        return hudi_metadata
            && !data_files.empty() && !hudi_metadata->data_files.empty()
            && data_files == hudi_metadata->data_files;
    }

    size_t getMemoryBytes() const override
    {
        size_t size = sizeof(*this);
        for (const String & data_file : data_files)
            size += data_file.size();
        return size;
    }

    static DataLakeMetadataPtr create(
        ObjectStoragePtr object_storage_,
        ConfigurationObserverPtr configuration_,
        ContextPtr local_context);

private:
    const ObjectStoragePtr object_storage;
    ConfigurationObserverPtr configuration;
    mutable Strings data_files;

    Strings getDataFilesImpl() const;
};

}
