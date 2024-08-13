#pragma once

#include <Interpreters/Context_fwd.h>
#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Disks/ObjectStorages/IObjectStorage.h>

namespace DB
{

class DeltaLakeMetadata final : public IDataLakeMetadata
{
public:
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;
    static constexpr auto name = "DeltaLake";

    DeltaLakeMetadata(
        ObjectStoragePtr object_storage_,
        ConfigurationPtr configuration_,
        ContextPtr context_);

    Strings getDataFiles() const override;

    NamesAndTypesList getTableSchema() const override { return {}; }

    bool operator ==(const IDataLakeMetadata & other) const override
    {
        const auto * deltalake_metadata = dynamic_cast<const DeltaLakeMetadata *>(&other);
        return deltalake_metadata
            && !data_files.empty() && !deltalake_metadata->data_files.empty()
            && data_files == deltalake_metadata->data_files;
    }

    static DataLakeMetadataPtr create(
        ObjectStoragePtr object_storage,
        ConfigurationPtr configuration,
        ContextPtr local_context)
    {
        return std::make_unique<DeltaLakeMetadata>(object_storage, configuration, local_context);
    }

private:
    struct Impl;
    const std::shared_ptr<Impl> impl;
    mutable Strings data_files;
};

}
