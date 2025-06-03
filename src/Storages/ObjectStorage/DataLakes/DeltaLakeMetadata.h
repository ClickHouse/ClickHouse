#pragma once

#include "config.h"

#if USE_PARQUET

#include <Interpreters/Context_fwd.h>
#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Poco/JSON/Object.h>
#include <Core/Settings.h>

namespace DB
{

struct DeltaLakePartitionColumn
{
    NameAndTypePair name_and_type;
    Field value;

    bool operator ==(const DeltaLakePartitionColumn & other) const = default;
};

/// Data file -> partition columns
using DeltaLakePartitionColumns = std::unordered_map<std::string, std::vector<DeltaLakePartitionColumn>>;


class DeltaLakeMetadata final : public IDataLakeMetadata
{
public:
    using ConfigurationObserverPtr = StorageObjectStorage::ConfigurationObserverPtr;
    static constexpr auto name = "DeltaLake";

    DeltaLakeMetadata(ObjectStoragePtr object_storage_, ConfigurationObserverPtr configuration_, ContextPtr context_);

    NamesAndTypesList getTableSchema() const override { return schema; }

    DeltaLakePartitionColumns getPartitionColumns() const { return partition_columns; }

    bool operator==(const IDataLakeMetadata & other) const override
    {
        const auto * deltalake_metadata = dynamic_cast<const DeltaLakeMetadata *>(&other);
        return deltalake_metadata
            && !data_files.empty() && !deltalake_metadata->data_files.empty()
            && data_files == deltalake_metadata->data_files;
    }

    static DataLakeMetadataPtr create(
        ObjectStoragePtr object_storage,
        ConfigurationObserverPtr configuration,
        ContextPtr local_context);

    static DataTypePtr getFieldType(const Poco::JSON::Object::Ptr & field, const String & type_key, bool is_nullable);
    static DataTypePtr getSimpleTypeByName(const String & type_name);
    static DataTypePtr getFieldValue(const Poco::JSON::Object::Ptr & field, const String & type_key, bool is_nullable);
    static Field getFieldValue(const String & value, DataTypePtr data_type);

protected:
    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        FileProgressCallback callback,
        size_t list_batch_size) const override;

private:
    mutable Strings data_files;
    NamesAndTypesList schema;
    DeltaLakePartitionColumns partition_columns;
    ObjectStoragePtr object_storage;

    Strings getDataFiles(const ActionsDAG *) const { return data_files; }
};

}

#endif
