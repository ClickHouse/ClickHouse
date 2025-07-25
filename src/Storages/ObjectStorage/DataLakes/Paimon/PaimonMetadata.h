#pragma once
#include "config.h"

#if USE_AVRO

#include <optional>
#include <vector>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Core/Block.h>
#include <Disks/IStoragePolicy.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/BinaryRow.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonClient.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonTableSchema.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>


namespace DB
{
class PaimonMetadata : public IDataLakeMetadata, private WithContext
{
public:
    static constexpr auto name = "Paimon";

    PaimonMetadata(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationWeakPtr configuration_,
        const DB::ContextPtr & context_,
        const Poco::JSON::Object::Ptr & schema_json_object_,
        PaimonTableClientPtr table_client_ptr_);

    static DataLakeMetadataPtr create(
        const ObjectStoragePtr & object_storage,
        const StorageObjectStorageConfigurationWeakPtr & configuration,
        const ContextPtr & local_context);

    static void createInitial(
        const ObjectStoragePtr & /*object_storage*/,
        const StorageObjectStorageConfigurationWeakPtr & /*configuration*/,
        const ContextPtr & /*local_context*/,
        const std::optional<ColumnsDescription> & /*columns*/,
        ASTPtr /*partition_by*/,
        bool /*if_not_exists*/)
    {
    }

    NamesAndTypesList getTableSchema() const override;

    bool operator==(const IDataLakeMetadata & other) const override
    {
        const auto * paimon_metadata = dynamic_cast<const PaimonMetadata *>(&other);
        return paimon_metadata && table_schema == paimon_metadata->table_schema && snapshot == paimon_metadata->snapshot;
    }

    // std::shared_ptr<const ActionsDAG> getSchemaTransformer(ContextPtr, const String & data_path) const override;

    bool supportsUpdate() const override { return true; }

    bool update(const ContextPtr & local_context) override;

    ObjectIterator
    iterate(const ActionsDAG * /* filter_dag */, FileProgressCallback /* callback */, size_t /* list_batch_size */, ContextPtr context)
        const override;

private:
    bool updateState();
    void checkSupportCofiguration();
    // String getBucketPath(String partition, Int32 bucket);
    // String getPartitionString(Paimon::BinaryRow & partition);
    std::optional<PaimonSnapshot> snapshot;
    std::optional<PaimonTableSchema> table_schema;
    std::vector<PaimonManifest> base_manifest;
    std::vector<PaimonManifest> delta_manifest;
    const ObjectStoragePtr object_storage;
    const StorageObjectStorageConfigurationWeakPtr configuration;
    LoggerPtr log;
    PaimonTableClientPtr table_client_ptr;
    Poco::JSON::Object::Ptr last_metadata_object;


    constexpr static String PARTITION_DEFAULT_NAME = "__DEFAULT_PARTITION__";
};

}
#endif
