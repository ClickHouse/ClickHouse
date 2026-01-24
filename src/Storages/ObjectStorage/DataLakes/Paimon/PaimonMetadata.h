#pragma once
#include <config.h>

#if USE_AVRO

#include <optional>
#include <vector>
#include <Core/Block.h>
#include <Disks/IStoragePolicy.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/BinaryRow.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonClient.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonTableSchema.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Common/SharedMutex.h>
#include <Common/SharedLockGuard.h>


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
        bool /*if_not_exists*/,
        std::shared_ptr<DataLake::ICatalog> /*catalog*/,
        const StorageID & /*table_id_*/)
    {
    }

    NamesAndTypesList getTableSchema(ContextPtr /*local_context*/) const override;

    bool operator==(const IDataLakeMetadata & other) const override
    {
        const auto * paimon_metadata = dynamic_cast<const PaimonMetadata *>(&other);
        SharedLockGuard lock_shared(mutex);
        SharedLockGuard lock_shared_other(paimon_metadata->mutex);
        return paimon_metadata && table_schema == paimon_metadata->table_schema && snapshot == paimon_metadata->snapshot;
    }

    bool supportsUpdate() const override { return true; }

    void update(const ContextPtr & local_context) override;

    ObjectIterator
    iterate(const ActionsDAG * /* filter_dag */,
        FileProgressCallback callback,
        size_t /* list_batch_size */,
        StorageMetadataPtr /* storage_metadata */,
        ContextPtr /* context */)
        const override;
    const char * getName() const override { return name; }
private:
    void updateState();
    void checkSupportCofiguration();

    mutable SharedMutex mutex;
    std::optional<PaimonSnapshot> snapshot TSA_GUARDED_BY(mutex);
    std::optional<PaimonTableSchema> table_schema TSA_GUARDED_BY(mutex);
    std::vector<PaimonManifest> base_manifest TSA_GUARDED_BY(mutex);
    std::vector<PaimonManifest> delta_manifest TSA_GUARDED_BY(mutex);
    const ObjectStoragePtr object_storage;
    const StorageObjectStorageConfigurationWeakPtr configuration;
    LoggerPtr log;
    PaimonTableClientPtr table_client_ptr;
    Poco::JSON::Object::Ptr last_metadata_object TSA_GUARDED_BY(mutex);


    constexpr static String PARTITION_DEFAULT_VALUE = "__DEFAULT_PARTITION__";
};

}
#endif
