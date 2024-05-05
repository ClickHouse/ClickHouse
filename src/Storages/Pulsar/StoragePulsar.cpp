#include <Storages/Pulsar/PulsarSettings.h>
#include <Storages/Pulsar/StoragePulsar.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromStreamLikeEngine.h>
#include <Storages/Pulsar/PulsarSource.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int QUERY_NOT_ALLOWED;
extern const int ABORTED;
}

class ReadFromStoragePulsar final : public ReadFromStreamLikeEngine
{
public:
    ReadFromStoragePulsar(
        const Names & column_names_,
        StoragePtr storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        SelectQueryInfo & query_info,
        ContextPtr context_)
        : ReadFromStreamLikeEngine{column_names_, storage_snapshot_, query_info.storage_limits, context_}
        , column_names{column_names_}
        , storage{storage_}
        , storage_snapshot{storage_snapshot_}
    {
    }

    String getName() const override { return "ReadFromStoragePulsar"; }

private:
    Pipe makePipe() final
    {
        // auto & pulsar_storage = storage->as<StoragePulsar &>();
        // if (pulsar_storage.shutdown_called.load())
        //     throw Exception(ErrorCodes::ABORTED, "Table is detached");

        // if (pulsar_storage.mv_attached)
        //     throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Cannot read from StorageKafka with attached materialized views");

        // /// Always use all consumers at once, otherwise SELECT may not read messages from all partitions.
        // Pipes pipes;
        // pipes.reserve(pulsar_storage.num_consumers);
        // auto modified_context = Context::createCopy(getContext());
        // modified_context->applySettingsChanges(pulsar_storage.settings_adjustments);

        // // Claim as many consumers as requested, but don't block
        // for (size_t i = 0; i < pulsar_storage.num_consumers; ++i)
        // {
        //     /// Use block size of 1, otherwise LIMIT won't work properly as it will buffer excess messages in the last block
        //     /// TODO: probably that leads to awful performance.
        //     /// FIXME: seems that doesn't help with extra reading and committing unprocessed messages.
        //     pipes.emplace_back(std::make_shared<PulsarSource>(
        //         pulsar_storage,
        //         storage_snapshot,
        //         modified_context,
        //         column_names,
        //         kafka_storage.log,
        //         1,
        //         kafka_storage.kafka_settings->kafka_commit_on_select));
        // }

        // // LOG_DEBUG(kafka_storage.log, "Starting reading {} streams", pipes.size());
        // return Pipe::unitePipes(std::move(pipes));
    }

    const Names column_names;
    StoragePtr storage;
    StorageSnapshotPtr storage_snapshot;
};

StoragePulsar::StoragePulsar(const StorageID & table_id_, ContextPtr context_, std::unique_ptr<PulsarSettings> pulsar_settings_)
    : IStorage(table_id_), WithContext(context_), num_consumers(pulsar_settings_->pulsar_num_consumers.value)
{
}

void StoragePulsar::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr query_context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    size_t /* num_streams */)
{
    query_plan.addStep(
        std::make_unique<ReadFromStoragePulsar>(column_names, shared_from_this(), storage_snapshot, query_info, std::move(query_context)));
}


}