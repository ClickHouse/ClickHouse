#pragma once

#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Core/Block_fwd.h>
#include <Core/StreamingHandleErrorMode.h>
#include <Core/Types.h>
#include <Storages/IStorage.h>
#include <Storages/Kafka/IKafkaExceptionInfoSink.h>
#include <Storages/Kafka/KafkaConsumer2.h>
#include <Storages/Kafka/Kafka_fwd.h>
#include <Storages/Kafka/KeeperHandlingConsumer.h>
#include <Common/Macros.h>
#include <Common/SettingsChanges.h>
#include <Common/ThreadStatus.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <atomic>
#include <filesystem>
#include <list>
#include <mutex>
#include <rdkafka.h>

namespace cppkafka
{

class Configuration;

}

namespace DB
{

struct KafkaSettings;
template <typename TStorageKafka>
struct KafkaInterceptors;

/// Implements a Kafka queue table engine that can be used as a persistent queue / buffer,
/// or as a basic building block for creating pipelines with a continuous insertion / ETL.
///
/// It is similar to the already existing StorageKafka, it instead of storing the offsets
/// in Kafka, its main source of information about offsets is Keeper. On top of the
/// offsets, it also stores the number of messages (intent size) it tried to insert from
/// each topic. By storing the intent sizes it is possible to retry the same batch of
/// messages in case of any errors and giving deduplication a chance to deduplicate
/// blocks.
///
/// To not complicate things too much, the current implementation makes sure to fetch
/// messages only from a single topic-partition on a single thread at a time by
/// manipulating the queues of librdkafka. By pulling from multiple topic-partitions
/// the order of messages are not guaranteed, therefore they would have different
/// hashes for deduplication.
///
/// For the committed offsets we try to mimic the same behavior as Kafka does: if the last
/// read offset is `n`, then we save the offset `n + 1`, same as Kafka does.
class StorageKafka2 final : public IStorage, WithContext
{
    using KafkaInterceptors = KafkaInterceptors<StorageKafka2>;
    friend KafkaInterceptors;

public:
    using KeeperHandlingConsumerPtr = std::shared_ptr<KeeperHandlingConsumer>;
    struct SafeConsumers
    {
        std::shared_ptr<IStorage> storage_ptr;
        std::unique_lock<std::mutex> lock;
        std::vector<KeeperHandlingConsumerPtr> & consumers;
    };

    StorageKafka2(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        const String & comment,
        std::unique_ptr<KafkaSettings> kafka_settings_,
        const String & collection_name_);

    ~StorageKafka2() override;

    std::string getName() const override { return Kafka::TABLE_ENGINE_NAME; }

    bool isMessageQueue() const override { return true; }

    bool noPushingToViewsOnInserts() const override { return true; }

    void startup() override;
    void shutdown(bool is_drop) override;

    void drop() override;

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr
    write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context, bool async_insert) override;

    /// We want to control the number of rows in a chunk inserted into Kafka
    bool prefersLargeBlocks() const override { return false; }

    const auto & getFormatName() const { return format_name; }

    StreamingHandleErrorMode getHandleKafkaErrorMode() const;

    bool supportsDynamicSubcolumns() const override { return true; }
    bool supportsSubcolumns() const override { return true; }

    const KafkaSettings & getKafkaSettings() const { return *kafka_settings; }

    SafeConsumers getSafeConsumers() { return {shared_from_this(), std::unique_lock(consumers_mutex), consumers}; }

private:
    // Stream thread
    struct TaskContext
    {
        BackgroundSchedulePoolTaskHolder holder;
        std::atomic<bool> stream_cancelled{false};
        explicit TaskContext(BackgroundSchedulePoolTaskHolder && task_)
            : holder(std::move(task_))
        {
        }
    };

    struct BlocksAndGuard
    {
        BlocksList blocks;
        KeeperHandlingConsumer::OffsetGuard guard;
    };

    // Configuration and state
    mutable std::mutex keeper_mutex;
    zkutil::ZooKeeperPtr keeper;
    const String keeper_path;
    const std::filesystem::path fs_keeper_path;
    String replica_path;
    std::unique_ptr<KafkaSettings> kafka_settings;
    Macros::MacroExpansionInfo macros_info;
    const Names topics;
    const String brokers;
    const String group;
    const String client_id;
    const String format_name;
    const size_t max_rows_per_message;
    const String schema_name;
    const size_t num_consumers; /// total number of consumers
    LoggerPtr log;
    const SettingsChanges settings_adjustments;
    /// Can differ from num_consumers in case of exception in startup() (or if startup() hasn't been called).
    /// In this case we still need to be able to shutdown() properly.
    size_t num_created_consumers = 0; /// number of actually created consumers.

    std::mutex consumers_mutex;
    std::condition_variable cv;
    std::vector<KeeperHandlingConsumerPtr> consumers TSA_GUARDED_BY(consumers_mutex);

    std::vector<std::shared_ptr<TaskContext>> tasks;
    bool thread_per_consumer = false;
    /// For memory accounting in the librdkafka threads.
    std::mutex thread_statuses_mutex;
    std::list<std::shared_ptr<ThreadStatus>> thread_statuses;
    /// If named_collection is specified.
    String collection_name;
    std::atomic<bool> shutdown_called = false;

    // Handling replica activation.
    std::atomic<bool> is_active = false;
    zkutil::EphemeralNodeHolderPtr replica_is_active_node;
    BackgroundSchedulePoolTaskHolder activating_task;
    String active_node_identifier;
    UInt64 consecutive_activate_failures = 0;

    bool activate();
    void activateAndReschedule();
    void partialShutdown();

    void assertActive() const;
    KafkaConsumer2Ptr createKafkaConsumer(size_t consumer_number);
    // Returns full consumer related configuration, also the configuration
    // contains global kafka properties.
    cppkafka::Configuration getConsumerConfiguration(size_t consumer_number, IKafkaExceptionInfoSinkPtr exception_sink);
    // Returns full producer related configuration, also the configuration
    // contains global kafka properties.
    cppkafka::Configuration getProducerConfiguration();

    void threadFunc(size_t idx);

    size_t getPollMaxBatchSize() const;
    size_t getMaxBlockSize() const;
    size_t getPollTimeoutMillisecond() const;
    size_t getSchemaRegistrySkipBytes() const;

    enum class StallKind : uint8_t
    {
        ShortStall,
        LongStall,
    };

    std::optional<StallKind> streamToViews(size_t idx);

    /// KeeperHandlingConsumer has to be acquired before polling it
    KeeperHandlingConsumerPtr acquireConsumer(size_t idx);
    void releaseConsumer(KeeperHandlingConsumerPtr && consumer_ptr);
    void cleanConsumers();

    std::optional<size_t> streamFromConsumer(KeeperHandlingConsumer & consumer_info, const Stopwatch & watch);

    // Returns true if this is the first replica
    bool createTableIfNotExists();
    // Returns true if all of the nodes were cleaned up
    bool removeTableNodesFromZooKeeper(zkutil::ZooKeeperPtr keeper_to_use, const zkutil::EphemeralNodeHolder::Ptr & drop_lock);
    // Creates only the replica in ZooKeeper. Shouldn't be called on the first replica as it is created in createTableIfNotExists
    void createReplica();
    void dropReplica();

    std::optional<BlocksAndGuard>
    pollConsumer(KeeperHandlingConsumer & consumer, const Stopwatch & watch, const ContextPtr & modified_context);

    void setZooKeeper();
    zkutil::ZooKeeperPtr tryGetZooKeeper() const;
    zkutil::ZooKeeperPtr getZooKeeper() const;
    zkutil::ZooKeeperPtr getZooKeeperAndAssertActive() const;
    zkutil::ZooKeeperPtr getZooKeeperIfTableShutDown() const;

    static StallKind getStallKind(const KeeperHandlingConsumer::CannotPollReason & cannotPollReason);
};

}
