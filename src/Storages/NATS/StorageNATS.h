#pragma once

#include <atomic>
#include <mutex>
#include <uv.h>
#include <Core/BackgroundSchedulePool.h>
#include <Storages/IStorage.h>
#include <Storages/NATS/Buffer_fwd.h>
#include <Storages/NATS/NATSConnection.h>
#include <Storages/NATS/NATSSettings.h>
#include <Poco/Semaphore.h>
#include <Common/thread_local_rng.h>

namespace DB
{

class StorageNATS final : public IStorage, WithContext
{
public:
    StorageNATS(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        std::unique_ptr<NATSSettings> nats_settings_,
        bool is_attach_);

    std::string getName() const override { return "NATS"; }

    bool noPushingToViews() const override { return true; }

    void startup() override;
    void shutdown() override;

    /// This is a bad way to let storage know in shutdown() that table is going to be dropped. There are some actions which need
    /// to be done only when table is dropped (not when detached). Also connection must be closed only in shutdown, but those
    /// actions require an open connection. Therefore there needs to be a way inside shutdown() method to know whether it is called
    /// because of drop query. And drop() method is not suitable at all, because it will not only require to reopen connection, but also
    /// it can be called considerable time after table is dropped (for example, in case of Atomic database), which is not appropriate for the case.
    void checkTableCanBeDropped() const override { drop_table = true; }

    /// Always return virtual columns in addition to required columns
    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum /* processed_stage */,
        size_t /* max_block_size */,
        unsigned /* num_streams */) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

    void pushReadBuffer(ConsumerBufferPtr buf);
    ConsumerBufferPtr popReadBuffer();
    ConsumerBufferPtr popReadBuffer(std::chrono::milliseconds timeout);

    const String & getFormatName() const { return format_name; }
    NamesAndTypesList getVirtuals() const override;

    void incrementReader();
    void decrementReader();

    void startStreaming() { if (!mv_attached) { streaming_task->activateAndSchedule(); } }

private:
    ContextMutablePtr nats_context;
    std::unique_ptr<NATSSettings> nats_settings;
    std::vector<String> subjects;

    const String format_name;
    char row_delimiter;
    const String schema_name;
    size_t num_consumers;

    Poco::Logger * log;

    NATSConnectionManagerPtr connection; /// Connection for all consumers
    NATSConfiguration configuration;

    size_t num_created_consumers = 0;
    Poco::Semaphore semaphore;
    std::mutex buffers_mutex;
    std::vector<ConsumerBufferPtr> buffers; /// available buffers for NATS consumers

    /// maximum number of messages in NATS queue (x-max-length). Also used
    /// to setup size of inner buffer for received messages
    uint32_t queue_size;

    std::once_flag flag; /// remove exchange only once
    std::mutex task_mutex;
    BackgroundSchedulePool::TaskHolder streaming_task;
    BackgroundSchedulePool::TaskHolder looping_task;
    BackgroundSchedulePool::TaskHolder connection_task;

    /// True if consumers have subscribed to all subjects
    std::atomic<bool> consumers_ready{false};
    /// Needed for tell MV or producer background tasks
    /// that they must finish as soon as possible.
    std::atomic<bool> shutdown_called{false};
    /// For select query we must be aware of the end of streaming
    /// to be able to turn off the loop.
    std::atomic<size_t> readers_count = 0;
    std::atomic<bool> mv_attached = false;

    /// In select query we start event loop, but do not stop it
    /// after that select is finished. Then in a thread, which
    /// checks for MV we also check if we have select readers.
    /// If not - we turn off the loop. The checks are done under
    /// mutex to avoid having a turned off loop when select was
    /// started.
    std::mutex loop_mutex;

    mutable bool drop_table = false;
    bool is_attach;

    ConsumerBufferPtr createReadBuffer();
    ProducerBufferPtr createWriteBuffer(const std::string & subject);

    bool isSubjectInSubscriptions(const std::string & subject);


    /// Functions working in the background
    void streamingToViewsFunc();
    void loopingFunc();
    void connectionFunc();

    bool initBuffers();

    void startLoop();
    void stopLoop();
    void stopLoopIfNoReaders();

    static Names parseList(const String & list, char delim);
    static String getTableBasedName(String name, const StorageID & table_id);

    ContextMutablePtr addSettings(ContextPtr context) const;
    size_t getMaxBlockSize() const;
    void deactivateTask(BackgroundSchedulePool::TaskHolder & task, bool stop_loop);

    bool streamToViews();
    bool checkDependencies(const StorageID & table_id);
};

}
