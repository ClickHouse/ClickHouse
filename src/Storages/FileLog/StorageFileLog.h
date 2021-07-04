#pragma once

#include <Storages/FileLog/Buffer_fwd.h>

#include <Core/BackgroundSchedulePool.h>
#include <Storages/IStorage.h>
#include <Common/SettingsChanges.h>

#include <Poco/File.h>
#include <Poco/Semaphore.h>
#include <common/shared_ptr_helper.h>

#include <mutex>
#include <list>
#include <atomic>

namespace DB
{
class StorageFileLog final : public shared_ptr_helper<StorageFileLog>, public IStorage, WithContext
{
    friend struct shared_ptr_helper<StorageFileLog>;

public:
    using Files = std::vector<String>;

    std::string getName() const override { return "FileLog"; }

    bool noPushingToViews() const override { return true; }

    void startup() override;
    void shutdown() override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    const auto & getFormatName() const { return format_name; }

    NamesAndTypesList getVirtuals() const override;
    static Names getVirtualColumnNames();

    auto & getBuffer() { return buffer; }

protected:
    StorageFileLog(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        const String & path_,
        const String & format_name_);

private:
    const String path;
    const String format_name;
    Poco::Logger * log;

    ReadBufferFromFileLogPtr buffer;

    std::mutex mutex;

    // Stream thread
    struct TaskContext
    {
        BackgroundSchedulePool::TaskHolder holder;
        std::atomic<bool> stream_cancelled {false};
        explicit TaskContext(BackgroundSchedulePool::TaskHolder&& task_) : holder(std::move(task_))
        {
        }
    };
    std::shared_ptr<TaskContext> task;

    void createReadBuffer();
    void destroyReadBuffer();

    void threadFunc();

    size_t getPollMaxBatchSize() const;
    size_t getMaxBlockSize() const;
    size_t getPollTimeoutMillisecond() const;

    bool streamToViews();
    bool checkDependencies(const StorageID & table_id);
};

}
