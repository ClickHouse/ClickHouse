#pragma once

#include <Storages/FileLog/Buffer_fwd.h>
#include <Storages/FileLog/FileLogSettings.h>

#include <Core/BackgroundSchedulePool.h>
#include <Storages/IStorage.h>
#include <Common/SettingsChanges.h>

#include <Poco/File.h>
#include <Poco/Semaphore.h>
#include <common/shared_ptr_helper.h>

#include <mutex>
#include <atomic>
#include <fstream>

namespace DB
{
class StorageFileLog final : public shared_ptr_helper<StorageFileLog>, public IStorage, WithContext
{
    friend struct shared_ptr_helper<StorageFileLog>;

public:
    enum class FileStatus
    {
        BEGIN,
        NO_CHANGE,
        UPDATED,
        REMOVED
    };

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

    auto & getFileNames() { return file_names; }
    auto & getFileStatus() { return file_status; }

protected:
    StorageFileLog(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        const String & path_,
        const String & format_name_,
        std::unique_ptr<FileLogSettings> settings);

private:
    std::unique_ptr<FileLogSettings> filelog_settings;
    const String path;
    bool path_is_directory = false;

    const String format_name;
    Poco::Logger * log;

    struct FileContext
    {
        FileStatus status = FileStatus::BEGIN;
        std::ifstream reader;
    };

    using NameToFile = std::unordered_map<String, FileContext>;
    NameToFile file_status;

    std::vector<String> file_names;

    std::mutex status_mutex;

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

    using TaskThread = BackgroundSchedulePool::TaskHolder;

    TaskThread watch_task;

    void threadFunc();

    void clearInvalidFiles();

    size_t getPollMaxBatchSize() const;
    size_t getMaxBlockSize() const;
    size_t getPollTimeoutMillisecond() const;

    bool streamToViews();
    bool checkDependencies(const StorageID & table_id);

    [[noreturn]] void watchFunc();
};

}
