#pragma once

#include <Storages/FileLog/Buffer_fwd.h>
#include <Storages/FileLog/FileLogDirectoryWatcher.h>
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

    void drop() override;

    bool dropTableImmediately() const override { return true; }

    const auto & getFormatName() const { return format_name; }

    enum class FileStatus
    {
        OPEN, /// first time open file after table start up
        NO_CHANGE,
        UPDATED,
        REMOVED,
    };

    struct FileContext
    {
        FileStatus status = FileStatus::OPEN;
        std::ifstream reader;
    };

    struct FileMeta
    {
        String file_name;
        UInt64 last_writen_position = 0;
        UInt64 last_open_end;
    };

    using FileNameToInode = std::unordered_map<String, UInt64>;
    using InodeToFileMeta = std::unordered_map<UInt64, FileMeta>;
    using FileNameToContext = std::unordered_map<String, FileContext>;

    struct FileInfos
    {
        FileNameToInode inode_by_name;
        InodeToFileMeta meta_by_inode;
        FileNameToContext context_by_name;
        /// file names without path
        Names file_names;
    };

    auto & getFileInfos() { return file_infos; }

    auto getFullMetaPath(const String & file_name) const { return root_meta_path + "/" + file_name; }
    auto getFullDataPath(const String & file_name) const { return root_data_path + "/" + file_name; }

    NamesAndTypesList getVirtuals() const override;

    static Names getVirtualColumnNames();

    static UInt64 getInode(const String & file_name);

protected:
    StorageFileLog(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        const String & relative_path_,
        const String & format_name_,
        std::unique_ptr<FileLogSettings> settings,
        bool attach);

private:
    std::unique_ptr<FileLogSettings> filelog_settings;

    /// user_files_path/ + path_argument/
    const String path;
    bool path_is_directory = true;

    /// If path argument of the table is a regular file, it equals to user_files_path
    /// otherwise, it equals to user_files_path/ + path_argument/, e.g. path
    String root_data_path;
    /// Database meta_path/ + .table_name/
    String root_meta_path;

    FileInfos file_infos;

    const String format_name;
    Poco::Logger * log;

    std::mutex status_mutex;

    std::unique_ptr<FileLogDirectoryWatcher> directory_watch = nullptr;

    uint64_t milliseconds_to_wait;

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

    void loadFiles();

    void loadMetaFiles(bool attach);

    void threadFunc();

    size_t getPollMaxBatchSize() const;
    size_t getMaxBlockSize() const;
    size_t getPollTimeoutMillisecond() const;

    bool streamToViews();
    bool checkDependencies(const StorageID & table_id);

    bool updateFileInfos();

    void openFilesAndSetPos();
    void closeFilesAndStoreMeta();

    /// Serialize all file meta
    void serialize(bool with_end_pos = false) const;

    void deserialize();
};

}
