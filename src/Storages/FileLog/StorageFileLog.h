#pragma once

#include <Disks/IDisk.h>

#include <Storages/FileLog/Buffer_fwd.h>
#include <Storages/FileLog/FileLogDirectoryWatcher.h>

#include <Core/BackgroundSchedulePool.h>
#include <Core/StreamingHandleErrorMode.h>
#include <Storages/IStorage.h>
#include <Common/SettingsChanges.h>

#include <atomic>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <optional>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class FileLogDirectoryWatcher;
struct FileLogSettings;

class StorageFileLog final : public IStorage, WithContext
{
public:
    StorageFileLog(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        const String & path_,
        const String & metadata_base_path_,
        const String & format_name_,
        std::unique_ptr<FileLogSettings> settings,
        const String & comment,
        LoadingStrictnessLevel mode);

    using Files = std::vector<String>;

    std::string getName() const override { return "FileLog"; }

    bool noPushingToViews() const override { return true; }

    void startup() override;
    void shutdown(bool is_drop) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    void drop() override;

    const auto & getFormatName() const { return format_name; }

    enum class FileStatus : uint8_t
    {
        OPEN, /// First time open file after table start up.
        NO_CHANGE,
        UPDATED,
        REMOVED,
    };

    struct FileContext
    {
        FileStatus status = FileStatus::OPEN;
        UInt64 inode{};
        std::optional<std::ifstream> reader = std::nullopt;
    };

    struct FileMeta
    {
        String file_name;
        UInt64 last_writen_position = 0;
        UInt64 last_open_end = 0;
        bool operator!() const { return file_name.empty(); }
    };

    using InodeToFileMeta = std::unordered_map<UInt64, FileMeta>;
    using FileNameToContext = std::unordered_map<String, FileContext>;

    struct FileInfos
    {
        InodeToFileMeta meta_by_inode;
        FileNameToContext context_by_name;
        /// File names without path.
        Names file_names;
    };

    auto & getFileInfos() { return file_infos; }

    String getFullMetaPath(const String & file_name) const { return std::filesystem::path(metadata_base_path) / file_name; }
    String getFullDataPath(const String & file_name) const { return std::filesystem::path(root_data_path) / file_name; }

    static UInt64 getInode(const String & file_name);

    void openFilesAndSetPos();

    /// Used in FileLogSource when finish generating all blocks.
    /// Each stream responsible for close its files and store meta.
    void closeFilesAndStoreMeta(size_t start, size_t end);

    /// Used in FileLogSource after generating every block
    void storeMetas(size_t start, size_t end);

    static void assertStreamGood(const std::ifstream & reader);

    template <typename K, typename V>
    static V & findInMap(std::unordered_map<K, V> & map, const K & key)
    {
        if (auto it = map.find(key); it != map.end())
            return it->second;
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The key {} doesn't exist.", key);
    }

    void increaseStreams();
    void reduceStreams();

    void wakeUp();

    const auto & getFileLogSettings() const { return filelog_settings; }

private:
    friend class ReadFromStorageFileLog;

    std::unique_ptr<FileLogSettings> filelog_settings;

    const String path;
    bool path_is_directory = true;

    /// If path argument of the table is a regular file, it equals to user_files_path
    /// otherwise, it equals to user_files_path/ + path_argument/, e.g. path
    String root_data_path;
    String metadata_base_path;

    FileInfos file_infos;

    const String format_name;
    LoggerPtr log;

    DiskPtr disk;

    uint64_t milliseconds_to_wait;

    /// In order to avoid data race, using a naive trick to forbid execute two select
    /// simultaneously, although read is not useful in this engine. Using an atomic
    /// variable to records current unfinishing streams, then if have unfinishing streams,
    /// later select should forbid to execute.
    std::atomic<int> running_streams = 0;

    std::mutex mutex;
    bool has_new_events = false;
    std::condition_variable cv;

    std::atomic<bool> mv_attached = false;

    std::mutex file_infos_mutex;

    struct TaskContext
    {
        BackgroundSchedulePool::TaskHolder holder;
        std::atomic<bool> stream_cancelled {false};
        explicit TaskContext(BackgroundSchedulePool::TaskHolder&& task_) : holder(std::move(task_))
        {
        }
    };
    std::shared_ptr<TaskContext> task;

    std::unique_ptr<FileLogDirectoryWatcher> directory_watch;

    void loadFiles();

    void loadMetaFiles(bool attach);

    void threadFunc();

    size_t getPollMaxBatchSize() const;
    size_t getMaxBlockSize() const;
    size_t getPollTimeoutMillisecond() const;

    bool streamToViews();
    bool checkDependencies(const StorageID & table_id);

    bool updateFileInfos();

    size_t getTableDependentCount() const;

    /// Used in shutdown()
    void serialize() const;
    /// Used in FileSource closeFileAndStoreMeta(file_name).
    void serialize(UInt64 inode, const FileMeta & file_meta) const;

    void deserialize();
    void checkOffsetIsValid(const String & filename, UInt64 offset) const;

    struct ReadMetadataResult
    {
        FileMeta metadata;
        UInt64 inode = 0;
    };
    ReadMetadataResult readMetadata(const String & filename) const;

    static VirtualColumnsDescription createVirtuals(StreamingHandleErrorMode handle_error_mode);
};

}
