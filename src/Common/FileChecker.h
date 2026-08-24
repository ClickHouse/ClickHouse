#pragma once

#include <Storages/CheckResults.h>
#include <Common/Logger.h>
#include <map>
#include <base/types.h>
#include <memory>
#include <mutex>

namespace Poco { class Logger; }

namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;


/// Stores the sizes of all columns, and can check whether the columns are corrupted.
class FileChecker
{
public:
    explicit FileChecker(const String & file_info_path_);
    FileChecker(DiskPtr disk_, const String & file_info_path_);

    void setPath(const String & file_info_path_);
    String getPath() const;

    void update(const String & full_file_path);
    void setEmpty(const String & full_file_path);
    void save() const;
    bool empty() const { return map.empty(); }

    /// Check the files whose parameters are specified in sizes.json
    /// See comment in IStorage::checkDataNext
    struct DataValidationTasks;
    using DataValidationTasksPtr = std::unique_ptr<DataValidationTasks>;
    DataValidationTasksPtr getDataValidationTasks();
    std::optional<CheckResult> checkNextEntry(DataValidationTasksPtr & check_data_tasks) const;

    /// Truncate files that have excessive size to the expected size.
    /// Throw exception if the file size is less than expected.
    /// The purpose of this function is to rollback a group of unfinished writes.
    void repair();

    /// Returns stored file size.
    size_t getFileSize(const String & full_file_path) const;

    /// Returns total size of all files.
    size_t getTotalSize() const;

    struct DataValidationTasks
    {
        explicit DataValidationTasks(const std::map<String, size_t> & map_)
            : map(map_), it(map.begin())
        {}

        bool next(String & out_name, size_t & out_size)
        {
            std::lock_guard lock(mutex);
            if (it == map.end())
                return true;
            out_name = it->first;
            out_size = it->second;
            ++it;
            return false;
        }

        size_t size() const
        {
            std::lock_guard lock(mutex);
            return std::distance(it, map.end());
        }

        const std::map<String, size_t> & map;

        mutable std::mutex mutex;
        using Iterator = std::map<String, size_t>::const_iterator;
        Iterator it;
    };

private:
    void load();

    bool fileReallyExists(const String & path_) const;
    size_t getRealFileSize(const String & path_) const;

    const DiskPtr disk;
    const LoggerPtr log;

    String files_info_path;
    std::map<String, size_t> map;
};

}
