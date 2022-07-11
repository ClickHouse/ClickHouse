#pragma once

#include <Common/logger_useful.h>
#include <Storages/CheckResults.h>


namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;


/// Stores the sizes of all columns, and can check whether the columns are corrupted.
class FileChecker
{
public:
    FileChecker(const String & file_info_path_);
    FileChecker(DiskPtr disk_, const String & file_info_path_);

    void setPath(const String & file_info_path_);
    String getPath() const;

    void update(const String & full_file_path);
    void setEmpty(const String & full_file_path);
    void save() const;
    bool empty() const { return map.empty(); }

    /// Check the files whose parameters are specified in sizes.json
    CheckResults check() const;

    /// Truncate files that have excessive size to the expected size.
    /// Throw exception if the file size is less than expected.
    /// The purpose of this function is to rollback a group of unfinished writes.
    void repair();

    /// Returns stored file size.
    size_t getFileSize(const String & full_file_path) const;

    /// Returns total size of all files.
    size_t getTotalSize() const;

private:
    void load();

    bool fileReallyExists(const String & path_) const;
    size_t getRealFileSize(const String & path_) const;

    const DiskPtr disk;
    const Poco::Logger * log = &Poco::Logger::get("FileChecker");

    String files_info_path;
    std::map<String, size_t> map;
};

}
