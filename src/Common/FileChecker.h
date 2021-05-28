#pragma once

#include <common/logger_useful.h>
#include <Storages/CheckResults.h>
#include <Disks/IDisk.h>


namespace DB
{

/// stores the sizes of all columns, and can check whether the columns are corrupted
class FileChecker
{
public:
    FileChecker(DiskPtr disk_, const String & file_info_path_);
    void setPath(const String & file_info_path_);

    void update(const String & full_file_path);
    void setEmpty(const String & full_file_path);
    void save() const;

    /// Check the files whose parameters are specified in sizes.json
    CheckResults check() const;

    /// Truncate files that have excessive size to the expected size.
    /// Throw exception if the file size is less than expected.
    /// The purpose of this function is to rollback a group of unfinished writes.
    void repair();

private:
    /// File name -> size.
    using Map = std::map<String, UInt64>;

    void initialize();
    void updateImpl(const String & file_path);
    void load(Map & local_map, const String & path) const;

    DiskPtr disk;
    String files_info_path;
    String tmp_files_info_path;

    /// The data from the file is read lazily.
    Map map;
    bool initialized = false;

    Poco::Logger * log = &Poco::Logger::get("FileChecker");
};

}
