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
    void update(const String & file_path);
    void update(const Strings::const_iterator & begin, const Strings::const_iterator & end);

    /// Check the files whose parameters are specified in sizes.json
    CheckResults check() const;

private:
    /// File name -> size.
    using Map = std::map<String, UInt64>;

    void initialize();
    void updateImpl(const String & file_path);
    void save() const;
    void load(Map & local_map, const String & path) const;

    DiskPtr disk;
    String files_info_path;
    String tmp_files_info_path;

    /// The data from the file is read lazily.
    Map map;
    bool initialized = false;

    Logger * log = &Logger::get("FileChecker");
};

}
