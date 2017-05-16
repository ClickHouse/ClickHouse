#pragma once

#include <string>
#include <common/logger_useful.h>
#include <Poco/File.h>


namespace DB
{

/// stores the sizes of all columns, and can check whether the columns are corrupted
class FileChecker
{
private:
    /// File name -> size.
    using Map = std::map<std::string, size_t>;

public:
    using Files = std::vector<Poco::File>;

    FileChecker(const std::string & file_info_path_);
    void setPath(const std::string & file_info_path_);
    void update(const Poco::File & file);
    void update(const Files::const_iterator & begin, const Files::const_iterator & end);

    /// Check the files whose parameters are specified in sizes.json
    bool check() const;

private:
    void initialize();
    void updateImpl(const Poco::File & file);
    void save() const;
    void load(Map & map) const;

    std::string files_info_path;
    std::string tmp_files_info_path;

    /// The data from the file is read lazily.
    Map map;
    bool initialized = false;

    Logger * log = &Logger::get("FileChecker");
};

}
