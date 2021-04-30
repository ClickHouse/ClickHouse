#pragma once

#include <Poco/Timestamp.h>
#include <string>
#include <filesystem>

namespace fs = std::filesystem;

class FileUpdatesTracker
{
private:
    std::string path;
    Poco::Timestamp known_time;

public:
    FileUpdatesTracker(const std::string & path_)
        : path(path_)
        , known_time(0)
    {}

    bool isModified() const
    {
        return getLastModificationTime() > known_time;
    }

    void fixCurrentVersion()
    {
        known_time = getLastModificationTime();
    }

private:
    Poco::Timestamp getLastModificationTime() const
    {
        fs::file_time_type fs_time = fs::last_write_time(path);
        auto micro_sec = std::chrono::duration_cast<std::chrono::microseconds>(fs_time.time_since_epoch());
        return Poco::Timestamp(micro_sec.count());
    }
};
