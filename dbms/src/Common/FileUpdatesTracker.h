#pragma once

#include <Poco/File.h>
#include <Poco/Timestamp.h>

#include <string>


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
        return Poco::File(path).getLastModified();
    }
};
