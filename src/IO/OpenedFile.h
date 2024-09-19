#pragma once

#include <Common/CurrentMetrics.h>
#include <memory>
#include <mutex>


namespace CurrentMetrics
{
    extern const Metric OpenFileForRead;
}


namespace DB
{

/// RAII for readonly opened file descriptor.
class OpenedFile
{
public:
    OpenedFile(const std::string & file_name_, int flags_);
    ~OpenedFile();

    /// Close prematurely.
    void close();

    int getFD() const;
    std::string getFileName() const;

private:
    std::string file_name;
    int flags = 0;

    mutable int fd = -1;
    mutable std::mutex mutex;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForRead};

    void open() const;
};

}
