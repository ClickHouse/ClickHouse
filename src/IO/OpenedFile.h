#pragma once

#include <Common/CurrentMetrics.h>
#include <memory>


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
    OpenedFile(const std::string & file_name_, int flags);
    ~OpenedFile();

    /// Close prematurally.
    void close();

    int getFD() const { return fd; }
    std::string getFileName() const;

private:
    std::string file_name;
    int fd = -1;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForRead};

    void open(int flags);
};

}

