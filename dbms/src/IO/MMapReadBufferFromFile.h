#pragma once

#include <Common/CurrentMetrics.h>
#include <IO/MMapReadBufferFromFileDescriptor.h>


namespace CurrentMetrics
{
    extern const Metric OpenFileForRead;
}


namespace DB
{

class MMapReadBufferFromFile : public MMapReadBufferFromFileDescriptor
{
public:
    MMapReadBufferFromFile(const std::string & file_name, size_t offset, size_t length);

    /// Map till end of file.
    MMapReadBufferFromFile(const std::string & file_name, size_t offset);

    ~MMapReadBufferFromFile() override;

    void close();

private:
    int fd = -1;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForRead};

    void open(const std::string & file_name);
};

}

