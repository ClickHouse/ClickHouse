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
    MMapReadBufferFromFile(const std::string & file_name_, size_t offset, size_t length_);

    /// Map till end of file.
    MMapReadBufferFromFile(const std::string & file_name_, size_t offset);

    ~MMapReadBufferFromFile() override;

    void close();

    std::string getFileName() const override;

    bool isRegularLocalFile(size_t * out_view_offset) override;

private:
    int fd = -1;
    std::string file_name;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForRead};

    void open();
};

}

