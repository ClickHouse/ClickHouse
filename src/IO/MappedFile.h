#pragma once

#include <Common/CurrentMetrics.h>
#include <IO/MappedFileDescriptor.h>
#include <cstddef>


namespace CurrentMetrics
{
    extern const Metric OpenFileForRead;
}


namespace DB
{

/// Opens a file and mmaps a region in it (or a whole file) into memory. Unmaps and closes in destructor.
class MappedFile : public MappedFileDescriptor
{
public:
    MappedFile(const std::string & file_name_, size_t offset_, size_t length_);

    /// Map till end of file.
    MappedFile(const std::string & file_name_, size_t offset_);

    ~MappedFile() override;

    void close();

    std::string getFileName() const;

private:
    std::string file_name;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForRead};

    void open();
};

}
