#pragma once

#include <Common/CurrentMetrics.h>
#include <IO/MMappedFileDescriptor.h>
#include <cstddef>


namespace CurrentMetrics
{
    extern const Metric OpenFileForRead;
}


namespace DB
{

/// Opens a file and mmaps a region in it (or a whole file) into memory. Unmaps and closes in destructor.
class MMappedFile : public MMappedFileDescriptor
{
public:
    MMappedFile(const std::string & file_name_, size_t offset_, size_t length_);

    /// Map till end of file.
    MMappedFile(const std::string & file_name_, size_t offset_);

    ~MMappedFile() override;

    void close();

    std::string getFileName() const;

private:
    std::string file_name;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForRead};

    void open();
};

}
