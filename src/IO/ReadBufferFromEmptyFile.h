#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>

namespace DB
{

/// In case of empty file it does not make any sense to read it.
///
/// Plus regular readers from file has an assert that buffer is not empty, that will fail:
/// - ReadBufferFromFileDescriptor
/// - SynchronousReader
/// - ThreadPoolReader
class ReadBufferFromEmptyFile : public ReadBufferFromFileBase
{
public:
    explicit ReadBufferFromEmptyFile(const String & file_name_) : file_name(file_name_) {}

private:
    String file_name;

    bool nextImpl() override { return false; }
    std::string getFileName() const override { return file_name; }
    off_t seek(off_t /*off*/, int /*whence*/) override { return 0; }
    off_t getPosition() override { return 0; }
    size_t getFileSize() override { return 0; }
    size_t getFileOffsetOfBufferEnd() const override { return 0; }
};

}
