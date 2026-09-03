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
private:
    bool nextImpl() override { return false; }
    std::string getFileName() const override { return "<empty>"; }
    off_t seek(off_t /*off*/, int /*whence*/) override { return 0; }
    off_t getPosition() override { return 0; }
    std::optional<size_t> tryGetFileSize() override { return 0; }
    size_t getFileOffsetOfBufferEnd() const override { return 0; }
};

}
