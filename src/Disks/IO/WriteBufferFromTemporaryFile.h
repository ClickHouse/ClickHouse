#pragma once
#include <IO/WriteBuffer.h>
#include <IO/IReadableWriteBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <Common/filesystemHelpers.h>


namespace DB
{

class TemporaryFileOnDisk;
using TemporaryFileOnDiskHolder = std::unique_ptr<TemporaryFileOnDisk>;

/// Rereadable WriteBuffer, could be used as disk buffer
/// Creates unique temporary in directory (and directory itself)
class WriteBufferFromTemporaryFile : public WriteBufferFromFile, public IReadableWriteBuffer
{
public:
    explicit WriteBufferFromTemporaryFile(TemporaryFileOnDiskHolder && tmp_file_);

    ~WriteBufferFromTemporaryFile() override;

private:
    std::unique_ptr<ReadBuffer> getReadBufferImpl() override;

    TemporaryFileOnDiskHolder tmp_file;

    friend class ReadBufferFromTemporaryWriteBuffer;
};

}
