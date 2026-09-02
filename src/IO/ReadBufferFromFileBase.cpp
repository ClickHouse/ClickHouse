#include <IO/ReadBufferFromFileBase.h>
#include <IO/Progress.h>
#include <Interpreters/Context.h>

namespace DB
{

ReadBufferFromFileBase::ReadBufferFromFileBase() : BufferWithOwnMemory<SeekableReadBuffer>(0)
{
}

ReadBufferFromFileBase::ReadBufferFromFileBase(
    size_t buf_size,
    char * existing_memory,
    size_t alignment,
    std::optional<size_t> file_size_)
    : BufferWithOwnMemory<SeekableReadBuffer>(buf_size, existing_memory, alignment)
    , file_size(file_size_)
{
}

ReadBufferFromFileBase::~ReadBufferFromFileBase() = default;

std::optional<size_t> ReadBufferFromFileBase::tryGetFileSize()
{
    return file_size;
}

void ReadBufferFromFileBase::setProgressCallback(ContextPtr context)
{
    auto file_progress_callback = context->getFileProgressCallback();

    if (!file_progress_callback)
        return;

    setProfileCallback([file_progress_callback](const ProfileInfo & progress)
    {
       file_progress_callback(FileProgress(progress.bytes_read));
    });
}

}
