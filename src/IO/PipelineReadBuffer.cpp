#include <IO/PipelineReadBuffer.h>
#include <IO/ReaderExecutor.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

PipelineReadBuffer::PipelineReadBuffer(std::unique_ptr<ReaderExecutor> executor_)
    : ReadBufferFromFileBase(0, nullptr, 0)
    , executor(std::move(executor_))
{
}

bool PipelineReadBuffer::nextImpl()
{
    /// Try next node from current rope
    if (!current_rope.empty())
    {
        current_node = current_rope.popFront();
        has_current_node = true;

        if (current_node.size > 0)
        {
            internal_buffer = Buffer(current_node.data(), current_node.data() + current_node.size);
            working_buffer = internal_buffer;
            pos = working_buffer.begin();
            return true;
        }
    }

    /// Current rope exhausted — get next window from executor
    current_rope = executor->readNextWindow();

    if (current_rope.empty())
        return false;

    return nextImpl();
}

off_t PipelineReadBuffer::seek(off_t off, int whence)
{
    size_t new_pos;
    if (whence == SEEK_SET)
        new_pos = off;
    else if (whence == SEEK_CUR)
        new_pos = getPosition() + off;
    else
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "PipelineReadBuffer::seek: unsupported whence");

    executor->seek(new_pos);
    current_rope = {};
    has_current_node = false;

    resetWorkingBuffer();

    return new_pos;
}

off_t PipelineReadBuffer::getPosition()
{
    if (has_current_node)
    {
        /// Position = node's logical end minus unread bytes in working_buffer
        size_t node_end = current_node.logical_offset + current_node.size;
        return node_end - available();
    }
    return executor->getPosition();
}

std::optional<size_t> PipelineReadBuffer::tryGetFileSize()
{
    return executor->totalSize();
}

String PipelineReadBuffer::getFileName() const
{
    return "PipelineReadBuffer";
}

}
