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
    , read_position(executor->getPosition())
{
}

bool PipelineReadBuffer::nextImpl()
{
    while (true)
    {
        /// Try next node from current rope
        while (!current_rope.empty())
        {
            current_node = current_rope.popFront();
            has_current_node = true;

            size_t node_start = current_node.logical_offset;
            size_t node_end = node_start + current_node.size;

            /// Skip nodes entirely before our read position
            if (node_end <= read_position)
                continue;

            /// Trim the front of the node if it starts before our position
            char * data_start = current_node.data();
            size_t usable_size = current_node.size;

            if (node_start < read_position)
            {
                size_t skip = read_position - node_start;
                data_start += skip;
                usable_size -= skip;
            }

            if (usable_size == 0)
                continue;

            internal_buffer = Buffer(data_start, data_start + usable_size);
            working_buffer = internal_buffer;
            pos = working_buffer.begin();
            read_position = node_start + current_node.size; /// advance past the full node
            return true;
        }

        /// Current rope exhausted — get next window from executor
        current_rope = executor->readNextWindow();
        has_current_node = false;

        if (current_rope.empty())
            return false; /// EOF
    }
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

    /// Check if seek target is within the current node's data
    if (has_current_node)
    {
        size_t node_start = current_node.logical_offset;
        size_t node_end = node_start + current_node.size;
        if (new_pos >= node_start && new_pos < node_end)
        {
            size_t offset_in_node = new_pos - node_start;
            char * data_start = current_node.data();
            internal_buffer = Buffer(data_start + offset_in_node, data_start + current_node.size);
            working_buffer = internal_buffer;
            pos = working_buffer.begin();
            read_position = node_end;
            return new_pos;
        }
    }

    /// Check if seek target is within the remaining rope
    if (!current_rope.empty())
    {
        auto rope_range = current_rope.range();
        if (new_pos >= rope_range.offset && new_pos < rope_range.end())
        {
            /// Target is in the rope — just adjust read_position,
            /// nextImpl will skip/trim nodes to the right place.
            read_position = new_pos;
            has_current_node = false;
            resetWorkingBuffer();
            return new_pos;
        }
    }

    /// Target is outside what we have — ask the executor
    executor->seek(new_pos);
    current_rope = {};
    has_current_node = false;
    read_position = new_pos;

    resetWorkingBuffer();

    return new_pos;
}

off_t PipelineReadBuffer::getPosition()
{
    /// read_position points to the end of the last served node.
    /// Subtract unread bytes remaining in working_buffer.
    return read_position - available();
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
