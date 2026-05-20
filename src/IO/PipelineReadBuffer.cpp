#include <IO/PipelineReadBuffer.h>
#include <IO/ReaderExecutor.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

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
    LOG_DEBUG(log, "Created, total_size={}, read_position={}", executor->totalSize(), read_position);
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
            {
                LOG_TRACE(log, "nextImpl: skipping node [{}, {}), read_position={}", node_start, node_end, read_position);
                continue;
            }

            /// Trim the front of the node if it starts before our position
            char * data_start = current_node.data();
            size_t usable_size = current_node.size;

            if (node_start < read_position)
            {
                size_t skip = read_position - node_start;
                LOG_TRACE(log, "nextImpl: trimming {} bytes from node [{}, {}), read_position={}", skip, node_start, node_end, read_position);
                data_start += skip;
                usable_size -= skip;
            }

            if (usable_size == 0)
                continue;

            internal_buffer = Buffer(data_start, data_start + usable_size);
            working_buffer = internal_buffer;
            pos = working_buffer.begin();
            read_position = node_start + current_node.size;
            LOG_TRACE(log, "nextImpl: serving {} bytes at offset {}, read_position advanced to {}",
                usable_size, node_start, read_position);
            return true;
        }

        /// Current rope exhausted — get next window from executor
        LOG_TRACE(log, "nextImpl: rope exhausted, requesting next window at position {}", read_position);
        current_rope = executor->readNextWindow();
        has_current_node = false;

        if (current_rope.empty())
        {
            LOG_TRACE(log, "nextImpl: EOF");
            return false;
        }

        LOG_TRACE(log, "nextImpl: got rope [{}, {}), {} nodes",
            current_rope.range().offset, current_rope.range().end(), current_rope.getNodes().size());
    }
}

off_t PipelineReadBuffer::seek(off_t off, int whence)
{
    size_t new_pos;
    if (whence == SEEK_SET)
    {
        if (off < 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "PipelineReadBuffer::seek: SEEK_SET with negative offset {}", off);
        new_pos = static_cast<size_t>(off);
    }
    else if (whence == SEEK_CUR)
    {
        off_t cur = getPosition();
        if (off < 0 && static_cast<size_t>(-off) > static_cast<size_t>(cur))
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "PipelineReadBuffer::seek: SEEK_CUR offset {} from position {} would underflow",
                off, cur);
        new_pos = static_cast<size_t>(cur + off);
    }
    else
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "PipelineReadBuffer::seek: unsupported whence");

    LOG_DEBUG(log, "seek to {}", new_pos);

    /// Check if seek target is within the current node's data
    if (has_current_node)
    {
        size_t node_start = current_node.logical_offset;
        size_t node_end = node_start + current_node.size;
        if (new_pos >= node_start && new_pos < node_end)
        {
            LOG_TRACE(log, "seek: found in current node [{}, {})", node_start, node_end);
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
            LOG_TRACE(log, "seek: found in remaining rope [{}, {})", rope_range.offset, rope_range.end());
            read_position = new_pos;
            has_current_node = false;
            resetWorkingBuffer();
            return new_pos;
        }
    }

    /// Target is outside what we have — ask the executor
    LOG_TRACE(log, "seek: delegating to executor");
    executor->seek(new_pos);
    current_rope = {};
    has_current_node = false;
    read_position = new_pos;

    resetWorkingBuffer();

    return new_pos;
}

off_t PipelineReadBuffer::getPosition()
{
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
