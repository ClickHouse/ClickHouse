#pragma once
#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <stack>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Similar to PeekableReadBuffer.
/// Allows to set checkpoint at some position in stream and come back to this position later.
/// When next() is called, saves data between checkpoint and current position to own memory instead of writing it to sub-buffer.
/// So, all the data after checkpoint won't be written in sub-buffer until checkpoint is dropped.
/// Rollback to checkpoint means that all data after checkpoint will be ignored and not sent to sub-buffer.
/// Sub-buffer should not be accessed directly during the lifetime of peekable buffer (unless
/// you reset() the state of peekable buffer after each change of underlying buffer)
/// If position() of peekable buffer is explicitly set to some position before checkpoint
/// (e.g. by istr.position() = prev_pos), behavior is undefined.
class PeekableWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
    friend class PeekableWriteBufferCheckpoint;
public:
    explicit PeekableWriteBuffer(WriteBuffer & sub_buf_);

    /// Sets checkpoint at current position
    ALWAYS_INLINE inline void setCheckpoint()
    {
        if (checkpoint)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PeekableWriteBuffer does not support recursive checkpoints.");

        checkpoint.emplace(pos);
    }

    /// Forget checkpoint and send all data from checkpoint to position to sub-buffer.
    void dropCheckpoint();

    /// Sets position at checkpoint and forget all data written from checkpoint to position.
    /// All pointers (such as this->buffer().end()) may be invalidated
    void rollbackToCheckpoint(bool drop = false);

    void finalizeImpl() override
    {
        assert(!checkpoint);
        sub_buf.position() = position();
    }

private:
    void nextImpl() override;

    WriteBuffer & sub_buf;
    bool write_to_own_memory = false;
    std::optional<Position> checkpoint = std::nullopt;
};

}
