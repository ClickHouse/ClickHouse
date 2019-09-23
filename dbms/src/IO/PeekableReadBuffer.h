#pragma once
#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

/// Allows to peek next part of data from sub-buffer without extracting it.
/// Also allows to set checkpoint at some position in stream and come back to this position later,
/// even if next() was called.
/// Sub-buffer should not be accessed directly during the lifelime of peekable buffer.
/// If position() of peekable buffer is explicitly set to some position before checkpoint
/// (e.g. by istr.position() = prev_pos), behavior is undefined.
class PeekableReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
    friend class PeekableReadBufferCheckpoint;
public:
    explicit PeekableReadBuffer(ReadBuffer & sub_buf_, size_t start_size_ = DBMS_DEFAULT_BUFFER_SIZE,
                                                       size_t unread_limit_ = 16 * DBMS_DEFAULT_BUFFER_SIZE);

    /// Use takeUnreadData() to extract unread data before destruct object
    ~PeekableReadBuffer() override;

    /// Saves unread data to own memory, so it will be possible to read it later. Loads next data to sub-buffer.
    /// Doesn't change checkpoint and position in stream,
    /// but all pointers (such as this->buffer().end() and this->position()) may be invalidated
    /// @returns false in case of EOF in sub-buffer, otherwise returns true
    bool peekNext();

    Buffer & lastPeeked() { return sub_buf.buffer(); }

    /// Sets checkpoint at current position
    void setCheckpoint();

    /// Forget checkpoint and all data between checkpoint and position
    void dropCheckpoint();

    /// Sets position at checkpoint.
    /// All pointers (such as this->buffer().end()) may be invalidated
    void rollbackToCheckpoint();

    /// If position is in own memory, returns buffer with data, which were extracted from sub-buffer,
    /// but not from this buffer, so the data will not be lost after destruction of this buffer.
    /// If position is in sub-buffer, returns empty buffer.
    std::shared_ptr<BufferWithOwnMemory<ReadBuffer>> takeUnreadData();
    void assertCanBeDestructed() const;

private:

    bool nextImpl() override;

    inline bool useSubbufferOnly() const;
    inline bool currentlyReadFromOwnMemory() const;
    inline bool checkpointInOwnMemory() const;

    void checkStateCorrect() const;

    /// Makes possible to append `bytes_to_append` bytes to data in own memory.
    /// Updates all invalidated pointers and sizes.
    /// @returns new offset of unread data in own memory
    size_t resizeOwnMemoryIfNecessary(size_t bytes_to_append);


    ReadBuffer & sub_buf;
    const size_t unread_limit;
    size_t peeked_size = 0;
    Position checkpoint = nullptr;
    bool checkpoint_in_own_memory = false;
};


class PeekableReadBufferCheckpoint : boost::noncopyable
{
    PeekableReadBuffer & buf;
    bool auto_rollback;
public:
    explicit PeekableReadBufferCheckpoint(PeekableReadBuffer & buf_, bool auto_rollback_ = false)
                : buf(buf_), auto_rollback(auto_rollback_) { buf.setCheckpoint(); }
    ~PeekableReadBufferCheckpoint()
    {
        if (!buf.checkpoint)
            return;
        if (auto_rollback)
            buf.rollbackToCheckpoint();
        buf.dropCheckpoint();
    }

};

}
