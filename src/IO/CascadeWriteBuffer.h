#pragma once
#include <functional>
#include <IO/WriteBuffer.h>


namespace DB
{

namespace ErrorCodes
{
}

/* The buffer is similar to ConcatReadBuffer, but writes data
 *
 * It has WriteBuffers sequence [prepared_sources, lazy_sources]
 * (lazy_sources contains not pointers themself, but their delayed constructors)
 *
 * Firtly, CascadeWriteBuffer redirects data to first buffer of the sequence
 * If current WriteBuffer cannot receive data anymore, it throws special exception CURRENT_WRITE_BUFFER_IS_EXHAUSTED in nextImpl() body,
 *  CascadeWriteBuffer prepare next buffer and continuously redirects data to it.
 * If there are no buffers anymore CascadeWriteBuffer throws an exception.
 *
 * NOTE: If you use one of underlying WriteBuffers buffers outside, you need sync its position() with CascadeWriteBuffer's position().
 * The sync is performed into nextImpl(), getResultBuffers() and destructor.
 */
class CascadeWriteBuffer : public WriteBuffer
{
public:

    using WriteBufferPtrs = std::vector<WriteBufferPtr>;
    using WriteBufferConstructor = std::function<WriteBufferPtr (const WriteBufferPtr & prev_buf)>;
    using WriteBufferConstructors = std::vector<WriteBufferConstructor>;

    explicit CascadeWriteBuffer(WriteBufferPtrs && prepared_sources_, WriteBufferConstructors && lazy_sources_ = {});

    void nextImpl() override;

    /// Should be called once
    void getResultBuffers(WriteBufferPtrs & res);

    const WriteBuffer * getCurrentBuffer() const
    {
        return curr_buffer;
    }

    ~CascadeWriteBuffer() override;

    void finalizeImpl() override;

private:

    WriteBuffer * setNextBuffer();

    WriteBufferPtrs prepared_sources;
    WriteBufferConstructors lazy_sources;
    size_t first_lazy_source_num;
    size_t num_sources;

    WriteBuffer * curr_buffer;
    size_t curr_buffer_num;
};

}
