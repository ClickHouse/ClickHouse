#include "config.h"

#if USE_SNAPPY
#include <snappy.h>
#include <snappy-sinksource.h>

#include <IO/SnappyBasicWriteBuffer.h>

namespace DB
{

namespace
{

/// A `snappy::Sink` that forwards compressed bytes directly into a `WriteBuffer`.
/// Avoids materializing a full-size compressed buffer in addition to the
/// accumulated uncompressed input.
class WriteBufferSnappySink : public snappy::Sink
{
public:
    explicit WriteBufferSnappySink(WriteBuffer & out_) : out(out_) {}
    void Append(const char * bytes, size_t n) override { out.write(bytes, n); }

private:
    WriteBuffer & out;
};

}

void SnappyBasicWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    uncompress_buffer.append(working_buffer.begin(), offset());
}

void SnappyBasicWriteBuffer::finalFlushBefore()
{
    next();

    /// Don't emit anything when no data was ever written and compress_empty is false
    /// (e.g. HTTP responses, where an empty body must stay zero bytes without a
    /// Content-Encoding header).
    if (uncompress_buffer.empty() && !compress_empty)
        return;

    snappy::ByteArraySource source(uncompress_buffer.data(), uncompress_buffer.size());
    WriteBufferSnappySink sink(*out);
    snappy::Compress(&source, &sink);
}

}

#endif
