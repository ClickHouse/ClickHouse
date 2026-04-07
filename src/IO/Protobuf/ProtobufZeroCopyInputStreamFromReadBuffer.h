#pragma once

#include "config.h"
#if USE_PROTOBUF

#include <google/protobuf/io/zero_copy_stream.h>


namespace DB
{
class ReadBuffer;

class ProtobufZeroCopyInputStreamFromReadBuffer : public google::protobuf::io::ZeroCopyInputStream
{
public:
    explicit ProtobufZeroCopyInputStreamFromReadBuffer(std::unique_ptr<ReadBuffer> in_);
    ~ProtobufZeroCopyInputStreamFromReadBuffer() override;

    // Obtains a chunk of data from the stream.
    bool Next(const void ** data, int * size) override;

    // Backs up a number of bytes, so that the next call to Next() returns
    // data again that was already returned by the last call to Next().
    void BackUp(int count) override;

    // Skips a number of bytes.
    bool Skip(int count) override;

    // Returns the total number of bytes read since this object was created.
    int64_t ByteCount() const override;

private:
    std::unique_ptr<ReadBuffer> in;
};

}

#endif
