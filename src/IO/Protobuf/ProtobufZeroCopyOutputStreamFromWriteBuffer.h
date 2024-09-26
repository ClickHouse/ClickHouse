#pragma once

#include "config.h"
#if USE_PROTOBUF

#include <google/protobuf/io/zero_copy_stream.h>


namespace DB
{
class WriteBuffer;

class ProtobufZeroCopyOutputStreamFromWriteBuffer : public google::protobuf::io::ZeroCopyOutputStream
{
public:
    explicit ProtobufZeroCopyOutputStreamFromWriteBuffer(WriteBuffer & out_);
    explicit ProtobufZeroCopyOutputStreamFromWriteBuffer(std::unique_ptr<WriteBuffer> out_);

    ~ProtobufZeroCopyOutputStreamFromWriteBuffer() override;

    // Obtains a buffer into which data can be written.
    bool Next(void ** data, int * size) override;

    // Backs up a number of bytes, so that the end of the last buffer returned
    // by Next() is not actually written.
    void BackUp(int count) override;

    // Returns the total number of bytes written since this object was created.
    int64_t ByteCount() const override;

    void finalize();

private:
    WriteBuffer * out;
    std::unique_ptr<WriteBuffer> out_holder;
};

}

#endif
