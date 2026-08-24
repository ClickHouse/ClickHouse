#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>

#include <Poco/Net/HTTPBasicStreamBuf.h>


namespace DB
{

class ReadBufferFromIStream : public BufferWithOwnMemory<ReadBuffer>
{
private:
    struct StreamHolder
    {
        std::istream & istr;
        Poco::Net::HTTPBasicStreamBuf & buf;
    };

    std::optional<StreamHolder> stream_holder;

    bool nextImpl() override;
    bool eof = false;

public:
    explicit ReadBufferFromIStream(std::istream & istr_, size_t size = DBMS_DEFAULT_BUFFER_SIZE);

    /// Allows to safely detach the referenced istream, e.g. when released to free resources.
    /// The buffer can still be used, but subsequent reads won't return any more data.
    void detachStream();

    /// Returns true when the referenced stream is eof, not when all data was read from buffer as eof() does
    bool isStreamEof() const;
};

}
