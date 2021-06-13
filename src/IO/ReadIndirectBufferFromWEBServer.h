#pragma once

#include <IO/SeekableReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Interpreters/Context.h>


namespace DB
{

/* Read buffer, which reads via http, but is used as ReadBufferFromFileBase.
 * Used to read files, hosted on a WEB server with static files.
 *
 * Usage: ReadIndirectBufferFromRemoteFS -> SeekAvoidingReadBuffer -> ReadIndirectBufferFromWEBServer -> ReadWriteBufferFromHTTP.
 */
class ReadIndirectBufferFromWEBServer : public BufferWithOwnMemory<SeekableReadBuffer>
{
public:
    explicit ReadIndirectBufferFromWEBServer(const String & url_,
                                  ContextPtr context,
                                  UInt64 max_read_tries_,
                                  size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

private:
    std::unique_ptr<ReadBuffer> initialize();

    Poco::Logger * log;
    ContextPtr context;

    const String url;
    size_t buffer_size, max_read_tries;

    std::unique_ptr<ReadBuffer> impl;

    off_t offset = 0;
};

}
