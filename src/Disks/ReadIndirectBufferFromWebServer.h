#pragma once

#include <IO/SeekableReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadSettings.h>
#include <Interpreters/Context.h>


namespace DB
{

/* Read buffer, which reads via http, but is used as ReadBufferFromFileBase.
 * Used to read files, hosted on a web server with static files.
 *
 * Usage: ReadIndirectBufferFromRemoteFS -> SeekAvoidingReadBuffer -> ReadIndirectBufferFromWebServer -> ReadWriteBufferFromHTTP.
 */
class ReadIndirectBufferFromWebServer : public BufferWithOwnMemory<SeekableReadBuffer>
{
public:
    explicit ReadIndirectBufferFromWebServer(const String & url_,
                                             ContextPtr context_,
                                             size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE,
                                             const ReadSettings & settings_ = {});

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

private:
    std::unique_ptr<ReadBuffer> initialize();

    void initializeWithRetry();

    Poco::Logger * log;
    ContextPtr context;

    const String url;
    size_t buf_size;

    std::unique_ptr<ReadBuffer> impl;

    off_t offset = 0;
    ReadSettings read_settings;
};

}
