#pragma once

#include <IO/SeekableReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadSettings.h>
#include <Interpreters/Context.h>
#include <Poco/Net/HTTPBasicCredentials.h>


namespace DB
{

/* Read buffer, which reads via http, but is used as ReadBufferFromFileBase.
 * Used to read files, hosted on a web server with static files.
 *
 * Usage: ReadIndirectBufferFromRemoteFS -> SeekAvoidingReadBuffer -> ReadBufferFromWebServer -> ReadWriteBufferFromHTTP.
 */
class ReadBufferFromWebServer : public SeekableReadBuffer
{
public:
    explicit ReadBufferFromWebServer(
        const String & url_, ContextPtr context_,
        const ReadSettings & settings_ = {},
        bool use_external_buffer_ = false,
        size_t read_until_position = 0);

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

    size_t getFileOffsetOfBufferEnd() const override { return offset; }

private:
    std::unique_ptr<ReadBuffer> initialize();

    Poco::Logger * log;
    ContextPtr context;

    const String url;
    size_t buf_size;

    std::unique_ptr<ReadBuffer> impl;

    ReadSettings read_settings;

    Poco::Net::HTTPBasicCredentials credentials{};

    bool use_external_buffer;

    off_t offset = 0;
    off_t read_until_position = 0;
};

}
