#pragma once

#include <atomic>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadSettings.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/IReadBufferMetadataProvider.h>
#include <Core/Field.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/Net/HTTPBasicCredentials.h>


namespace DB
{

/* Read buffer, which reads via http, but is used as ReadBufferFromFileBase.
 * Used to read files, hosted on a web server with static files.
 */
class ReadBufferFromWebServer : public ReadBufferFromFileBase, public IReadBufferMetadataProvider
{
public:
    explicit ReadBufferFromWebServer(
        const String & url_,
        ContextPtr context_,
        size_t file_size_,
        const ReadSettings & settings_ = {},
        bool use_external_buffer_ = false,
        size_t read_until_position = 0,
        HTTPHeaderEntries headers_ = {});

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

    String getFileName() const override { return url; }

    void setReadUntilPosition(size_t position) override;

    size_t getFileOffsetOfBufferEnd() const override { return offset.load(std::memory_order_relaxed); }

    bool supportsRightBoundedReads() const override { return true; }

    Map getResponseHeaders() const;
    std::optional<Field> getMetadata(const String & name) const override;

private:
    std::unique_ptr<SeekableReadBuffer> initialize();

    LoggerPtr log;
    ContextPtr context;

    const String url;
    size_t buf_size;

    std::unique_ptr<SeekableReadBuffer> impl;

    ReadSettings read_settings;
    HTTPHeaderEntries headers;

    Poco::Net::HTTPBasicCredentials credentials{};

    bool use_external_buffer;

    /// atomic is required for CachedOnDiskReadBufferFromFile, which can access
    /// to this variable via getFileOffsetOfBufferEnd()/seek() from multiple
    /// threads.
    std::atomic<off_t> offset = 0;
    off_t read_until_position = 0;
};

}
