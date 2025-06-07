#pragma once

#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/HTTP/HTTPServerResponse.h>

#include <IO/WriteBufferFromPocoSocket.h>

#include <Poco/Net/HTTPServerSession.h>

#include <memory>

namespace DB
{

class HTTP1ServerResponse;

class WriteBufferFromHTTP1ServerResponse : public WriteBufferFromHTTPServerResponseBase
{
public:
    WriteBufferFromHTTP1ServerResponse(
        HTTP1ServerResponse & response,
        bool is_http_method_head,
        const ProfileEvents::Event & write_event = ProfileEvents::end(),
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    void sendBufferAndFinalize(const char * ptr, size_t size) override;

    void onProgress(const Progress & progress) override;

    void setExceptionCode(int code) override;

    bool cancelWithException(int exception_code_, const std::string & message, WriteBuffer * compression_buffer) noexcept override;

private:
    /// Send at least HTTP headers if no data has been sent yet.
    /// Use after the data has possibly been sent and no error happened (and thus you do not plan
    /// to change response HTTP code.
    /// This method is idempotent.
    void finalizeImpl() override;

    void cancelImpl() noexcept override;

    /// Must be called under locked mutex.
    /// This method send headers, if this was not done already,
    ///  but not finish them with \r\n, allowing to send more headers subsequently.
    void startSendHeaders();

    /// Used to write the header X-ClickHouse-Progress / X-ClickHouse-Summary
    void writeHeaderProgressImpl(const char * header_name);
    /// Used to write the header X-ClickHouse-Progress
    void writeHeaderProgress();
    /// Used to write the header X-ClickHouse-Summary
    void writeHeaderSummary();
    /// Use to write the header X-ClickHouse-Exception-Code even when progress has been sent
    void writeExceptionCode();

    /// This method finish headers with \r\n, allowing to start to send body.
    void finishSendHeaders();

    void nextImpl() override;

    HTTP1ServerResponse & response;

    std::unique_ptr<WriteBufferFromPocoSocket> socket_out;

    bool is_http_method_head;

    /// FIXME: this flag looks useless
    bool initialized = false;

    bool headers_started_sending = false;
    bool headers_finished_sending = false;    /// If true, you could not add any headers.

    Progress accumulated_progress;
    Stopwatch progress_watch;

    int exception_code = 0;

    std::mutex mutex;    /// progress callback could be called from different threads.
};

class HTTP1ServerResponse : public HTTPServerResponseBase
{
public:
    explicit HTTP1ServerResponse(
        Poco::Net::HTTPServerSession & session,
        const ProfileEvents::Event & write_event = ProfileEvents::end(),
        size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    void send100Continue();

    void setResponseDefaultHeaders() override;
    void drainRequestIfNeeded() override;

    std::unique_ptr<WriteBufferFromHTTPServerResponseBase> makeUniqueStream() override;

    Poco::Net::HTTPServerSession & getSession() const noexcept { return session; }

private:
    Poco::Net::HTTPServerSession & session;
    ProfileEvents::Event write_event;
    size_t buf_size;
};

}
