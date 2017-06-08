#pragma once

#include <Poco/Version.h>
#include <Poco/URI.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/NetException.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/HashingWriteBuffer.h>

#include <common/logger_useful.h>

#define DEFAULT_REMOTE_WRITE_BUFFER_CONNECTION_TIMEOUT 1
#define DEFAULT_REMOTE_WRITE_BUFFER_RECEIVE_TIMEOUT 1800
#define DEFAULT_REMOTE_WRITE_BUFFER_SEND_TIMEOUT 1800


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_TO_OSTREAM;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
}

/** Allows you to write a file to a remote server.
  */
class RemoteWriteBuffer : public WriteBuffer
{
private:
    std::string host;
    int port;
    std::string path;
    std::string encoded_path;
    std::string encoded_tmp_path;
    std::string tmp_path;
    std::string if_exists;
    bool decompress;
    unsigned connection_retries;

    std::string uri_str;

    Poco::Net::HTTPClientSession session;
    std::ostream * ostr;    /// this is owned by session
    std::unique_ptr<WriteBuffer> impl;

    /// Have sent all the data and renamed the file
    bool finalized;
public:
    /** If tmp_path is not empty, it writes first the temporary file, and then renames it,
      *  deleting existing files, if any.
      * Otherwise, if_exists parameter is used.
      */
    RemoteWriteBuffer(const std::string & host_, int port_, const std::string & path_,
        const std::string & tmp_path_ = "", const std::string & if_exists_ = "remove",
        bool decompress_ = false,
        unsigned connection_retries_ = 3,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        const Poco::Timespan & connection_timeout = Poco::Timespan(DEFAULT_REMOTE_WRITE_BUFFER_CONNECTION_TIMEOUT, 0),
        const Poco::Timespan & send_timeout = Poco::Timespan(DEFAULT_REMOTE_WRITE_BUFFER_SEND_TIMEOUT, 0),
        const Poco::Timespan & receive_timeout = Poco::Timespan(DEFAULT_REMOTE_WRITE_BUFFER_RECEIVE_TIMEOUT, 0))
        : WriteBuffer(nullptr, 0), host(host_), port(port_), path(path_),
        tmp_path(tmp_path_), if_exists(if_exists_),
        decompress(decompress_), connection_retries(connection_retries_), finalized(false)
    {
        Poco::URI::encode(path, "&#", encoded_path);
        Poco::URI::encode(tmp_path, "&#", encoded_tmp_path);

        std::stringstream uri;
        uri << "http://" << host << ":" << port
            << "/?action=write"
            << "&path=" << (tmp_path.empty() ? encoded_path : encoded_tmp_path)
            << "&if_exists=" << if_exists
            << "&decompress=" << (decompress ? "true" : "false");

        uri_str = Poco::URI(uri.str()).getPathAndQuery();

        session.setHost(host);
        session.setPort(port);
        session.setKeepAlive(true);

        /// set the timeout
#if POCO_CLICKHOUSE_PATCH || POCO_VERSION >= 0x02000000
        session.setTimeout(connection_timeout, send_timeout, receive_timeout);
#else
        session.setTimeout(connection_timeout);
#endif

        Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, uri_str, Poco::Net::HTTPRequest::HTTP_1_1);

        request.setChunkedTransferEncoding(true);

        for (unsigned i = 0; i < connection_retries; ++i)
        {
            LOG_TRACE((&Logger::get("RemoteWriteBuffer")), "Sending write request to " << host << ":" << port << uri_str);

            try
            {
                ostr = &session.sendRequest(request);
            }
            catch (const Poco::Net::NetException & e)
            {
                if (i + 1 == connection_retries)
                    throw;

                LOG_WARNING((&Logger::get("RemoteWriteBuffer")), e.displayText() << ", URL: " << host << ":" << port << uri_str << ", try No " << i + 1 << ".");
                session.reset();
                continue;
            }
            catch (const Poco::TimeoutException & e)
            {
                if (i + 1 == connection_retries)
                    throw;

                LOG_WARNING((&Logger::get("RemoteWriteBuffer")), "Connection timeout from " << host << ":" << port << uri_str << ", try No " << i + 1 << ".");
                session.reset();
                continue;
            }

            break;
        }

        impl = std::make_unique<WriteBufferFromOStream>(*ostr, buffer_size_);

        set(impl->buffer().begin(), impl->buffer().size());
    }

    void nextImpl() override
    {
        if (!offset() || finalized)
            return;

        /// For correct work with AsynchronousWriteBuffer, which replaces buffers.
        impl->set(buffer().begin(), buffer().size());

        impl->position() = pos;

        try
        {
            impl->next();
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::CANNOT_WRITE_TO_OSTREAM)
                checkStatus();    /// Change the error message to a clearer one.
            throw;
        }
    }

    void finalize()
    {
        if (finalized)
            return;

        next();
        checkStatus();

        /// Rename the file if necessary.
        if (!tmp_path.empty())
            rename();

        finalized = true;
    }

    void cancel()
    {
        finalized = true;
    }

    ~RemoteWriteBuffer()
    {
        try
        {
            finalize();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }


private:

    void checkStatus()
    {
        Poco::Net::HTTPResponse response;
        std::istream & istr = session.receiveResponse(response);
        Poco::Net::HTTPResponse::HTTPStatus status = response.getStatus();

        std::stringstream message;
        message << istr.rdbuf();

        if (status != Poco::Net::HTTPResponse::HTTP_OK || message.str() != "Ok.\n")
        {
            std::stringstream error_message;
            error_message << "Received error from remote server " << uri_str << ", body: " << message.str();

            throw Exception(error_message.str(), ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER);
        }
    }

    void rename()
    {
        std::stringstream uri;
        uri << "http://" << host << ":" << port
            << "/?action=rename"
            << "&from=" << encoded_tmp_path
            << "&to=" << encoded_path;

        uri_str = Poco::URI(uri.str()).getPathAndQuery();

        Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, uri_str, Poco::Net::HTTPRequest::HTTP_1_1);

        for (unsigned i = 0; i < connection_retries; ++i)
        {
            LOG_TRACE((&Logger::get("RemoteWriteBuffer")), "Sending rename request to " << host << ":" << port << uri_str);

            try
            {
                session.sendRequest(request);
                checkStatus();
            }
            catch (const Poco::Net::NetException & e)
            {
                if (i + 1 == connection_retries)
                    throw;

                LOG_WARNING((&Logger::get("RemoteWriteBuffer")), e.what() << ", message: " << e.displayText()
                    << ", URL: " << host << ":" << port << uri_str << ", try No " << i + 1 << ".");
                session.reset();
                continue;
            }
            catch (const Poco::TimeoutException & e)
            {
                if (i + 1 == connection_retries)
                    throw;

                LOG_WARNING((&Logger::get("RemoteWriteBuffer")), "Connection timeout from " << host << ":" << port << uri_str << ", try No " << i + 1 << ".");
                session.reset();
                continue;
            }
            catch (const Exception & e)
            {
                /// If in the last attempt we did not receive a response from the server, but the file was renamed already.
                if (i != 0 && e.code() == ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER
                    && nullptr != strstr(e.displayText().data(), "File not found"))
                {
                    LOG_TRACE((&Logger::get("RemoteWriteBuffer")), "File already renamed");
                }
                else
                    throw;
            }

            break;
        }
    }
};

}
