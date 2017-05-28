#pragma once

#include <IO/ReadWriteBufferFromHTTP.h>
#include "ReadHelpers.h"

#define DEFAULT_REMOTE_READ_BUFFER_CONNECTION_TIMEOUT 1
#define DEFAULT_REMOTE_READ_BUFFER_RECEIVE_TIMEOUT 1800
#define DEFAULT_REMOTE_READ_BUFFER_SEND_TIMEOUT 1800

namespace DB
{

/** Allows you to read a file from a remote server via riod.
  */
class RemoteReadBuffer : public ReadBuffer
{
private:
    std::unique_ptr<ReadWriteBufferFromHTTP> impl;

public:
    RemoteReadBuffer(
        const std::string & host,
        int port,
        const std::string & path,
        bool compress = true,
        size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE,
        const Poco::Timespan & connection_timeout = Poco::Timespan(DEFAULT_REMOTE_READ_BUFFER_CONNECTION_TIMEOUT, 0),
        const Poco::Timespan & send_timeout = Poco::Timespan(DEFAULT_REMOTE_READ_BUFFER_SEND_TIMEOUT, 0),
        const Poco::Timespan & receive_timeout = Poco::Timespan(DEFAULT_REMOTE_READ_BUFFER_RECEIVE_TIMEOUT, 0))
        : ReadBuffer(nullptr, 0)
    {
        Poco::URI uri;
        uri.setScheme("http");
        uri.setHost(host);
        uri.setPort(port);
        uri.setQueryParameters(
        {
            std::make_pair("action", "read"),
            std::make_pair("path", path),
            std::make_pair("compress", (compress ? "true" : "false"))});

        impl = std::make_unique<ReadWriteBufferFromHTTP>(uri, std::string(), ReadWriteBufferFromHTTP::OutStreamCallback(), buffer_size, HTTPTimeouts{connection_timeout, send_timeout, receive_timeout});
    }

    bool nextImpl() override
    {
        if (!impl->next())
            return false;
        internal_buffer = impl->buffer();
        working_buffer = internal_buffer;
        return true;
    }

    /// Return the list of file names in the directory.
    static std::vector<std::string> listFiles(
        const std::string & host,
        int port,
        const std::string & path,
        size_t timeout = 0)
    {
        Poco::URI uri;
        uri.setScheme("http");
        uri.setHost(host);
        uri.setPort(port);
        uri.setQueryParameters(
        {
            std::make_pair("action", "list"),
            std::make_pair("path", path)});

        ReadWriteBufferFromHTTP in(uri, {}, {}, {}, HTTPTimeouts{timeout});

        std::vector<std::string> files;
        while (!in.eof())
        {
            std::string s;
            readString(s, in);
            skipWhitespaceIfAny(in);
            files.push_back(s);
        }

        return files;
    }
};

}
