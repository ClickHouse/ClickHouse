#pragma once

#include <Common/config.h>

#include <string>
#include <memory>

#include <Poco/File.h>
#include <Poco/URI.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <fmt/core.h>

#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>

namespace DB
{
class AlluxioClient
{
public:
    using HTTPRequest = Poco::Net::HTTPRequest;
    using HTTPClientSession = Poco::Net::HTTPClientSession;
    using HTTPResponse = Poco::Net::HTTPResponse;
    using HTTPMessage = Poco::Net::HTTPMessage;

    AlluxioClient(const std::string & host_, int port_) : host(host_), port(port_), session(host, port) { session.setKeepAlive(true); }

    ~AlluxioClient() = default;

    inline int openFile(const std::string & file)
    {
        std::string path = fmt::format(API_OPEN_FILE, file);
        std::string body = R"({"readType": "CACHE"})";
        HTTPRequest request(HTTPRequest::HTTP_POST, path, HTTPMessage::HTTP_1_1);
        request.setContentType("application/json");
        request.setContentLength(body.size());
        session.sendRequest(request) << body;

        HTTPResponse response;
        std::istream & s = session.receiveResponse(response);
        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        oss << s.rdbuf();
        std::string res = oss.str();
        // std::cout << "res:" << res << std::endl;
        return std::stoi(res);
    }

    inline void close(int id)
    {
        std::string path = fmt::format(API_CLOSE_STREAM, id);

        HTTPRequest request(HTTPRequest::HTTP_POST, path, HTTPMessage::HTTP_1_1);
        session.sendRequest(request);
    }

    inline std::istream * read(int id)
    {
        std::string path = fmt::format(API_READ_STREAM, id);
        HTTPRequest request(HTTPRequest::HTTP_POST, path, HTTPMessage::HTTP_1_1);
        session.sendRequest(request);

        HTTPResponse response;
        std::istream & res = session.receiveResponse(response);
        return &res;
    }


private:
    std::string host;
    uint16_t port;
    HTTPClientSession session;

    inline static const std::string API_OPEN_FILE{"/api/v1/paths/{}/open-file"};
    inline static const std::string API_READ_STREAM{"/api/v1/streams/{}/read"};
    inline static const std::string API_CLOSE_STREAM{"/api/v1/streams/{}/close"};
};

/** Accepts Alluxio path to file and opens it.
 * Closes file by himself (thus "owns" a file descriptor).
 */
class ReadBufferFromAlluxio : public BufferWithOwnMemory<ReadBuffer>
{
public:
    explicit ReadBufferFromAlluxio(const String & host_, int port_, const String & file_, size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE);
    ~ReadBufferFromAlluxio() override;

    bool nextImpl() override;

private:
    std::shared_ptr<AlluxioClient> client;
    String file;
    int id;
    std::istream * is;
};
}
