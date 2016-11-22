#pragma once

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <DB/IO/ReadBufferFromHTTP.h>
#include <DB/IO/ReadBuffer.h>

namespace DB
{

struct HTTPLocation {
	using Params = std::vector<std::pair<String, String>>;
	std::string protocol = "http";
	// user
	// password
	std::string host = "::1";
	unsigned short port = 80;
	std::string path = "/";
	Params params;
	//std::string query = "";
	std::string method = Poco::Net::HTTPRequest::HTTP_GET;
};

struct HTTPTimeouts {
	Poco::Timespan connection_timeout = Poco::Timespan(DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT, 0);
	Poco::Timespan send_timeout = Poco::Timespan(DEFAULT_HTTP_READ_BUFFER_TIMEOUT, 0);
	Poco::Timespan receive_timeout = Poco::Timespan(DEFAULT_HTTP_READ_BUFFER_TIMEOUT, 0);
};

/** Perform HTTP POST request and provide response to read.
  */
class ReadWriteBufferFromHTTP : public ReadBuffer
{
private:
	HTTPLocation location;
	HTTPTimeouts timeouts;

	Poco::Net::HTTPClientSession session;
	std::istream * istr;	/// owned by session
	std::unique_ptr<ReadBuffer> impl;

public:
	using OutStreamCallback = std::function<void(std::ostream&)>;
	//using Params = std::vector<std::pair<String, String>>;

	ReadWriteBufferFromHTTP(
		HTTPLocation http_query,
		OutStreamCallback out_stream_callback = {},
		size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
		HTTPTimeouts timeouts = HTTPTimeouts()
	);

	bool nextImpl() override;

};

}
