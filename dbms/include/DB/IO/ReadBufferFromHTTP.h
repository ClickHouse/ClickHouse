#pragma once

#include <memory>

#include <Poco/Net/HTTPClientSession.h>

#include <DB/IO/ReadBuffer.h>
#include <DB/Core/Types.h>

#define DEFAULT_HTTP_READ_BUFFER_TIMEOUT 1800
#define DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT 1


namespace DB
{

/** Perform HTTP-request and provide response to read.
  */
class ReadBufferFromHTTP : public ReadBuffer
{
private:
	String host;
	int port;

	Poco::Net::HTTPClientSession session;
	std::istream * istr;	/// owned by session
	std::unique_ptr<ReadBuffer> impl;

public:
	using Params = std::vector<std::pair<String, String>>;

	ReadBufferFromHTTP(
		const String & host_,
		int port_,
		const Params & params,
		size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
		const Poco::Timespan & connection_timeout = Poco::Timespan(DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT, 0),
		const Poco::Timespan & send_timeout = Poco::Timespan(DEFAULT_HTTP_READ_BUFFER_TIMEOUT, 0),
		const Poco::Timespan & receive_timeout = Poco::Timespan(DEFAULT_HTTP_READ_BUFFER_TIMEOUT, 0));

	bool nextImpl() override;
};

}
