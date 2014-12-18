#pragma once

#include <Poco/URI.h>
#include <Poco/SharedPtr.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPClientSession.h>

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/ReadBufferFromIStream.h>

#include <Yandex/logger_useful.h>

#define DEFAULT_HTTP_READ_BUFFER_TIMEOUT 1800
#define DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT 1


namespace DB
{

/** Делает указанный HTTP-запрос и отдает ответ.
  */
class ReadBufferFromHTTP : public ReadBuffer
{
private:
	std::string host;
	int port;

	Poco::Net::HTTPClientSession session;
	std::istream * istr;	/// этим владеет session
	Poco::SharedPtr<ReadBufferFromIStream> impl;

public:
	typedef std::vector<std::pair<String, String> > Params;

	ReadBufferFromHTTP(
		const std::string & host_,
		int port_,
		const Params & params,
		size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
		const Poco::Timespan & connection_timeout = Poco::Timespan(DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT, 0),
		const Poco::Timespan & send_timeout = Poco::Timespan(DEFAULT_HTTP_READ_BUFFER_TIMEOUT, 0),
		const Poco::Timespan & receive_timeout = Poco::Timespan(DEFAULT_HTTP_READ_BUFFER_TIMEOUT, 0))
		: ReadBuffer(nullptr, 0), host(host_), port(port_)
	{
		std::stringstream uri;
		uri << "http://" << host << ":" << port << "/";

		bool first = true;
		for (const auto & it : params)
		{
			uri << (first ? "?" : "&");
			first = false;
			String encoded_key;
			String encoded_value;
			Poco::URI::encode(it.first, "=&#", encoded_key);
			Poco::URI::encode(it.second, "&#", encoded_value);
			uri << encoded_key << "=" << encoded_value;
		}

		session.setHost(host);
		session.setPort(port);

		/// устанавливаем таймаут
		session.setTimeout(connection_timeout, send_timeout, receive_timeout);

		Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, uri.str());
		Poco::Net::HTTPResponse response;

		LOG_TRACE((&Logger::get("ReadBufferFromHTTP")), "Sending request to " << uri.str());

		session.sendRequest(request);
		istr = &session.receiveResponse(response);

		Poco::Net::HTTPResponse::HTTPStatus status = response.getStatus();

		if (status != Poco::Net::HTTPResponse::HTTP_OK)
		{
			std::stringstream error_message;
			error_message << "Received error from remote server " << uri.str() << ". HTTP status code: "
				<< status << ", body: " << istr->rdbuf();

			throw Exception(error_message.str(), ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER);
		}

		impl = new ReadBufferFromIStream(*istr, buffer_size_);
	}

	bool nextImpl()
	{
		if (!impl->next())
			return false;
		internal_buffer = impl->buffer();
		working_buffer = internal_buffer;
		return true;
	}
};

}
