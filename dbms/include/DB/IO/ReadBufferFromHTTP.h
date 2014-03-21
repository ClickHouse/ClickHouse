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


namespace DB
{

/** Делает указанный HTTP-запрос и отдает ответ.
  */
class ReadBufferFromHTTP : public ReadBuffer
{
private:
	std::string host;
	int port;
	std::string params;

	Poco::Net::HTTPClientSession session;
	std::istream * istr;	/// этим владеет session
	Poco::SharedPtr<ReadBufferFromIStream> impl;

public:
	ReadBufferFromHTTP(
		const std::string & host_,
		int port_,
		const std::string & params_,
		size_t timeout_ = 0,
		size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE)
		: ReadBuffer(NULL, 0), host(host_), port(port_), params(params_)
	{
		std::string encoded_path;
		Poco::URI::encode(path, "&#", encoded_path);

		std::stringstream uri;
		uri << "http://" << host << ":" << port << "/?" << params;

		session.setHost(host);
		session.setPort(port);

		/// устанавливаем таймаут
		session.setTimeout(Poco::Timespan(timeout_ ? timeout_ : DEFAULT_REMOTE_READ_BUFFER_TIMEOUT, 0));

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
