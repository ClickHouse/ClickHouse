#pragma once

#include <Poco/URI.h>
#include <Poco/SharedPtr.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPClientSession.h>

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/ReadBufferFromIStream.h>

#include <Yandex/logger_useful.h>

#define DEFAULT_REMOTE_READ_BUFFER_TIMEOUT 1800


namespace DB
{

/** Позволяет читать файл с удалённого сервера.
  */
class RemoteReadBuffer : public ReadBuffer
{
private:
	std::string host;
	int port;
	std::string path;
	bool compress;

	Poco::Net::HTTPClientSession session;
	std::istream * istr;	/// этим владеет session
	Poco::SharedPtr<ReadBufferFromIStream> impl;
	
public:
	RemoteReadBuffer(const std::string & host_, int port_, const std::string & path_, bool compress_ = true, size_t timeout_ = 0)
		: ReadBuffer(NULL, 0), host(host_), port(port_), path(path_), compress(compress_)
	{
		std::string encoded_path;
		Poco::URI::encode(path, "&#", encoded_path);
		
		std::stringstream uri;
		uri << "http://" << host << ":" << port << "/?action=read&path=" << encoded_path << "&compress=" << (compress ? "true" : "false");

		session.setHost(host);
		session.setPort(port);
		
		/// устанавливаем таймаут
		session.setTimeout(Poco::Timespan(timeout_ || DEFAULT_REMOTE_READ_BUFFER_TIMEOUT, 0));
		
		Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, uri.str());
		Poco::Net::HTTPResponse response;

		LOG_TRACE((&Logger::get("RemoteReadBuffer")), "Sending request to " << uri.str());

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

		impl = new ReadBufferFromIStream(*istr);
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
