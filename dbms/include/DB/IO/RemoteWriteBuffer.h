#pragma once

#include <Poco/URI.h>
#include <Poco/SharedPtr.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPClientSession.h>

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteBufferFromOStream.h>

#include <Yandex/logger_useful.h>

#define DEFAULT_REMOTE_WRITE_BUFFER_TIMEOUT 1800


namespace DB
{

/** Позволяет писать файл на удалённый сервер.
  */
class RemoteWriteBuffer : public WriteBuffer
{
private:
	std::string host;
	int port;
	std::string path;
	std::string tmp_path;
	std::string if_exists;
	bool decompress;

	std::string uri_str;

	Poco::Net::HTTPClientSession session;
	std::ostream * ostr;	/// этим владеет session
	Poco::SharedPtr<WriteBufferFromOStream> impl;
	
public:
	/** Если tmp_path не пустой, то записывает сначала временный файл, а затем переименовывает,
	  *  удаляя существующие файлы, если есть.
	  * Иначе используется параметр if_exists.
	  */
	RemoteWriteBuffer(const std::string & host_, int port_, const std::string & path_,
		const std::string & tmp_path_ = "", const std::string & if_exists_ = "append",
		bool decompress_ = false, size_t timeout_ = 0)
		: WriteBuffer(NULL, 0), host(host_), port(port_), path(path_),
		tmp_path(tmp_path_), if_exists(if_exists_),
		decompress(decompress_)
	{
		std::string encoded_path;
		Poco::URI::encode(path, "&#", encoded_path);
		std::string encoded_tmp_path;
		Poco::URI::encode(tmp_path, "&#", encoded_tmp_path);
		
		std::stringstream uri;
		uri << "http://" << host << ":" << port
			<< "/?action=" << (tmp_path.empty() ? "write" : "create_and_write")
			<< "&path=" << encoded_path
			<< "&tmp_path=" << encoded_tmp_path
			<< "&if_exists=" << if_exists
			<< "&decompress=" << (decompress ? "true" : "false");

		uri_str = uri.str();

		session.setHost(host);
		session.setPort(port);
		
		/// устанавливаем таймаут
		session.setTimeout(Poco::Timespan(timeout_ || DEFAULT_REMOTE_WRITE_BUFFER_TIMEOUT, 0));
		
		Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, uri_str);
		
		LOG_TRACE((&Logger::get("RemoteReadBuffer")), "Sending request to " << uri_str);

		ostr = &session.sendRequest(request);
		impl = new WriteBufferFromOStream(*ostr);

		set(impl->buffer().begin(), impl->buffer().size());
	}

	void nextImpl()
	{
		if (!offset())
			return;

		impl->position() = pos;
		impl->next();
	}

	virtual ~RemoteWriteBuffer()
	{
		bool uncaught_exception = std::uncaught_exception();

		try
		{
			next();

			Poco::Net::HTTPResponse response;
			std::istream & istr = session.receiveResponse(response);
			Poco::Net::HTTPResponse::HTTPStatus status = response.getStatus();

			if (status != Poco::Net::HTTPResponse::HTTP_OK)
			{
				std::stringstream error_message;
				error_message << "Received error from remote server " << uri_str << ". HTTP status code: "
					<< status << ", body: " << istr.rdbuf();

				throw Exception(error_message.str(), ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER);
			}
		}
		catch (...)
		{
			/// Если до этого уже было какое-то исключение, то второе исключение проигнорируем.
			if (!uncaught_exception)
				throw;
		}
	}
};

}
