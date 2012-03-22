#pragma once

#include <Poco/URI.h>
#include <Poco/SharedPtr.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/NetException.h>

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
	std::string encoded_path;
	std::string encoded_tmp_path;
	std::string tmp_path;
	std::string if_exists;
	bool decompress;
	unsigned connection_retries;

	std::string uri_str;

	Poco::Net::HTTPClientSession session;
	std::ostream * ostr;	/// этим владеет session
	Poco::SharedPtr<WriteBufferFromOStream> impl;

	/// Отправили все данные и переименовали файл
	bool finalized;
	
public:
	/** Если tmp_path не пустой, то записывает сначала временный файл, а затем переименовывает,
	  *  удаляя существующие файлы, если есть.
	  * Иначе используется параметр if_exists.
	  */
	RemoteWriteBuffer(const std::string & host_, int port_, const std::string & path_,
		const std::string & tmp_path_ = "", const std::string & if_exists_ = "remove",
		bool decompress_ = false, size_t timeout_ = 0, unsigned connection_retries_ = 3,
		size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE)
		: WriteBuffer(NULL, 0), host(host_), port(port_), path(path_),
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

		uri_str = uri.str();

		session.setHost(host);
		session.setPort(port);
		
		/// устанавливаем таймаут
		session.setTimeout(Poco::Timespan(timeout_ || DEFAULT_REMOTE_WRITE_BUFFER_TIMEOUT, 0));
		
		Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, uri_str);
		
		for (unsigned i = 0; i < connection_retries; ++i)
		{
			LOG_TRACE((&Logger::get("RemoteWriteBuffer")), "Sending write request to " << uri_str);

			try
			{
				ostr = &session.sendRequest(request);
			}
			catch (const Poco::Net::NetException & e)
			{
				if (i + 1 == connection_retries)
					throw;

				LOG_WARNING((&Logger::get("RemoteWriteBuffer")), e.message() << ", URL: " << uri_str << ", try No " << i + 1 << ".");
				session.reset();
				continue;
			}
			catch (const Poco::TimeoutException & e)
			{
				if (i + 1 == connection_retries)
					throw;

				LOG_WARNING((&Logger::get("RemoteWriteBuffer")), "Connection timeout from " << uri_str << ", try No " << i + 1 << ".");
				session.reset();
				continue;
			}

			break;
		}

		impl = new WriteBufferFromOStream(*ostr, buffer_size_);
		set(impl->buffer().begin(), impl->buffer().size());
	}

	void nextImpl()
	{
		if (!offset())
			return;

		/// Для корректной работы с AsynchronousWriteBuffer, который подменяет буферы.
		impl->set(buffer().begin(), buffer().size());
		
		impl->position() = pos;

		try
		{
			impl->next();
		}
		catch (const DB::Exception & e)
		{
			if (e.code() == ErrorCodes::CANNOT_WRITE_TO_OSTREAM)
				checkStatus();	/// Меняем сообщение об ошибке на более ясное.
			throw;
		}
	}

	void finalize()
	{
		if (finalized)
			return;
		
		next();
		checkStatus();

		/// Переименовываем файл, если нужно.
		if (!tmp_path.empty())
			rename();

		finalized = true;
	}

	~RemoteWriteBuffer()
	{
		/// Если объект уничтожается из-за эксепшена - то не отправляем последние данные (если они ещё не были отправлены).
		if (!std::uncaught_exception())
			finalize();
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

		uri_str = uri.str();
		Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, uri_str);

		for (unsigned i = 0; i < connection_retries; ++i)
		{
			LOG_TRACE((&Logger::get("RemoteWriteBuffer")), "Sending rename request to " << uri_str);

			try
			{
				session.sendRequest(request);
				checkStatus();
			}
			catch (const Poco::Net::NetException & e)
			{
				if (i + 1 == connection_retries)
					throw;
				
				LOG_WARNING((&Logger::get("RemoteWriteBuffer")), e.what() << ", message: " << e.message()
					<< ", URL: " << uri_str << ", try No " << i + 1 << ".");
				session.reset();
				continue;
			}
			catch (const Poco::TimeoutException & e)
			{
				if (i + 1 == connection_retries)
					throw;
				
				LOG_WARNING((&Logger::get("RemoteWriteBuffer")), "Connection timeout from " << uri_str << ", try No " << i + 1 << ".");
				session.reset();
				continue;
			}
			catch (const DB::Exception & e)
			{
				/// Если в прошлую попытку от сервера не пришло ответа, но файл всё же был переименован.
				if (NULL != strstr(e.what(), "File not found"))
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
