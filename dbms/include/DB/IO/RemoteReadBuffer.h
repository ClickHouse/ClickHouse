#pragma once

#include <DB/IO/ReadBufferFromHTTP.h>
#include "ReadHelpers.h"


namespace DB
{

/** Позволяет читать файл с удалённого сервера через riod.
  */
class RemoteReadBuffer : public ReadBuffer
{
private:
	Poco::SharedPtr<ReadBufferFromHTTP> impl;
	
public:
	RemoteReadBuffer(
		const std::string & host,
		int port,
		const std::string & path,
		bool compress = true,
		size_t timeout = 0,
		size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE)
	{
		std::string encoded_path;
		Poco::URI::encode(path, "&#", encoded_path);
		
		std::stringstream params;
		params << "action=read&path=" << encoded_path << "&compress=" << (compress ? "true" : "false");

		impl = new ReadBufferFromHTTP(host, port, params.str, timeout, buffer_size);
	}

	bool nextImpl()
	{
		if (!impl->next())
			return false;
		internal_buffer = impl->buffer();
		working_buffer = internal_buffer;
		return true;
	}

	/// Вернуть список имен файлов в директории.
	static std::vector<std::string> listFiles(
		const std::string & host,
		int port,
		const std::string & path,
		size_t timeout = 0)
	{
		std::string encoded_path;
		Poco::URI::encode(path, "&#", encoded_path);

		std::stringstream params;
		params << "action=list&path=" << encoded_path;

		ReadBufferFromHTTP in(host, port, params.str(), timeout);

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
