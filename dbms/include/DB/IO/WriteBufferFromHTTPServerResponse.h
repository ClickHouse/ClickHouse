#pragma once

#include <experimental/optional>

#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/DeflatingStream.h>

#include <DB/Common/Exception.h>

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <DB/Common/NetException.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int CANNOT_WRITE_TO_OSTREAM;
	extern const int LOGICAL_ERROR;
}


/** Отличается от WriteBufferFromOStream тем, что инициализируется не std::ostream, а Poco::Net::HTTPServerResponse.
  * При первом сбросе данных, получает из него std::ostream (с помощью метода send).
  * Это нужно в HTTP серверах, чтобы после передачи в какой-нибудь метод WriteBuffer-а,
  *  но до вывода первых данных клиенту, можно было изменить какие-нибудь HTTP заголовки (например, код ответа).
  * (После вызова Poco::Net::HTTPServerResponse::send() изменить заголовки уже нельзя.)
  * То есть, суть в том, чтобы вызывать метод Poco::Net::HTTPServerResponse::send() не сразу.
  *
  * Дополнительно, позволяет сжимать тело HTTP-ответа, выставив соответствующий заголовок Content-Encoding.
  */
class WriteBufferFromHTTPServerResponse : public BufferWithOwnMemory<WriteBuffer>
{
private:
	Poco::Net::HTTPServerResponse & response;
	bool add_cors_header;
	bool compress;
	Poco::DeflatingStreamBuf::StreamType compression_method;
	int compression_level = Z_DEFAULT_COMPRESSION;

	std::ostream * response_ostr = nullptr;	/// Сюда записывается тело HTTP ответа, возможно, сжатое.
	std::experimental::optional<Poco::DeflatingOutputStream> deflating_stream;
	std::ostream * ostr = nullptr;	/// Куда записывать несжатое тело HTTP ответа. Указывает туда же, куда response_ostr или на deflating_stream.

	void sendHeaders()
	{
		if (!ostr)
		{
			if (add_cors_header)
			{
				response.set("Access-Control-Allow-Origin","*");
			}

			if (compress && offset())	/// Пустой ответ сжимать не нужно.
			{
				if (compression_method == Poco::DeflatingStreamBuf::STREAM_GZIP)
					response.set("Content-Encoding", "gzip");
				else if (compression_method == Poco::DeflatingStreamBuf::STREAM_ZLIB)
					response.set("Content-Encoding", "deflate");
				else
					throw Exception("Logical error: unknown compression method passed to WriteBufferFromHTTPServerResponse",
						ErrorCodes::LOGICAL_ERROR);

				response_ostr = &response.send();
				deflating_stream.emplace(*response_ostr, compression_method, compression_level);
				ostr = &deflating_stream.value();
			}
			else
			{
				response_ostr = &response.send();
				ostr = response_ostr;
			}
		}
	}

	void nextImpl() override
	{
		if (!offset())
			return;

		sendHeaders();

		ostr->write(working_buffer.begin(), offset());
		ostr->flush();

		if (!ostr->good())
			throw NetException("Cannot write to ostream", ErrorCodes::CANNOT_WRITE_TO_OSTREAM);
	}

public:
	WriteBufferFromHTTPServerResponse(
		Poco::Net::HTTPServerResponse & response_,
		bool compress_ = false,		/// Если true - выставить заголовок Content-Encoding и сжимать результат.
		Poco::DeflatingStreamBuf::StreamType compression_method_ = Poco::DeflatingStreamBuf::STREAM_GZIP,	/// Как сжимать результат (gzip, deflate).
		size_t size = DBMS_DEFAULT_BUFFER_SIZE)
		: BufferWithOwnMemory<WriteBuffer>(size), response(response_),
		compress(compress_), compression_method(compression_method_) {}

	/** Если данные ещё не были отправлены - отправить хотя бы HTTP заголовки.
	  * Используйте эту функцию после того, как данные, возможно, были отправлены,
	  *  и не было ошибок (вы не планируете поменять код ответа).
	  */
	void finalize()
	{
		sendHeaders();
	}

	/** Включить или отключить сжатие.
	  * Работает только перед тем, как были отправлены HTTP заголовки.
	  * Иначе - не имеет эффекта.
	  */
	void setCompression(bool enable_compression)
	{
		compress = enable_compression;
	}

	/** Установить уровень сжатия, если данные будут сжиматься.
	  * Работает только перед тем, как были отправлены HTTP заголовки.
	  * Иначе - не имеет эффекта.
	  */
	void setCompressionLevel(int level)
	{
		compression_level = level;
	}

	/** Включить или отключить CORS.
	  * Работает только перед тем, как были отправлены HTTP заголовки.
	  * Иначе - не имеет эффекта.
	  */
	void addHeaderCORS(bool enable_cors)
	{
		add_cors_header = enable_cors;
	}

	~WriteBufferFromHTTPServerResponse()
	{
		if (!offset())
			return;

		try
		{
			next();

			if (deflating_stream)
				deflating_stream->close();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}
	}
};

}
