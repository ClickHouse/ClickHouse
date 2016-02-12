#include <iomanip>
#include <Poco/InflatingStream.h>

#include <Poco/Net/HTTPBasicCredentials.h>

#include <DB/Common/Stopwatch.h>

#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/ConcatReadBuffer.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>

#include <DB/Interpreters/executeQuery.h>
#include <DB/Interpreters/Quota.h>

#include <DB/Common/ExternalTable.h>

#include "HTTPHandler.h"



namespace DB
{

namespace ErrorCodes
{
	extern const int READONLY;
}


void HTTPHandler::processQuery(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response, Output & used_output)
{
	LOG_TRACE(log, "Request URI: " << request.getURI());

	HTMLForm params(request);
	std::istream & istr = request.stream();

	/// Если метод GET, то это эквивалентно выставлению настройки readonly.
	bool readonly = request.getMethod() == Poco::Net::HTTPServerRequest::HTTP_GET;

	BlockInputStreamPtr query_plan;

	/** Часть запроса может быть передана в параметре query, а часть - POST-ом
	  *  (точнее - в теле запроса, а метод не обязательно должен быть POST).
	  * В таком случае, считается, что запрос - параметр query, затем перевод строки, а затем - данные POST-а.
	  */
	std::string query_param = params.get("query", "");
	if (!query_param.empty())
		query_param += '\n';

	/** Клиент может указать поддерживаемый метод сжатия (gzip или deflate) в HTTP-заголовке.
	  */
	String http_response_compression_methods = request.get("Accept-Encoding", "");
	bool http_response_compress = false;
	Poco::DeflatingStreamBuf::StreamType http_response_compression_method {};

	if (!http_response_compression_methods.empty())
	{
		/// Мы поддерживаем gzip или deflate. Если клиент поддерживает оба, то предпочитается gzip.
		/// NOTE Парсинг списка методов слегка некорректный.

		if (std::string::npos != http_response_compression_methods.find("gzip"))
		{
			http_response_compress = true;
			http_response_compression_method = Poco::DeflatingStreamBuf::STREAM_GZIP;
		}
		else if (std::string::npos != http_response_compression_methods.find("deflate"))
		{
			http_response_compress = true;
			http_response_compression_method = Poco::DeflatingStreamBuf::STREAM_ZLIB;
		}
	}

	used_output.out = new WriteBufferFromHTTPServerResponse(response, http_response_compress, http_response_compression_method);

	/** Клиент может указать compress в query string.
	  * В этом случае, результат сжимается несовместимым алгоритмом для внутреннего использования и этот факт не отражается в HTTP заголовках.
	  */
	if (parse<bool>(params.get("compress", "0")))
		used_output.out_maybe_compressed = new CompressedWriteBuffer(*used_output.out);
	else
		used_output.out_maybe_compressed = used_output.out;

	/// Имя пользователя и пароль могут быть заданы как в параметрах URL, так и с помощью HTTP Basic authentification (и то, и другое не секъюрно).
	std::string user = params.get("user", "default");
	std::string password = params.get("password", "");

	if (request.hasCredentials())
	{
		Poco::Net::HTTPBasicCredentials credentials(request);

		user = credentials.getUsername();
		password = credentials.getPassword();
	}

	std::string quota_key = params.get("quota_key", "");
	std::string query_id = params.get("query_id", "");

	Context context = *server.global_context;
	context.setGlobalContext(*server.global_context);

	context.setUser(user, password, request.clientAddress().host(), quota_key);
	context.setCurrentQueryId(query_id);

	SharedPtr<ReadBuffer> in_param = new ReadBufferFromString(query_param);

	/// Данные POST-а могут быть сжаты алгоритмом, указанным в Content-Encoding заголовке.
	String http_request_compression_method_str = request.get("Content-Encoding", "");
	bool http_request_decompress = false;
	Poco::InflatingStreamBuf::StreamType http_request_compression_method {};

	if (!http_request_compression_method_str.empty())
	{
		if (http_request_compression_method_str == "gzip")
		{
			http_request_decompress = true;
			http_request_compression_method = Poco::InflatingStreamBuf::STREAM_GZIP;
		}
		else if (http_request_compression_method_str == "deflate")
		{
			http_request_decompress = true;
			http_request_compression_method = Poco::InflatingStreamBuf::STREAM_ZLIB;
		}
		else
			throw Exception("Unknown Content-Encoding of HTTP request: " + http_request_compression_method_str,
				ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
	}

	std::experimental::optional<Poco::InflatingInputStream> decompressing_stream;
	SharedPtr<ReadBuffer> in_post;

	if (http_request_decompress)
	{
		decompressing_stream.emplace(istr, http_request_compression_method);
		in_post = new ReadBufferFromIStream(decompressing_stream.value());
	}
	else
		in_post = new ReadBufferFromIStream(istr);

	/// Также данные могут быть сжаты несовместимым алгоритмом для внутреннего использования - это определяется параметром query_string.
	SharedPtr<ReadBuffer> in_post_maybe_compressed;

	if (parse<bool>(params.get("decompress", "0")))
		in_post_maybe_compressed = new CompressedReadBuffer(*in_post);
	else
		in_post_maybe_compressed = in_post;

	SharedPtr<ReadBuffer> in;

	/// Поддержка "внешних данных для обработки запроса".
	if (0 == strncmp(request.getContentType().data(), "multipart/form-data", strlen("multipart/form-data")))
	{
		in = in_param;
		ExternalTablesHandler handler(context, params);

		params.load(request, istr, handler);

		/// Удаляем уже нененужные параметры из хранилища, чтобы впоследствии не перепутать их с наcтройками контекста и параметрами запроса.
		for (const auto & it : handler.names)
		{
			params.erase(it + "_format");
			params.erase(it + "_types");
			params.erase(it + "_structure");
		}
	}
	else
		in = new ConcatReadBuffer(*in_param, *in_post_maybe_compressed);

	/// Настройки могут быть переопределены в запросе.
	for (Poco::Net::NameValueCollection::ConstIterator it = params.begin(); it != params.end(); ++it)
	{
		if (it->first == "database")
		{
			context.setCurrentDatabase(it->second);
		}
		else if (it->first == "default_format")
		{
			context.setDefaultFormat(it->second);
		}
		else if (readonly && it->first == "readonly")
		{
			throw Exception("Setting 'readonly' cannot be overrided in readonly mode", ErrorCodes::READONLY);
		}
		else if (it->first == "query"
			|| it->first == "compress"
			|| it->first == "decompress"
			|| it->first == "user"
			|| it->first == "password"
			|| it->first == "quota_key"
			|| it->first == "query_id")
		{
		}
		else	/// Все неизвестные параметры запроса рассматриваются, как настройки.
			context.setSetting(it->first, it->second);
	}

	if (readonly)
		context.getSettingsRef().limits.readonly = true;

	context.setInterface(Context::Interface::HTTP);

	Context::HTTPMethod http_method = Context::HTTPMethod::UNKNOWN;
	if (request.getMethod() == Poco::Net::HTTPServerRequest::HTTP_GET)
		http_method = Context::HTTPMethod::GET;
	else if (request.getMethod() == Poco::Net::HTTPServerRequest::HTTP_POST)
		http_method = Context::HTTPMethod::POST;

	context.setHTTPMethod(http_method);

	executeQuery(*in, *used_output.out_maybe_compressed, context, query_plan,
		[&response] (const String & content_type) { response.setContentType(content_type); });

	/// Если не было эксепшена и данные ещё не отправлены - отправляются HTTP заголовки с кодом 200.
	used_output.out->finalize();
}


void HTTPHandler::trySendExceptionToClient(const std::string & s,
	Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response,
	Output & used_output)
{
	try
	{
		/** Если POST и Keep-Alive, прочитаем тело до конца.
		  * Иначе вместо следующего запроса, будет прочитан кусок этого тела.
		  */
		if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST
			&& response.getKeepAlive()
			&& !request.stream().eof())
		{
			request.stream().ignore(std::numeric_limits<std::streamsize>::max());
		}

		response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);

		if (!response.sent() && !used_output.out_maybe_compressed)
		{
			/// Ещё ничего не отправляли, и даже не знаем, нужно ли сжимать ответ.
			response.send() << s << std::endl;
		}
		else if (used_output.out_maybe_compressed)
		{
			/** Отправим в использованный (возможно сжатый) поток сообщение об ошибке.
			  * Сообщение об ошибке может идти невпопад - после каких-то данных.
			  * Также стоит иметь ввиду, что мы могли уже отправить код 200.
			  */

			/** Если данные есть в буфере, но их ещё не отправили, то и не будем отправлять */
			if (used_output.out->count() - used_output.out->offset() == 0)
			{
				used_output.out_maybe_compressed->position() = used_output.out_maybe_compressed->buffer().begin();
				used_output.out->position() = used_output.out->buffer().begin();
			}

			writeString(s, *used_output.out_maybe_compressed);
			writeChar('\n', *used_output.out_maybe_compressed);
			used_output.out_maybe_compressed->next();
			used_output.out->finalize();
		}
	}
	catch (...)
	{
		LOG_ERROR(log, "Cannot send exception to client");
	}
}


void HTTPHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
	Output used_output;

	try
	{
		response.setContentType("text/plain; charset=UTF-8");

		/// Для того, чтобы работал keep-alive.
		if (request.getVersion() == Poco::Net::HTTPServerRequest::HTTP_1_1)
			response.setChunkedTransferEncoding(true);

		processQuery(request, response, used_output);
		LOG_INFO(log, "Done processing query");
	}
	catch (...)
	{
		tryLogCurrentException(log);
		trySendExceptionToClient(getCurrentExceptionMessage(true), request, response, used_output);
	}
}


}
