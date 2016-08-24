#include <iomanip>

#include <Poco/InflatingStream.h>

#include <Poco/Net/HTTPBasicCredentials.h>

#include <DB/Common/Stopwatch.h>
#include <DB/Common/StringUtils.h>

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
	extern const int UNKNOWN_COMPRESSION_METHOD;
}


void HTTPHandler::processQuery(
	Poco::Net::HTTPServerRequest & request,
	HTMLForm & params,
	Poco::Net::HTTPServerResponse & response,
	Output & used_output)
{
	LOG_TRACE(log, "Request URI: " << request.getURI());

	std::istream & istr = request.stream();

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
	bool client_supports_http_compression = false;
	Poco::DeflatingStreamBuf::StreamType http_response_compression_method {};

	if (!http_response_compression_methods.empty())
	{
		/// Мы поддерживаем gzip или deflate. Если клиент поддерживает оба, то предпочитается gzip.
		/// NOTE Парсинг списка методов слегка некорректный.

		if (std::string::npos != http_response_compression_methods.find("gzip"))
		{
			client_supports_http_compression = true;
			http_response_compression_method = Poco::DeflatingStreamBuf::STREAM_GZIP;
		}
		else if (std::string::npos != http_response_compression_methods.find("deflate"))
		{
			client_supports_http_compression = true;
			http_response_compression_method = Poco::DeflatingStreamBuf::STREAM_ZLIB;
		}
	}

	used_output.out = std::make_shared<WriteBufferFromHTTPServerResponse>(
		response, client_supports_http_compression, http_response_compression_method);

	/** Клиент может указать compress в query string.
	  * В этом случае, результат сжимается несовместимым алгоритмом для внутреннего использования и этот факт не отражается в HTTP заголовках.
	  */
	if (parse<bool>(params.get("compress", "0")))
		used_output.out_maybe_compressed = std::make_shared<CompressedWriteBuffer>(*used_output.out);
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

	std::unique_ptr<ReadBuffer> in_param = std::make_unique<ReadBufferFromString>(query_param);

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
	std::unique_ptr<ReadBuffer> in_post;

	if (http_request_decompress)
	{
		decompressing_stream.emplace(istr, http_request_compression_method);
		in_post = std::make_unique<ReadBufferFromIStream>(decompressing_stream.value());
	}
	else
		in_post = std::make_unique<ReadBufferFromIStream>(istr);

	/// Также данные могут быть сжаты несовместимым алгоритмом для внутреннего использования - это определяется параметром query_string.
	std::unique_ptr<ReadBuffer> in_post_maybe_compressed;
	bool in_post_compressed = false;

	if (parse<bool>(params.get("decompress", "0")))
	{
		in_post_maybe_compressed = std::make_unique<CompressedReadBuffer>(*in_post);
		in_post_compressed = true;
	}
	else
		in_post_maybe_compressed = std::move(in_post);

	std::unique_ptr<ReadBuffer> in;

	/// Поддержка "внешних данных для обработки запроса".
	if (startsWith(request.getContentType().data(), "multipart/form-data"))
	{
		in = std::move(in_param);
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
		in = std::make_unique<ConcatReadBuffer>(*in_param, *in_post_maybe_compressed);

	/** Настройки могут быть переопределены в запросе.
	  * Некоторые параметры (database, default_format, и все что использовались выше),
	  *  не относятся к обычным настройкам (Settings).
	  *
	  * Среди настроек есть также readonly.
	  * readonly = 0 - можно выполнять любые запросы и изменять любые настройки
	  * readonly = 1 - можно выполнять только запросы на чтение, нельзя изменять настройки
	  * readonly = 2 - можно выполнять только запросы на чтение, можно изменять настройки кроме настройки readonly
	  *
	  * Заметим, что в запросе, если до этого readonly было равно 0,
	  *  пользователь может изменить любые настройки и одновременно выставить readonly в другое значение.
	  */
	auto & limits = context.getSettingsRef().limits;

	/// Если метод GET, то это эквивалентно настройке readonly, выставленной в ненулевое значение.
	if (request.getMethod() == Poco::Net::HTTPServerRequest::HTTP_GET)
	{
		if (limits.readonly == 0)
			limits.readonly = 2;
	}

	auto readonly_before_query = limits.readonly;

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
		else if (it->first == "query"
			|| it->first == "compress"
			|| it->first == "decompress"
			|| it->first == "user"
			|| it->first == "password"
			|| it->first == "quota_key"
			|| it->first == "query_id"
			|| it->first == "stacktrace")
		{
		}
		else
		{
			/// Все остальные параметры запроса рассматриваются, как настройки.

			if (readonly_before_query == 1)
				throw Exception("Cannot override setting (" + it->first + ") in readonly mode", ErrorCodes::READONLY);

			if (readonly_before_query && it->first == "readonly")
				throw Exception("Setting 'readonly' cannot be overrided in readonly mode", ErrorCodes::READONLY);

			context.setSetting(it->first, it->second);
		}
	}

	/// Сжатие ответа (Content-Encoding) включается только если клиент сказал, что он это понимает (Accept-Encoding)
	/// и выставлена настройка, разрешающая сжатие.
	used_output.out->setCompression(client_supports_http_compression && context.getSettingsRef().enable_http_compression);
	if (client_supports_http_compression)
		used_output.out->setCompressionLevel(context.getSettingsRef().http_zlib_compression_level);

	/// Возможно, что выставлена настройка - не проверять чексуммы при разжатии данных от клиента, сжатых родным форматом.
	if (in_post_compressed && context.getSettingsRef().http_native_compression_disable_checksumming_on_decompress)
		static_cast<CompressedReadBuffer &>(*in_post_maybe_compressed).disableChecksumming();

	/// Добавить CORS header выставлена настройка, и если клиент передал заголовок Origin
	used_output.out->addHeaderCORS( context.getSettingsRef().add_http_cors_header && !request.get("Origin", "").empty() );

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

			/** If buffer has data, and that data wasn't sent yet, then no need to send that data */
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

	/// In case of exception, send stack trace to client.
	bool with_stacktrace = false;

	try
	{
		response.setContentType("text/plain; charset=UTF-8");

		/// For keep-alive to work.
		if (request.getVersion() == Poco::Net::HTTPServerRequest::HTTP_1_1)
			response.setChunkedTransferEncoding(true);

		HTMLForm params(request);
		with_stacktrace = parse<bool>(params.get("stacktrace", "0"));

		processQuery(request, params, response, used_output);
		LOG_INFO(log, "Done processing query");
	}
	catch (...)
	{
		tryLogCurrentException(log);

		std::string exception_message = getCurrentExceptionMessage(with_stacktrace);

		/** If exception is received from remote server, then stack trace is embedded in message.
		  * If exception is thrown on local server, then stack trace is in separate field.
		  */

		auto embedded_stack_trace_pos = exception_message.find("Stack trace");
		if (std::string::npos != embedded_stack_trace_pos && !with_stacktrace)
			exception_message.resize(embedded_stack_trace_pos);

		trySendExceptionToClient(exception_message, request, response, used_output);
	}
}


}
