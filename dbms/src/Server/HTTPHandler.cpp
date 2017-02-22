#include <iomanip>

#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/File.h>

#include <DB/Common/ExternalTable.h>
#include <DB/Common/StringUtils.h>
#include <DB/Common/escapeForFileName.h>

#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/ZlibInflatingReadBuffer.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/ConcatReadBuffer.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteBufferFromHTTPServerResponse.h>
#include <DB/IO/MemoryReadWriteBuffer.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/copyData.h>
#include <DB/IO/ConcatReadBuffer.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>

#include <DB/Interpreters/executeQuery.h>
#include <DB/Interpreters/Quota.h>

#include "HTTPHandler.h"

namespace DB
{

namespace ErrorCodes
{
	extern const int READONLY;
	extern const int UNKNOWN_COMPRESSION_METHOD;

	extern const int CANNOT_PARSE_TEXT;
	extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
	extern const int CANNOT_PARSE_QUOTED_STRING;
	extern const int CANNOT_PARSE_DATE;
	extern const int CANNOT_PARSE_DATETIME;
	extern const int CANNOT_PARSE_NUMBER;
	extern const int CANNOT_OPEN_FILE;

	extern const int UNKNOWN_ELEMENT_IN_AST;
	extern const int UNKNOWN_TYPE_OF_AST_NODE;
	extern const int TOO_DEEP_AST;
	extern const int TOO_BIG_AST;
	extern const int UNEXPECTED_AST_STRUCTURE;

	extern const int UNKNOWN_TABLE;
	extern const int UNKNOWN_FUNCTION;
	extern const int UNKNOWN_IDENTIFIER;
	extern const int UNKNOWN_TYPE;
	extern const int UNKNOWN_STORAGE;
	extern const int UNKNOWN_DATABASE;
	extern const int UNKNOWN_SETTING;
	extern const int UNKNOWN_DIRECTION_OF_SORTING;
	extern const int UNKNOWN_AGGREGATE_FUNCTION;
	extern const int UNKNOWN_FORMAT;
	extern const int UNKNOWN_DATABASE_ENGINE;
	extern const int UNKNOWN_TYPE_OF_QUERY;

	extern const int QUERY_IS_TOO_LARGE;

	extern const int NOT_IMPLEMENTED;
	extern const int SOCKET_TIMEOUT;

	extern const int UNKNOWN_USER;
	extern const int WRONG_PASSWORD;
	extern const int REQUIRED_PASSWORD;
}

static Poco::Net::HTTPResponse::HTTPStatus exceptionCodeToHTTPStatus(int exception_code)
{
	using namespace Poco::Net;

	if (exception_code == ErrorCodes::REQUIRED_PASSWORD)
		return HTTPResponse::HTTP_UNAUTHORIZED;
	else if (exception_code == ErrorCodes::CANNOT_PARSE_TEXT ||
			 exception_code == ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE ||
			 exception_code == ErrorCodes::CANNOT_PARSE_QUOTED_STRING ||
			 exception_code == ErrorCodes::CANNOT_PARSE_DATE ||
			 exception_code == ErrorCodes::CANNOT_PARSE_DATETIME ||
			 exception_code == ErrorCodes::CANNOT_PARSE_NUMBER)
		return HTTPResponse::HTTP_BAD_REQUEST;
	else if (exception_code == ErrorCodes::UNKNOWN_ELEMENT_IN_AST ||
			 exception_code == ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE ||
			 exception_code == ErrorCodes::TOO_DEEP_AST ||
			 exception_code == ErrorCodes::TOO_BIG_AST ||
			 exception_code == ErrorCodes::UNEXPECTED_AST_STRUCTURE)
		return HTTPResponse::HTTP_BAD_REQUEST;
	else if (exception_code == ErrorCodes::UNKNOWN_TABLE ||
			 exception_code == ErrorCodes::UNKNOWN_FUNCTION ||
			 exception_code == ErrorCodes::UNKNOWN_IDENTIFIER ||
			 exception_code == ErrorCodes::UNKNOWN_TYPE ||
			 exception_code == ErrorCodes::UNKNOWN_STORAGE ||
			 exception_code == ErrorCodes::UNKNOWN_DATABASE ||
			 exception_code == ErrorCodes::UNKNOWN_SETTING ||
			 exception_code == ErrorCodes::UNKNOWN_DIRECTION_OF_SORTING ||
			 exception_code == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION ||
			 exception_code == ErrorCodes::UNKNOWN_FORMAT ||
			 exception_code == ErrorCodes::UNKNOWN_DATABASE_ENGINE)
		return HTTPResponse::HTTP_NOT_FOUND;
	else if (exception_code == ErrorCodes::UNKNOWN_TYPE_OF_QUERY)
		return HTTPResponse::HTTP_NOT_FOUND;
	else if (exception_code == ErrorCodes::QUERY_IS_TOO_LARGE)
		return HTTPResponse::HTTP_REQUESTENTITYTOOLARGE;
	else if (exception_code == ErrorCodes::NOT_IMPLEMENTED)
		return HTTPResponse::HTTP_NOT_IMPLEMENTED;
	else if (exception_code == ErrorCodes::SOCKET_TIMEOUT ||
			 exception_code == ErrorCodes::CANNOT_OPEN_FILE)
		return HTTPResponse::HTTP_SERVICE_UNAVAILABLE;

	return HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;
}

HTTPHandler::HTTPHandler(Server & server_)
	: server(server_)
	, log(&Logger::get("HTTPHandler"))
{
}

void HTTPHandler::processQuery(
	Poco::Net::HTTPServerRequest & request,
	HTMLForm & params,
	Poco::Net::HTTPServerResponse & response,
	Output & used_output)
{
	LOG_TRACE(log, "Request URI: " << request.getURI());

	std::istream & istr = request.stream();

	/// Part of the query can be passed in the 'query' parameter and the rest in the request body
	/// (http method need not necessarily be POST). In this case the entire query consists of the
	/// contents of the 'query' parameter, a line break and the request body.
	std::string query_param = params.get("query", "");
	if (!query_param.empty())
		query_param += '\n';


	/// User name and password can be passed using query parameters or using HTTP Basic auth (both methods are insecure).
	/// The user and password can be passed by headers (similar to X-Auth-*), which is used by load balancers to pass authentication information
	std::string user = request.get("X-ClickHouse-User", params.get("user", "default"));
	std::string password = request.get("X-ClickHouse-Key", params.get("password", ""));

	if (request.hasCredentials())
	{
		Poco::Net::HTTPBasicCredentials credentials(request);

		user = credentials.getUsername();
		password = credentials.getPassword();
	}

	std::string quota_key = request.get("X-ClickHouse-Quota", params.get("quota_key", ""));
	std::string query_id = params.get("query_id", "");

	Context context = *server.global_context;
	context.setGlobalContext(*server.global_context);

	context.setUser(user, password, request.clientAddress(), quota_key);
	context.setCurrentQueryId(query_id);


	/// The client can pass a HTTP header indicating supported compression method (gzip or deflate).
	String http_response_compression_methods = request.get("Accept-Encoding", "");
	bool client_supports_http_compression = false;
	ZlibCompressionMethod http_response_compression_method {};

	if (!http_response_compression_methods.empty())
	{
		/// Both gzip and deflate are supported. If the client supports both, gzip is preferred.
		/// NOTE parsing of the list of methods is slightly incorrect.
		if (std::string::npos != http_response_compression_methods.find("gzip"))
		{
			client_supports_http_compression = true;
			http_response_compression_method = ZlibCompressionMethod::Gzip;
		}
		else if (std::string::npos != http_response_compression_methods.find("deflate"))
		{
			client_supports_http_compression = true;
			http_response_compression_method = ZlibCompressionMethod::Zlib;
		}
	}

	/// Client can pass a 'compress' flag in the query string. In this case the query result is
	/// compressed using internal algorithm. This is not reflected in HTTP headers.
	bool internal_compression = params.getParsed<bool>("compress", false);

	size_t response_buffer_size = params.getParsed<size_t>("buffer_size", DBMS_DEFAULT_BUFFER_SIZE);
	response_buffer_size = response_buffer_size ? response_buffer_size : DBMS_DEFAULT_BUFFER_SIZE;

	auto response_raw = std::make_shared<WriteBufferFromHTTPServerResponse>(
		response, client_supports_http_compression, http_response_compression_method, response_buffer_size);

	WriteBufferPtr response_maybe_compressed;

	size_t result_buffer_memory_size = params.getParsed<size_t>("result_buffer_size", 0);
	bool use_memory_buffer = result_buffer_memory_size;

	bool result_buffer_overflow_to_disk = params.get("result_buffer_on_overflow", "http") == "disk";

	if (use_memory_buffer)
	{
		CascadeWriteBuffer::WriteBufferPtrs concat_buffers1{ std::make_shared<MemoryWriteBuffer>(result_buffer_memory_size) };
		CascadeWriteBuffer::WriteBufferConstructors concat_buffers2{};

		if (result_buffer_overflow_to_disk)
		{
			std::string tmp_path_template = context.getTemporaryPath() + "http_buffers/" + escapeForFileName(user) + ".XXXXXX";

			auto create_tmp_disk_buffer = [tmp_path_template] (const WriteBufferPtr &)
			{
				return WriteBufferFromTemporaryFile::create(tmp_path_template);
			};

			concat_buffers2.emplace_back(std::move(create_tmp_disk_buffer));
		}
		else
		{
			auto rewrite_memory_buffer_to_http_and_continue = [response_raw] (const WriteBufferPtr & prev_buf)
			{
				auto memory_write_buffer = typeid_cast<MemoryWriteBuffer *>(prev_buf.get());

				if (!memory_write_buffer)
					throw Exception("Memory buffer was not allocated", ErrorCodes::LOGICAL_ERROR);

				auto memory_read_buffer = memory_write_buffer->getReadBuffer();
				copyData(*memory_read_buffer, *response_raw);

				return response_raw;
			};

			concat_buffers2.emplace_back(rewrite_memory_buffer_to_http_and_continue);
		}

		used_output.out = response_raw;
		used_output.delayed_out_raw = std::make_shared<CascadeWriteBuffer>(std::move(concat_buffers1), std::move(concat_buffers2));
		if (internal_compression)
			used_output.delayed_out_maybe_compressed = std::make_shared<CompressedWriteBuffer>(*used_output.delayed_out_raw);
		else
			used_output.delayed_out_maybe_compressed = used_output.delayed_out_raw;
		used_output.out_maybe_compressed = used_output.delayed_out_maybe_compressed;
	}
	else
	{
		used_output.out = response_raw;
		if (internal_compression)
			used_output.out_maybe_compressed = std::make_shared<CompressedWriteBuffer>(*response_raw);
		else
			used_output.out_maybe_compressed = response_raw;
	}

	std::unique_ptr<ReadBuffer> in_param = std::make_unique<ReadBufferFromString>(query_param);

	std::unique_ptr<ReadBuffer> in_post_raw = std::make_unique<ReadBufferFromIStream>(istr);

	/// Request body can be compressed using algorithm specified in the Content-Encoding header.
	std::unique_ptr<ReadBuffer> in_post;
	String http_request_compression_method_str = request.get("Content-Encoding", "");
	if (!http_request_compression_method_str.empty())
	{
		ZlibCompressionMethod method;
		if (http_request_compression_method_str == "gzip")
		{
			method = ZlibCompressionMethod::Gzip;
		}
		else if (http_request_compression_method_str == "deflate")
		{
			method = ZlibCompressionMethod::Zlib;
		}
		else
			throw Exception("Unknown Content-Encoding of HTTP request: " + http_request_compression_method_str,
				ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
		in_post = std::make_unique<ZlibInflatingReadBuffer>(*in_post_raw, method);
	}
	else
		in_post = std::move(in_post_raw);

	/// The data can also be compressed using incompatible internal algorithm. This is indicated by
	/// 'decompress' query parameter.
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

	/// Support for "external data for query processing".
	if (startsWith(request.getContentType().data(), "multipart/form-data"))
	{
		in = std::move(in_param);
		ExternalTablesHandler handler(context, params);

		params.load(request, istr, handler);

		/// Erase unneeded parameters to avoid confusing them later with context settings or query
		/// parameters.
		for (const auto & it : handler.names)
		{
			params.erase(it + "_format");
			params.erase(it + "_types");
			params.erase(it + "_structure");
		}
	}
	else
		in = std::make_unique<ConcatReadBuffer>(*in_param, *in_post_maybe_compressed);

	/// Settings can be overridden in the query.
	/// Some parameters (database, default_format, everything used in the code above) do not
	/// belong to the Settings class.

	/// 'readonly' setting values mean:
	/// readonly = 0 - any query is allowed, client can change any setting.
	/// readonly = 1 - only readonly queries are allowed, client can't change settings.
	/// readonly = 2 - only readonly queries are allowed, client can change any setting except 'readonly'.

	/// In theory if initially readonly = 0, the client can change any setting and then set readonly
	/// to some other value.
	auto & limits = context.getSettingsRef().limits;

	/// Only readonly queries are allowed for HTTP GET requests.
	if (request.getMethod() == Poco::Net::HTTPServerRequest::HTTP_GET)
	{
		if (limits.readonly == 0)
			limits.readonly = 2;
	}

	auto readonly_before_query = limits.readonly;

	NameSet reserved_param_names{"query", "compress", "decompress", "user", "password", "quota_key", "query_id", "stacktrace",
		"buffer_size", "result_buffer_size", "result_buffer_on_overflow"
	};

	for (auto it = params.begin(); it != params.end(); ++it)
	{
		if (it->first == "database")
		{
			context.setCurrentDatabase(it->second);
		}
		else if (it->first == "default_format")
		{
			context.setDefaultFormat(it->second);
		}
		else if (reserved_param_names.find(it->first) != reserved_param_names.end())
		{
		}
		else
		{
			/// All other query parameters are treated as settings.

			if (readonly_before_query == 1)
				throw Exception("Cannot override setting (" + it->first + ") in readonly mode", ErrorCodes::READONLY);

			if (readonly_before_query && it->first == "readonly")
				throw Exception("Setting 'readonly' cannot be overrided in readonly mode", ErrorCodes::READONLY);

			context.setSetting(it->first, it->second);
		}
	}

	const Settings & settings = context.getSettingsRef();

	/// HTTP response compression is turned on only if the client signalled that they support it
	/// (using Accept-Encoding header) and 'enable_http_compression' setting is turned on.
	used_output.out->setCompression(client_supports_http_compression && settings.enable_http_compression);
	if (client_supports_http_compression)
		used_output.out->setCompressionLevel(settings.http_zlib_compression_level);

	used_output.out->setSendProgressInterval(settings.http_headers_progress_interval_ms);

	/// If 'http_native_compression_disable_checksumming_on_decompress' setting is turned on,
	/// checksums of client data compressed with internal algorithm are not checked.
	if (in_post_compressed && settings.http_native_compression_disable_checksumming_on_decompress)
		static_cast<CompressedReadBuffer &>(*in_post_maybe_compressed).disableChecksumming();

	/// Add CORS header if 'add_http_cors_header' setting is turned on and the client passed
	/// Origin header.
	used_output.out->addHeaderCORS(settings.add_http_cors_header && !request.get("Origin", "").empty());

	ClientInfo & client_info = context.getClientInfo();
	client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
	client_info.interface = ClientInfo::Interface::HTTP;

	/// Query sent through HTTP interface is initial.
	client_info.initial_user = client_info.current_user;
	client_info.initial_query_id = client_info.current_query_id;
	client_info.initial_address = client_info.current_address;

	ClientInfo::HTTPMethod http_method = ClientInfo::HTTPMethod::UNKNOWN;
	if (request.getMethod() == Poco::Net::HTTPServerRequest::HTTP_GET)
		http_method = ClientInfo::HTTPMethod::GET;
	else if (request.getMethod() == Poco::Net::HTTPServerRequest::HTTP_POST)
		http_method = ClientInfo::HTTPMethod::POST;

	client_info.http_method = http_method;
	client_info.http_user_agent = request.get("User-Agent", "");

	/// While still no data has been sent, we will report about query execution progress by sending HTTP headers.
	if (settings.send_progress_in_http_headers)
		context.setProgressCallback([&used_output] (const Progress & progress) { used_output.out->onProgress(progress); });

	executeQuery(*in, *used_output.out_maybe_compressed, /* allow_into_outfile = */ false, context,
		[&response] (const String & content_type) { response.setContentType(content_type); });

	if (use_memory_buffer)
	{
		std::vector<WriteBufferPtr> write_buffers;
		used_output.delayed_out_raw->getResultBuffers(write_buffers);

		std::vector<ReadBufferPtr> read_buffers;
		ConcatReadBuffer::ReadBuffers read_buffers_raw_ptr;
		for (auto & write_buf : write_buffers)
		{
			IReadableWriteBuffer * write_buf_concrete;
			if (write_buf && (write_buf_concrete = dynamic_cast<IReadableWriteBuffer *>(write_buf.get())))
			{
				read_buffers.emplace_back(write_buf_concrete->getReadBuffer());
				read_buffers_raw_ptr.emplace_back(read_buffers.back().get());
			}
		}

		if (result_buffer_overflow_to_disk)
		{
			/// All results in concat buffer
			if (read_buffers_raw_ptr.empty())
				throw Exception("There are no buffers to overwrite result into response", ErrorCodes::LOGICAL_ERROR);

			ConcatReadBuffer concat_read_buffer(read_buffers_raw_ptr);
			copyData(concat_read_buffer, *used_output.out);
		}
		else
		{
			/// Results could be either in the first buffer, or they could be already pushed to response
			if (!write_buffers.at(1))
			{
				/// Results was not pushed to reponse
				copyData(*read_buffers.at(0), *used_output.out);
			}
		}

		used_output.delayed_out_raw.reset();
		used_output.delayed_out_maybe_compressed.reset();
	}

	/// Send HTTP headers with code 200 if no exception happened and the data is still not sent to
	/// the client.
	used_output.out->finalize();
}

void HTTPHandler::trySendExceptionToClient(const std::string & s, int exception_code,
	Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response,
	Output & used_output)
{
	try
	{
		/// If HTTP method is POST and Keep-Alive is turned on, we should read the whole request body
		/// to avoid reading part of the current request body in the next request.
		if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST
			&& response.getKeepAlive()
			&& !request.stream().eof())
		{
			request.stream().ignore(std::numeric_limits<std::streamsize>::max());
		}

		bool auth_fail = exception_code == ErrorCodes::UNKNOWN_USER ||
						 exception_code == ErrorCodes::WRONG_PASSWORD ||
						 exception_code == ErrorCodes::REQUIRED_PASSWORD;

		if (auth_fail)
		{
			response.requireAuthentication("ClickHouse server HTTP API");
		}
		else
		{
			response.setStatusAndReason(exceptionCodeToHTTPStatus(exception_code));
		}

		if (!response.sent() && !used_output.out_maybe_compressed)
		{
			/// If nothing was sent yet and we don't even know if we must compress the response.
			response.send() << s << std::endl;
		}
		else if (used_output.out_maybe_compressed)
		{
			/// Send the error message into already used (and possibly compressed) stream.
			/// Note that the error message will possibly be sent after some data.
			/// Also HTTP code 200 could have already been sent.

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

		/** If exception is received from remote server, then stack trace is embedded in message.
		  * If exception is thrown on local server, then stack trace is in separate field.
		  */
		std::string exception_message = getCurrentExceptionMessage(with_stacktrace, true);
		int exception_code = getCurrentExceptionCode();

		trySendExceptionToClient(exception_message, exception_code, request, response, used_output);
	}
}


}
