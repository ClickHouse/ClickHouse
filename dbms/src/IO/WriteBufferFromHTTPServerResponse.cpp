#include <Poco/Net/HTTPServerResponse.h>

#include <DB/Common/Exception.h>

#include <DB/IO/WriteBufferFromHTTPServerResponse.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/HTTPCommon.h>
#include <DB/Common/NetException.h>
#include <DB/Common/Stopwatch.h>
#include <DB/Core/Progress.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
}


void WriteBufferFromHTTPServerResponse::startSendHeaders()
{
	if (!headers_started_sending)
	{
		headers_started_sending = true;

		if (add_cors_header)
			response.set("Access-Control-Allow-Origin", "*");

		setResponseDefaultHeaders(response);

		std::tie(response_header_ostr, response_body_ostr) = response.beginSend();
	}
}


void WriteBufferFromHTTPServerResponse::finishSendHeaders()
{
	if (!headers_finished_sending)
	{
		headers_finished_sending = true;

		/// Send end of headers delimiter.
		*response_header_ostr << "\r\n" << std::flush;
	}
}


void WriteBufferFromHTTPServerResponse::nextImpl()
{
	{
		std::lock_guard<std::mutex> lock(mutex);

		startSendHeaders();

		if (!out)
		{
			if (compress)
			{
				if (compression_method == ZlibCompressionMethod::Gzip)
					*response_header_ostr << "Content-Encoding: gzip\r\n";
				else if (compression_method == ZlibCompressionMethod::Zlib)
					*response_header_ostr << "Content-Encoding: deflate\r\n";
				else
					throw Exception("Logical error: unknown compression method passed to WriteBufferFromHTTPServerResponse",
									ErrorCodes::LOGICAL_ERROR);

				/// Use memory allocated for the outer buffer in the buffer pointed to by out. This avoids extra allocation and copy.
				out_raw.emplace(*response_body_ostr);
				deflating_buf.emplace(out_raw.value(), compression_method, compression_level, working_buffer.size(), working_buffer.begin());
				out = &deflating_buf.value();
			}
			else
			{
				out_raw.emplace(*response_body_ostr, working_buffer.size(), working_buffer.begin());
				out = &out_raw.value();
			}
		}

		finishSendHeaders();
	}

	out->position() = position();
	out->next();
}


WriteBufferFromHTTPServerResponse::WriteBufferFromHTTPServerResponse(
	Poco::Net::HTTPServerResponse & response_,
	bool compress_,
	ZlibCompressionMethod compression_method_,
	size_t size)
	: BufferWithOwnMemory<WriteBuffer>(size), response(response_),
	compress(compress_), compression_method(compression_method_)
{
}


void WriteBufferFromHTTPServerResponse::onProgress(const Progress & progress)
{
	std::lock_guard<std::mutex> lock(mutex);

	/// Cannot add new headers if body was started to send.
	if (headers_finished_sending)
		return;

	accumulated_progress.incrementPiecewiseAtomically(progress);

	if (progress_watch.elapsed() >= send_progress_interval_ms * 1000000)
	{
		progress_watch.restart();

		/// Send all common headers before our special progress headers.
		startSendHeaders();

		std::string progress_string;
		{
			WriteBufferFromString progress_string_writer(progress_string);
			accumulated_progress.writeJSON(progress_string_writer);
		}

		*response_header_ostr << "X-ClickHouse-Progress: " << progress_string << "\r\n" << std::flush;
	}
}


void WriteBufferFromHTTPServerResponse::finalize()
{
	if (offset())
	{
		next();
	}
	else
	{
		/// If no remaining data, just send headers.
		std::lock_guard<std::mutex> lock(mutex);
		startSendHeaders();
		finishSendHeaders();
	}
}


WriteBufferFromHTTPServerResponse::~WriteBufferFromHTTPServerResponse()
{
	try
	{
		finalize();
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
	}
}

}
