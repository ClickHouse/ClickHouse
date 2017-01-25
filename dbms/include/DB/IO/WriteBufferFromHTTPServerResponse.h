#pragma once

#include <experimental/optional>
#include <mutex>

#include <Poco/Net/HTTPServerResponse.h>

#include <DB/Common/Exception.h>

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/ZlibDeflatingWriteBuffer.h>
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


/// The difference from WriteBufferFromOStream is that this buffer gets the underlying std::ostream
/// (using response.send()) only after data is flushed for the first time. This is needed in HTTP
/// servers to change some HTTP headers (e.g. response code) before any data is sent to the client
/// (headers can't be changed after response.send() is called).
///
/// In short, it allows delaying the call to response.send().
///
/// Additionally, supports HTTP response compression (in this case corresponding Content-Encoding
/// header will be set).
class WriteBufferFromHTTPServerResponse : public BufferWithOwnMemory<WriteBuffer>
{
private:
	Poco::Net::HTTPServerResponse & response;

	bool add_cors_header;
	bool compress;
	ZlibCompressionMethod compression_method;
	int compression_level = Z_DEFAULT_COMPRESSION;

	std::ostream * response_body_ostr = nullptr;
	std::ostream * response_header_ostr = nullptr;

	std::experimental::optional<WriteBufferFromOStream> out_raw;
	std::experimental::optional<ZlibDeflatingWriteBuffer> deflating_buf;

	WriteBuffer * out = nullptr; 	/// Uncompressed HTTP body is written to this buffer. Points to out_raw or possibly to deflating_buf.

	bool headers_started_sending = false;
	bool headers_finished_sending = false;	/// If true, you could not add any headers.

	Progress accumulated_progress;
	size_t send_progress_interval_ms = 100;
	Stopwatch progress_watch;

	std::mutex mutex;	/// progress callback could be called from different threads.


	/// Must be called under locked mutex.
	/// This method send headers, if this was not done already,
	///  but not finish them with \r\n, allowing to send more headers subsequently.
	void startSendHeaders()
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

	/// This method finish headers with \r\n, allowing to start to send body.
	void finishSendHeaders()
	{
		if (!headers_finished_sending)
		{
			headers_finished_sending = true;

			/// Send end of headers delimiter.
			*response_header_ostr << "\r\n" << std::flush;
		}
	}

	void nextImpl() override
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

public:
	WriteBufferFromHTTPServerResponse(
		Poco::Net::HTTPServerResponse & response_,
		bool compress_ = false,		/// If true - set Content-Encoding header and compress the result.
		ZlibCompressionMethod compression_method_ = ZlibCompressionMethod::Gzip,
		size_t size = DBMS_DEFAULT_BUFFER_SIZE)
		: BufferWithOwnMemory<WriteBuffer>(size), response(response_),
		compress(compress_), compression_method(compression_method_) {}

	/// Writes progess in repeating HTTP headers.
	void onProgress(const Progress & progress)
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

	/// Send at least HTTP headers if no data has been sent yet.
	/// Use after the data has possibly been sent and no error happened (and thus you do not plan
	/// to change response HTTP code.
	/// This method is idempotent.
	void finalize()
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

	/// Turn compression on or off.
	/// The setting has any effect only if HTTP headers haven't been sent yet.
	void setCompression(bool enable_compression)
	{
		compress = enable_compression;
	}

	/// Set compression level if the compression is turned on.
	/// The setting has any effect only if HTTP headers haven't been sent yet.
	void setCompressionLevel(int level)
	{
		compression_level = level;
	}

	/// Turn CORS on or off.
	/// The setting has any effect only if HTTP headers haven't been sent yet.
	void addHeaderCORS(bool enable_cors)
	{
		add_cors_header = enable_cors;
	}

	/// Don't send HTTP headers with progress more frequently.
	void setSendProgressInterval(size_t send_progress_interval_ms_)
	{
		send_progress_interval_ms = send_progress_interval_ms_;
	}

	~WriteBufferFromHTTPServerResponse()
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
};

}
