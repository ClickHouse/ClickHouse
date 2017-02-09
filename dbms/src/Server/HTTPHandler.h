#pragma once

#include <DB/Common/CurrentMetrics.h>
#include "Server.h"


namespace CurrentMetrics
{
	extern const Metric HTTPConnection;
}

namespace DB
{

class WriteBufferFromHTTPServerResponse;
class CascadeWriteBuffer;


class HTTPHandler : public Poco::Net::HTTPRequestHandler
{
public:
	explicit HTTPHandler(Server & server_);

	void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

private:
	struct Output
	{
		std::shared_ptr<WriteBufferFromHTTPServerResponse> out;
		/// Used for sending response. Points to 'out', or to CompressedWriteBuffer(*out), depending on settings.
		std::shared_ptr<WriteBuffer> out_maybe_compressed;

		std::shared_ptr<CascadeWriteBuffer> delayed_out_raw;
		std::shared_ptr<WriteBuffer> delayed_out_maybe_compressed;
	};

	Server & server;

	CurrentMetrics::Increment metric_increment{CurrentMetrics::HTTPConnection};

	Logger * log;

	/// Also initializes 'used_output'.
	void processQuery(
		Poco::Net::HTTPServerRequest & request,
		HTMLForm & params,
		Poco::Net::HTTPServerResponse & response,
		Output & used_output);

	void trySendExceptionToClient(const std::string & s, int exception_code,
		Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response,
		Output & used_output);
};

}
