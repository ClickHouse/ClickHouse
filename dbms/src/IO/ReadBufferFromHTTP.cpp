#include <Poco/URI.h>
#include <Poco/Net/DNS.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>

#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/ReadBufferFromHTTP.h>

#include <DB/Common/SimpleCache.h>

#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
}


static Poco::Net::IPAddress resolveHostImpl(const String & host)
{
	return Poco::Net::DNS::resolveOne(host);
}

static Poco::Net::IPAddress resolveHost(const String & host)
{
	static SimpleCache<decltype(resolveHostImpl), &resolveHostImpl> cache;
	return cache(host);
}


ReadBufferFromHTTP::ReadBufferFromHTTP(
	const String & host_,
	int port_,
	const String & path_,
	const Params & params,
	const String & method_,
	size_t buffer_size_,
	const Poco::Timespan & connection_timeout,
	const Poco::Timespan & send_timeout,
	const Poco::Timespan & receive_timeout)
	: ReadBuffer(nullptr, 0), host(host_), port(port_), path(path_), method(method_)
{
	if (method.empty())
		method = Poco::Net::HTTPRequest::HTTP_POST;
	if (path.empty())
		path = "/";

	std::stringstream path_params;
	path_params << path;

	bool first = true;
	for (const auto & it : params)
	{
		path_params << (first ? "?" : "&");
		first = false;
		String encoded_key;
		String encoded_value;
		Poco::URI::encode(it.first, "=&#", encoded_key);
		Poco::URI::encode(it.second, "&#", encoded_value);
		path_params << encoded_key << "=" << encoded_value;
	}

	std::stringstream uri;
	uri << "http://" << host << ":" << port << path_params.str();

	session.setHost(resolveHost(host).toString());	/// Cache DNS forever (until server restart)
	session.setPort(port);

	session.setTimeout(connection_timeout, send_timeout, receive_timeout);

	Poco::Net::HTTPRequest request(method, path_params.str());
	Poco::Net::HTTPResponse response;

	LOG_TRACE((&Logger::get("ReadBufferFromHTTP")), "Sending request to " << uri.str());

	session.sendRequest(request);
	istr = &session.receiveResponse(response);

	Poco::Net::HTTPResponse::HTTPStatus status = response.getStatus();

	if (status != Poco::Net::HTTPResponse::HTTP_OK)
	{
		std::stringstream error_message;
		error_message << "Received error from remote server " << uri.str() << ". HTTP status code: "
			<< status << ", body: " << istr->rdbuf();

		throw Exception(error_message.str(), ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER);
	}

	impl = std::make_unique<ReadBufferFromIStream>(*istr, buffer_size_);
}


bool ReadBufferFromHTTP::nextImpl()
{
	if (!impl->next())
		return false;
	internal_buffer = impl->buffer();
	working_buffer = internal_buffer;
	return true;
}

}
