#include <DB/IO/ReadWriteBufferFromHTTP.h>

#include <Poco/URI.h>
#include <Poco/Net/DNS.h>
//#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>

#include <DB/IO/ReadBufferFromIStream.h>

#include <DB/Common/SimpleCache.h>

#include <common/logger_useful.h>


namespace DB
{

// copypaste from ReadBufferFromHTTP.cpp
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
// ==========


ReadWriteBufferFromHTTP::ReadWriteBufferFromHTTP(
		HTTPLocation location,
		size_t buffer_size_,
		HTTPTimeouts timeouts
	) :
	ReadBuffer(nullptr, 0),
	location{location},
	timeouts{timeouts}
{

	std::stringstream path_params;
	path_params << location.path;

	bool first = true;
	for (const auto & it : location.params)
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
	uri << "http://" << location.host << ":" << location.port << path_params.str();

	session.setHost(resolveHost(location.host).toString());	/// Cache DNS forever (until server restart)
	session.setPort(location.port);

	session.setTimeout(timeouts.connection_timeout, timeouts.send_timeout, timeouts.receive_timeout);

	Poco::Net::HTTPRequest request(location.method, path_params.str());
	Poco::Net::HTTPResponse response;

	LOG_TRACE((&Logger::get("ReadBufferFromHTTP")), "Sending request to " << uri.str());

	//auto & stream_out = 
	session.sendRequest(request);
	istr = &session.receiveResponse(response);

	auto status = response.getStatus();

	if (status != Poco::Net::HTTPResponse::HTTP_OK)
	{
		std::stringstream error_message;
		error_message << "Received error from remote server " << uri.str() << ". HTTP status code: "
			<< status << ", body: " << istr->rdbuf();

		throw Exception(error_message.str(), ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER);
	}

	impl = std::make_unique<ReadBufferFromIStream>(*istr, buffer_size_);
}


bool ReadWriteBufferFromHTTP::nextImpl()
{
	if (!impl->next())
		return false;
	internal_buffer = impl->buffer();
	working_buffer = internal_buffer;
	return true;
}

}
