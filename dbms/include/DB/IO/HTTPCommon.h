#pragma once


namespace Poco
{
	namespace Net
	{
		class HTTPServerResponse;
	}
}


namespace DB
{

void setResponseDefaultHeaders(Poco::Net::HTTPServerResponse & response);

}
