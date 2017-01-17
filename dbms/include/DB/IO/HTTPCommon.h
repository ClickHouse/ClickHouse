#pragma once

#include <Poco/Net/HTTPServerResponse.h> 

namespace DB
{

void setResponseDefaultHeaders(Poco::Net::HTTPServerResponse & response);

}
