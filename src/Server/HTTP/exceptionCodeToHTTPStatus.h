#pragma once
#include <Poco/Net/HTTPResponse.h>


namespace DB
{

/// Converts Exception code to HTTP status code.
Poco::Net::HTTPResponse::HTTPStatus exceptionCodeToHTTPStatus(int exception_code);

}
