//
// HTTPResponse.cpp
//
// $Id: //poco/1.4/Net/src/HTTPResponse.cpp#2 $
//
// Library: Net
// Package: HTTP
// Module:  HTTPResponse
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/HTTPResponse.h"
#include "Poco/Net/NetException.h"
#include "Poco/NumberFormatter.h"
#include "Poco/NumberParser.h"
#include "Poco/DateTime.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/DateTimeFormat.h"
#include "Poco/DateTimeParser.h"
#include "Poco/Ascii.h"
#include "Poco/String.h"


using Poco::DateTime;
using Poco::NumberFormatter;
using Poco::NumberParser;
using Poco::DateTimeFormatter;
using Poco::DateTimeFormat;
using Poco::DateTimeParser;


namespace Poco {
namespace Net {


const std::string HTTPResponse::HTTP_REASON_CONTINUE                        = "Continue";
const std::string HTTPResponse::HTTP_REASON_SWITCHING_PROTOCOLS             = "Switching Protocols";
const std::string HTTPResponse::HTTP_REASON_OK                              = "OK";
const std::string HTTPResponse::HTTP_REASON_CREATED                         = "Created";
const std::string HTTPResponse::HTTP_REASON_ACCEPTED                        = "Accepted";
const std::string HTTPResponse::HTTP_REASON_NONAUTHORITATIVE                = "Non-Authoritative Information";
const std::string HTTPResponse::HTTP_REASON_NO_CONTENT                      = "No Content";
const std::string HTTPResponse::HTTP_REASON_RESET_CONTENT                   = "Reset Content";
const std::string HTTPResponse::HTTP_REASON_PARTIAL_CONTENT                 = "Partial Content";
const std::string HTTPResponse::HTTP_REASON_MULTIPLE_CHOICES                = "Multiple Choices";
const std::string HTTPResponse::HTTP_REASON_MOVED_PERMANENTLY               = "Moved Permanently";
const std::string HTTPResponse::HTTP_REASON_FOUND                           = "Found";
const std::string HTTPResponse::HTTP_REASON_SEE_OTHER                       = "See Other";
const std::string HTTPResponse::HTTP_REASON_NOT_MODIFIED                    = "Not Modified";
const std::string HTTPResponse::HTTP_REASON_USEPROXY                        = "Use Proxy";
const std::string HTTPResponse::HTTP_REASON_TEMPORARY_REDIRECT              = "Temporary Redirect";
const std::string HTTPResponse::HTTP_REASON_BAD_REQUEST                     = "Bad Request";
const std::string HTTPResponse::HTTP_REASON_UNAUTHORIZED                    = "Unauthorized";
const std::string HTTPResponse::HTTP_REASON_PAYMENT_REQUIRED                = "Payment Required";
const std::string HTTPResponse::HTTP_REASON_FORBIDDEN                       = "Forbidden";
const std::string HTTPResponse::HTTP_REASON_NOT_FOUND                       = "Not Found";
const std::string HTTPResponse::HTTP_REASON_METHOD_NOT_ALLOWED              = "Method Not Allowed";
const std::string HTTPResponse::HTTP_REASON_NOT_ACCEPTABLE                  = "Not Acceptable";
const std::string HTTPResponse::HTTP_REASON_PROXY_AUTHENTICATION_REQUIRED   = "Proxy Authentication Required";
const std::string HTTPResponse::HTTP_REASON_REQUEST_TIMEOUT                 = "Request Time-out";
const std::string HTTPResponse::HTTP_REASON_CONFLICT                        = "Conflict";
const std::string HTTPResponse::HTTP_REASON_GONE                            = "Gone";
const std::string HTTPResponse::HTTP_REASON_LENGTH_REQUIRED                 = "Length Required";
const std::string HTTPResponse::HTTP_REASON_PRECONDITION_FAILED             = "Precondition Failed";
const std::string HTTPResponse::HTTP_REASON_REQUESTENTITYTOOLARGE           = "Request Entity Too Large";
const std::string HTTPResponse::HTTP_REASON_REQUESTURITOOLONG               = "Request-URI Too Large";
const std::string HTTPResponse::HTTP_REASON_UNSUPPORTEDMEDIATYPE            = "Unsupported Media Type";
const std::string HTTPResponse::HTTP_REASON_REQUESTED_RANGE_NOT_SATISFIABLE = "Requested Range Not Satisfiable";
const std::string HTTPResponse::HTTP_REASON_EXPECTATION_FAILED              = "Expectation Failed";
const std::string HTTPResponse::HTTP_REASON_INTERNAL_SERVER_ERROR           = "Internal Server Error";
const std::string HTTPResponse::HTTP_REASON_NOT_IMPLEMENTED                 = "Not Implemented";
const std::string HTTPResponse::HTTP_REASON_BAD_GATEWAY                     = "Bad Gateway";
const std::string HTTPResponse::HTTP_REASON_SERVICE_UNAVAILABLE             = "Service Unavailable";
const std::string HTTPResponse::HTTP_REASON_GATEWAY_TIMEOUT                 = "Gateway Time-out";
const std::string HTTPResponse::HTTP_REASON_VERSION_NOT_SUPPORTED           = "HTTP Version not supported";
const std::string HTTPResponse::HTTP_REASON_UNKNOWN                         = "???";
const std::string HTTPResponse::DATE       = "Date";
const std::string HTTPResponse::SET_COOKIE = "Set-Cookie";


HTTPResponse::HTTPResponse():
	_status(HTTP_OK),
	_reason(getReasonForStatus(HTTP_OK))
{
}

	
HTTPResponse::HTTPResponse(HTTPStatus status, const std::string& reason):
	_status(status),
	_reason(reason)
{
}


	
HTTPResponse::HTTPResponse(const std::string& version, HTTPStatus status, const std::string& reason):
	HTTPMessage(version),
	_status(status),
	_reason(reason)
{
}

	
HTTPResponse::HTTPResponse(HTTPStatus status):
	_status(status),
	_reason(getReasonForStatus(status))
{
}


HTTPResponse::HTTPResponse(const std::string& version, HTTPStatus status):
	HTTPMessage(version),
	_status(status),
	_reason(getReasonForStatus(status))
{
}


HTTPResponse::~HTTPResponse()
{
}


void HTTPResponse::setStatus(HTTPStatus status)
{
	_status = status;
}


void HTTPResponse::setStatus(const std::string& status)
{
	setStatus((HTTPStatus) NumberParser::parse(status));
}
	
	
void HTTPResponse::setReason(const std::string& reason)
{
	_reason = reason;
}


void HTTPResponse::setStatusAndReason(HTTPStatus status, const std::string& reason)
{
	_status = status;
	_reason = reason;
}

	
void HTTPResponse::setStatusAndReason(HTTPStatus status)
{
	setStatusAndReason(status, getReasonForStatus(status));
}


void HTTPResponse::setDate(const Poco::Timestamp& dateTime)
{
	set(DATE, DateTimeFormatter::format(dateTime, DateTimeFormat::HTTP_FORMAT));
}

	
Poco::Timestamp HTTPResponse::getDate() const
{
	const std::string& dateTime = get(DATE);
	int tzd;
	return DateTimeParser::parse(dateTime, tzd).timestamp();
}


void HTTPResponse::addCookie(const HTTPCookie& cookie)
{
	add(SET_COOKIE, cookie.toString());
}


void HTTPResponse::getCookies(std::vector<HTTPCookie>& cookies) const
{
	cookies.clear();
	NameValueCollection::ConstIterator it = find(SET_COOKIE);
	while (it != end() && Poco::icompare(it->first, SET_COOKIE) == 0)
	{
		NameValueCollection nvc;
		splitParameters(it->second.begin(), it->second.end(), nvc);
		cookies.push_back(HTTPCookie(nvc));
		++it;
	}
}


void HTTPResponse::write(std::ostream& ostr) const
{
	ostr << getVersion() << " " << static_cast<int>(_status) << " " << _reason << "\r\n";
	HTTPMessage::write(ostr);
	ostr << "\r\n";
}


void HTTPResponse::read(std::istream& istr)
{
	static const int eof = std::char_traits<char>::eof();

	std::string version;
	std::string status;
	std::string reason;
	
	int ch =  istr.get();
	if (istr.bad()) throw NetException("Error reading HTTP response header");
	if (ch == eof) throw NoMessageException();
	while (Poco::Ascii::isSpace(ch)) ch = istr.get();
	if (ch == eof) throw MessageException("No HTTP response header");
	while (!Poco::Ascii::isSpace(ch) && ch != eof && version.length() < MAX_VERSION_LENGTH) { version += (char) ch; ch = istr.get(); }
	if (!Poco::Ascii::isSpace(ch)) throw MessageException("Invalid HTTP version string");
	while (Poco::Ascii::isSpace(ch)) ch = istr.get();
	while (!Poco::Ascii::isSpace(ch) && ch != eof && status.length() < MAX_STATUS_LENGTH) { status += (char) ch; ch = istr.get(); }
	if (!Poco::Ascii::isSpace(ch)) throw MessageException("Invalid HTTP status code");
	while (Poco::Ascii::isSpace(ch) && ch != '\r' && ch != '\n' && ch != eof) ch = istr.get();
	while (ch != '\r' && ch != '\n' && ch != eof && reason.length() < MAX_REASON_LENGTH) { reason += (char) ch; ch = istr.get(); }
	if (!Poco::Ascii::isSpace(ch)) throw MessageException("HTTP reason string too long");
	if (ch == '\r') ch = istr.get();

	HTTPMessage::read(istr);
	ch = istr.get();
	while (ch != '\n' && ch != eof) { ch = istr.get(); }
	setVersion(version);
	setStatus(status);
	setReason(reason);
}


const std::string& HTTPResponse::getReasonForStatus(HTTPStatus status)
{
	switch (status)
	{
	case HTTP_CONTINUE: 
		return HTTP_REASON_CONTINUE;
	case HTTP_SWITCHING_PROTOCOLS: 
		return HTTP_REASON_SWITCHING_PROTOCOLS;
	case HTTP_OK: 
		return HTTP_REASON_OK;
	case HTTP_CREATED: 
		return HTTP_REASON_CREATED;
	case HTTP_ACCEPTED: 
		return HTTP_REASON_ACCEPTED;
	case HTTP_NONAUTHORITATIVE:	
		return HTTP_REASON_NONAUTHORITATIVE;
	case HTTP_NO_CONTENT: 
		return HTTP_REASON_NO_CONTENT;
	case HTTP_RESET_CONTENT: 
		return HTTP_REASON_RESET_CONTENT;
	case HTTP_PARTIAL_CONTENT: 
		return HTTP_REASON_PARTIAL_CONTENT;
	case HTTP_MULTIPLE_CHOICES: 
		return HTTP_REASON_MULTIPLE_CHOICES;
	case HTTP_MOVED_PERMANENTLY: 
		return HTTP_REASON_MOVED_PERMANENTLY;
	case HTTP_FOUND: 
		return HTTP_REASON_FOUND;
	case HTTP_SEE_OTHER: 
		return HTTP_REASON_SEE_OTHER;
	case HTTP_NOT_MODIFIED: 
		return HTTP_REASON_NOT_MODIFIED;
	case HTTP_USEPROXY: 
		return HTTP_REASON_USEPROXY;
	case HTTP_TEMPORARY_REDIRECT: 
		return HTTP_REASON_TEMPORARY_REDIRECT;
	case HTTP_BAD_REQUEST: 
		return HTTP_REASON_BAD_REQUEST;
	case HTTP_UNAUTHORIZED: 
		return HTTP_REASON_UNAUTHORIZED;
	case HTTP_PAYMENT_REQUIRED: 
		return HTTP_REASON_PAYMENT_REQUIRED;
	case HTTP_FORBIDDEN: 
		return HTTP_REASON_FORBIDDEN;
	case HTTP_NOT_FOUND: 
		return HTTP_REASON_NOT_FOUND;
	case HTTP_METHOD_NOT_ALLOWED:
		return HTTP_REASON_METHOD_NOT_ALLOWED;
	case HTTP_NOT_ACCEPTABLE: 
		return HTTP_REASON_NOT_ACCEPTABLE;
	case HTTP_PROXY_AUTHENTICATION_REQUIRED: 
		return HTTP_REASON_PROXY_AUTHENTICATION_REQUIRED;
	case HTTP_REQUEST_TIMEOUT: 
		return HTTP_REASON_REQUEST_TIMEOUT;
	case HTTP_CONFLICT: 
		return HTTP_REASON_CONFLICT;
	case HTTP_GONE: 
		return HTTP_REASON_GONE;
	case HTTP_LENGTH_REQUIRED: 
		return HTTP_REASON_LENGTH_REQUIRED;
	case HTTP_PRECONDITION_FAILED: 
		return HTTP_REASON_PRECONDITION_FAILED;
	case HTTP_REQUESTENTITYTOOLARGE: 
		return HTTP_REASON_REQUESTENTITYTOOLARGE;
	case HTTP_REQUESTURITOOLONG: 
		return HTTP_REASON_REQUESTURITOOLONG;
	case HTTP_UNSUPPORTEDMEDIATYPE: 
		return HTTP_REASON_UNSUPPORTEDMEDIATYPE;
	case HTTP_REQUESTED_RANGE_NOT_SATISFIABLE: 
		return HTTP_REASON_REQUESTED_RANGE_NOT_SATISFIABLE;
	case HTTP_EXPECTATION_FAILED: 
		return HTTP_REASON_EXPECTATION_FAILED;
	case HTTP_INTERNAL_SERVER_ERROR: 
		return HTTP_REASON_INTERNAL_SERVER_ERROR;
	case HTTP_NOT_IMPLEMENTED: 
		return HTTP_REASON_NOT_IMPLEMENTED;
	case HTTP_BAD_GATEWAY: 
		return HTTP_REASON_BAD_GATEWAY;
	case HTTP_SERVICE_UNAVAILABLE:
		return HTTP_REASON_SERVICE_UNAVAILABLE;
	case HTTP_GATEWAY_TIMEOUT: 
		return HTTP_REASON_GATEWAY_TIMEOUT;
	case HTTP_VERSION_NOT_SUPPORTED: 
		return HTTP_REASON_VERSION_NOT_SUPPORTED;
	default: 
		return HTTP_REASON_UNKNOWN;
	}
}


} } // namespace Poco::Net
