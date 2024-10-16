//
// HTTPResponse.cpp
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
const std::string HTTPResponse::HTTP_REASON_PROCESSING                      = "Processing";
const std::string HTTPResponse::HTTP_REASON_OK                              = "OK";
const std::string HTTPResponse::HTTP_REASON_CREATED                         = "Created";
const std::string HTTPResponse::HTTP_REASON_ACCEPTED                        = "Accepted";
const std::string HTTPResponse::HTTP_REASON_NONAUTHORITATIVE                = "Non-Authoritative Information";
const std::string HTTPResponse::HTTP_REASON_NO_CONTENT                      = "No Content";
const std::string HTTPResponse::HTTP_REASON_RESET_CONTENT                   = "Reset Content";
const std::string HTTPResponse::HTTP_REASON_PARTIAL_CONTENT                 = "Partial Content";
const std::string HTTPResponse::HTTP_REASON_MULTI_STATUS                    = "Multi Status";
const std::string HTTPResponse::HTTP_REASON_ALREADY_REPORTED                = "Already Reported";
const std::string HTTPResponse::HTTP_REASON_IM_USED                         = "IM Used";
const std::string HTTPResponse::HTTP_REASON_MULTIPLE_CHOICES                = "Multiple Choices";
const std::string HTTPResponse::HTTP_REASON_MOVED_PERMANENTLY               = "Moved Permanently";
const std::string HTTPResponse::HTTP_REASON_FOUND                           = "Found";
const std::string HTTPResponse::HTTP_REASON_SEE_OTHER                       = "See Other";
const std::string HTTPResponse::HTTP_REASON_NOT_MODIFIED                    = "Not Modified";
const std::string HTTPResponse::HTTP_REASON_USE_PROXY                       = "Use Proxy";
const std::string HTTPResponse::HTTP_REASON_TEMPORARY_REDIRECT              = "Temporary Redirect";
const std::string HTTPResponse::HTTP_REASON_PERMANENT_REDIRECT              = "Permanent Redirect";
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
const std::string HTTPResponse::HTTP_REASON_REQUEST_ENTITY_TOO_LARGE        = "Request Entity Too Large";
const std::string HTTPResponse::HTTP_REASON_REQUEST_URI_TOO_LONG            = "Request-URI Too Large";
const std::string HTTPResponse::HTTP_REASON_UNSUPPORTED_MEDIA_TYPE          = "Unsupported Media Type";
const std::string HTTPResponse::HTTP_REASON_REQUESTED_RANGE_NOT_SATISFIABLE = "Requested Range Not Satisfiable";
const std::string HTTPResponse::HTTP_REASON_EXPECTATION_FAILED              = "Expectation Failed";
const std::string HTTPResponse::HTTP_REASON_IM_A_TEAPOT                     = "I'm a Teapot";
const std::string HTTPResponse::HTTP_REASON_ENCHANCE_YOUR_CALM              = "Enchance Your Calm";
const std::string HTTPResponse::HTTP_REASON_MISDIRECTED_REQUEST             = "Misdirected Request";
const std::string HTTPResponse::HTTP_REASON_UNPROCESSABLE_ENTITY            = "Unprocessable Entity";
const std::string HTTPResponse::HTTP_REASON_LOCKED                          = "Locked";
const std::string HTTPResponse::HTTP_REASON_FAILED_DEPENDENCY               = "Failed Dependency";
const std::string HTTPResponse::HTTP_REASON_UPGRADE_REQUIRED                = "Upgrade Required";
const std::string HTTPResponse::HTTP_REASON_PRECONDITION_REQUIRED           = "Precondition Required";
const std::string HTTPResponse::HTTP_REASON_TOO_MANY_REQUESTS               = "Too Many Requests";
const std::string HTTPResponse::HTTP_REASON_REQUEST_HEADER_FIELDS_TOO_LARGE = "Request Header Fields Too Large";
const std::string HTTPResponse::HTTP_REASON_UNAVAILABLE_FOR_LEGAL_REASONS   = "Unavailable For Legal Reasons";
const std::string HTTPResponse::HTTP_REASON_INTERNAL_SERVER_ERROR           = "Internal Server Error";
const std::string HTTPResponse::HTTP_REASON_NOT_IMPLEMENTED                 = "Not Implemented";
const std::string HTTPResponse::HTTP_REASON_BAD_GATEWAY                     = "Bad Gateway";
const std::string HTTPResponse::HTTP_REASON_SERVICE_UNAVAILABLE             = "Service Unavailable";
const std::string HTTPResponse::HTTP_REASON_GATEWAY_TIMEOUT                 = "Gateway Time-Out";
const std::string HTTPResponse::HTTP_REASON_VERSION_NOT_SUPPORTED           = "HTTP Version Not Supported";
const std::string HTTPResponse::HTTP_REASON_VARIANT_ALSO_NEGOTIATES         = "Variant Also Negotiates";
const std::string HTTPResponse::HTTP_REASON_INSUFFICIENT_STORAGE            = "Insufficient Storage";
const std::string HTTPResponse::HTTP_REASON_LOOP_DETECTED                   = "Loop Detected";
const std::string HTTPResponse::HTTP_REASON_NOT_EXTENDED                    = "Not Extended";
const std::string HTTPResponse::HTTP_REASON_NETWORK_AUTHENTICATION_REQUIRED = "Network Authentication Required";
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

void HTTPResponse::getHeaders(std::map<std::string, std::string> & headers) const
{
    headers.clear();
    for (const auto & it : *this)
    {
        headers.emplace(it.first, it.second);
    }
}


void HTTPResponse::write(std::ostream& ostr) const
{
	beginWrite(ostr);
	ostr << "\r\n";
}


void HTTPResponse::beginWrite(std::ostream& ostr) const
{
	ostr << getVersion() << " " << static_cast<int>(_status) << " " << _reason << "\r\n";
	HTTPMessage::write(ostr);
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
	if (ch != '\n') throw MessageException("Unterminated HTTP response line");

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
	case HTTP_PROCESSING:
		return HTTP_REASON_PROCESSING;
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
	case HTTP_MULTI_STATUS:
		return HTTP_REASON_MULTI_STATUS;
	case HTTP_ALREADY_REPORTED:
		return HTTP_REASON_ALREADY_REPORTED;
	case HTTP_IM_USED:
		return HTTP_REASON_IM_USED;
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
	case HTTP_USE_PROXY:
		return HTTP_REASON_USE_PROXY;
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
	case HTTP_REQUEST_ENTITY_TOO_LARGE:
		return HTTP_REASON_REQUEST_ENTITY_TOO_LARGE;
	case HTTP_REQUEST_URI_TOO_LONG:
		return HTTP_REASON_REQUEST_URI_TOO_LONG;
	case HTTP_UNSUPPORTED_MEDIA_TYPE:
		return HTTP_REASON_UNSUPPORTED_MEDIA_TYPE;
	case HTTP_REQUESTED_RANGE_NOT_SATISFIABLE:
		return HTTP_REASON_REQUESTED_RANGE_NOT_SATISFIABLE;
	case HTTP_EXPECTATION_FAILED:
		return HTTP_REASON_EXPECTATION_FAILED;
	case HTTP_IM_A_TEAPOT:
		return HTTP_REASON_IM_A_TEAPOT;
	case HTTP_ENCHANCE_YOUR_CALM:
		return HTTP_REASON_ENCHANCE_YOUR_CALM;
	case HTTP_MISDIRECTED_REQUEST:
		return HTTP_REASON_MISDIRECTED_REQUEST;
	case HTTP_UNPROCESSABLE_ENTITY:
		return HTTP_REASON_UNPROCESSABLE_ENTITY;
	case HTTP_LOCKED:
		return HTTP_REASON_LOCKED;
	case HTTP_FAILED_DEPENDENCY:
		return HTTP_REASON_FAILED_DEPENDENCY;
	case HTTP_UPGRADE_REQUIRED:
		return HTTP_REASON_UPGRADE_REQUIRED;
	case HTTP_PRECONDITION_REQUIRED:
		return HTTP_REASON_PRECONDITION_REQUIRED;
	case HTTP_TOO_MANY_REQUESTS:
		return HTTP_REASON_TOO_MANY_REQUESTS;
	case HTTP_REQUEST_HEADER_FIELDS_TOO_LARGE:
		return HTTP_REASON_REQUEST_HEADER_FIELDS_TOO_LARGE;
	case HTTP_UNAVAILABLE_FOR_LEGAL_REASONS:
		return HTTP_REASON_UNAVAILABLE_FOR_LEGAL_REASONS;
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
	case HTTP_VARIANT_ALSO_NEGOTIATES:
		return HTTP_REASON_VARIANT_ALSO_NEGOTIATES;
	case HTTP_INSUFFICIENT_STORAGE:
		return HTTP_REASON_INSUFFICIENT_STORAGE;
	case HTTP_LOOP_DETECTED:
		return HTTP_REASON_LOOP_DETECTED;
	case HTTP_NOT_EXTENDED:
		return HTTP_REASON_NOT_EXTENDED;
	case HTTP_NETWORK_AUTHENTICATION_REQUIRED:
		return HTTP_REASON_NETWORK_AUTHENTICATION_REQUIRED;
	default:
		return HTTP_REASON_UNKNOWN;
	}
}


} } // namespace Poco::Net
