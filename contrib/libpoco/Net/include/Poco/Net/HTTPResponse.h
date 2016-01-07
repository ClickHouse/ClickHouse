//
// HTTPResponse.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/HTTPResponse.h#1 $
//
// Library: Net
// Package: HTTP
// Module:  HTTPResponse
//
// Definition of the HTTPResponse class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPResponse_INCLUDED
#define Net_HTTPResponse_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/HTTPMessage.h"
#include "Poco/Net/HTTPCookie.h"
#include "Poco/Timestamp.h"
#include <vector>


namespace Poco {
namespace Net {


class HTTPCookie;


class Net_API HTTPResponse: public HTTPMessage
	/// This class encapsulates an HTTP response
	/// message.
	///
	/// In addition to the properties common to
	/// all HTTP messages, a HTTP response has
	/// status code and a reason phrase.
{
public:
	enum HTTPStatus
	{
		HTTP_CONTINUE                        = 100,
		HTTP_SWITCHING_PROTOCOLS             = 101,
		HTTP_OK                              = 200,
		HTTP_CREATED                         = 201,
		HTTP_ACCEPTED                        = 202,
		HTTP_NONAUTHORITATIVE                = 203,
		HTTP_NO_CONTENT                      = 204,
		HTTP_RESET_CONTENT                   = 205,
		HTTP_PARTIAL_CONTENT                 = 206,
		HTTP_MULTIPLE_CHOICES                = 300,
		HTTP_MOVED_PERMANENTLY               = 301,
		HTTP_FOUND                           = 302,
		HTTP_SEE_OTHER                       = 303,
		HTTP_NOT_MODIFIED                    = 304,
		HTTP_USEPROXY                        = 305,
		// UNUSED: 306
		HTTP_TEMPORARY_REDIRECT              = 307,
		HTTP_BAD_REQUEST                     = 400,
		HTTP_UNAUTHORIZED                    = 401,
		HTTP_PAYMENT_REQUIRED                = 402,
		HTTP_FORBIDDEN                       = 403,
		HTTP_NOT_FOUND                       = 404,
		HTTP_METHOD_NOT_ALLOWED              = 405,
		HTTP_NOT_ACCEPTABLE                  = 406,
		HTTP_PROXY_AUTHENTICATION_REQUIRED   = 407,
		HTTP_REQUEST_TIMEOUT                 = 408,
		HTTP_CONFLICT                        = 409,
		HTTP_GONE                            = 410,
		HTTP_LENGTH_REQUIRED                 = 411,
		HTTP_PRECONDITION_FAILED             = 412,
		HTTP_REQUESTENTITYTOOLARGE           = 413,
		HTTP_REQUESTURITOOLONG               = 414,
		HTTP_UNSUPPORTEDMEDIATYPE            = 415,
		HTTP_REQUESTED_RANGE_NOT_SATISFIABLE = 416,
		HTTP_EXPECTATION_FAILED              = 417,
		HTTP_INTERNAL_SERVER_ERROR           = 500,
		HTTP_NOT_IMPLEMENTED                 = 501,
		HTTP_BAD_GATEWAY                     = 502,
		HTTP_SERVICE_UNAVAILABLE             = 503,
		HTTP_GATEWAY_TIMEOUT                 = 504,
		HTTP_VERSION_NOT_SUPPORTED           = 505
	};

	HTTPResponse();
		/// Creates the HTTPResponse with OK status.
		
	HTTPResponse(HTTPStatus status, const std::string& reason);
		/// Creates the HTTPResponse with the given status
		/// and reason phrase.

	HTTPResponse(const std::string& version, HTTPStatus status, const std::string& reason);
		/// Creates the HTTPResponse with the given version, status
		/// and reason phrase.
		
	HTTPResponse(HTTPStatus status);
		/// Creates the HTTPResponse with the given status
		/// an an appropriate reason phrase.

	HTTPResponse(const std::string& version, HTTPStatus status);
		/// Creates the HTTPResponse with the given version, status
		/// an an appropriate reason phrase.

	virtual ~HTTPResponse();
		/// Destroys the HTTPResponse.

	void setStatus(HTTPStatus status);
		/// Sets the HTTP status code.
		///
		/// Does not change the reason phrase.
		
	HTTPStatus getStatus() const;
		/// Returns the HTTP status code.
		
	void setStatus(const std::string& status);
		/// Sets the HTTP status code.
		///
		/// The string must contain a valid
		/// HTTP numerical status code.
		
	void setReason(const std::string& reason);
		/// Sets the HTTP reason phrase.
		
	const std::string& getReason() const;
		/// Returns the HTTP reason phrase.

	void setStatusAndReason(HTTPStatus status, const std::string& reason);
		/// Sets the HTTP status code and reason phrase.
		
	void setStatusAndReason(HTTPStatus status);
		/// Sets the HTTP status code and reason phrase.
		///
		/// The reason phrase is set according to the status code.

	void setDate(const Poco::Timestamp& dateTime);
		/// Sets the Date header to the given date/time value.
		
	Poco::Timestamp getDate() const;
		/// Returns the value of the Date header.

	void addCookie(const HTTPCookie& cookie);
		/// Adds the cookie to the response by
		/// adding a Set-Cookie header.

	void getCookies(std::vector<HTTPCookie>& cookies) const;
		/// Returns a vector with all the cookies
		/// set in the response header.
		///
		/// May throw an exception in case of a malformed
		/// Set-Cookie header.

	void write(std::ostream& ostr) const;
		/// Writes the HTTP response to the given
		/// output stream.

	void read(std::istream& istr);
		/// Reads the HTTP response from the
		/// given input stream.
		///
		/// 100 Continue responses are ignored.
	
	static const std::string& getReasonForStatus(HTTPStatus status);
		/// Returns an appropriate reason phrase
		/// for the given status code.

	static const std::string HTTP_REASON_CONTINUE;
	static const std::string HTTP_REASON_SWITCHING_PROTOCOLS;
	static const std::string HTTP_REASON_OK;
	static const std::string HTTP_REASON_CREATED;
	static const std::string HTTP_REASON_ACCEPTED;
	static const std::string HTTP_REASON_NONAUTHORITATIVE;
	static const std::string HTTP_REASON_NO_CONTENT;
	static const std::string HTTP_REASON_RESET_CONTENT;
	static const std::string HTTP_REASON_PARTIAL_CONTENT;
	static const std::string HTTP_REASON_MULTIPLE_CHOICES;
	static const std::string HTTP_REASON_MOVED_PERMANENTLY;
	static const std::string HTTP_REASON_FOUND;
	static const std::string HTTP_REASON_SEE_OTHER;
	static const std::string HTTP_REASON_NOT_MODIFIED;
	static const std::string HTTP_REASON_USEPROXY;
	static const std::string HTTP_REASON_TEMPORARY_REDIRECT;
	static const std::string HTTP_REASON_BAD_REQUEST;
	static const std::string HTTP_REASON_UNAUTHORIZED;
	static const std::string HTTP_REASON_PAYMENT_REQUIRED;
	static const std::string HTTP_REASON_FORBIDDEN;
	static const std::string HTTP_REASON_NOT_FOUND;
	static const std::string HTTP_REASON_METHOD_NOT_ALLOWED;
	static const std::string HTTP_REASON_NOT_ACCEPTABLE;
	static const std::string HTTP_REASON_PROXY_AUTHENTICATION_REQUIRED;
	static const std::string HTTP_REASON_REQUEST_TIMEOUT;
	static const std::string HTTP_REASON_CONFLICT;
	static const std::string HTTP_REASON_GONE;
	static const std::string HTTP_REASON_LENGTH_REQUIRED;
	static const std::string HTTP_REASON_PRECONDITION_FAILED;
	static const std::string HTTP_REASON_REQUESTENTITYTOOLARGE;
	static const std::string HTTP_REASON_REQUESTURITOOLONG;
	static const std::string HTTP_REASON_UNSUPPORTEDMEDIATYPE;
	static const std::string HTTP_REASON_REQUESTED_RANGE_NOT_SATISFIABLE;
	static const std::string HTTP_REASON_EXPECTATION_FAILED;
	static const std::string HTTP_REASON_INTERNAL_SERVER_ERROR;
	static const std::string HTTP_REASON_NOT_IMPLEMENTED;
	static const std::string HTTP_REASON_BAD_GATEWAY;
	static const std::string HTTP_REASON_SERVICE_UNAVAILABLE;
	static const std::string HTTP_REASON_GATEWAY_TIMEOUT;
	static const std::string HTTP_REASON_VERSION_NOT_SUPPORTED;
	static const std::string HTTP_REASON_UNKNOWN;
	
	static const std::string DATE;
	static const std::string SET_COOKIE;

private:
	enum Limits
	{
		MAX_VERSION_LENGTH = 8,
		MAX_STATUS_LENGTH  = 3,
		MAX_REASON_LENGTH  = 512
	};
	
	HTTPStatus  _status;
	std::string _reason;
	
	HTTPResponse(const HTTPResponse&);
	HTTPResponse& operator = (const HTTPResponse&);
};


//
// inlines
//
inline HTTPResponse::HTTPStatus HTTPResponse::getStatus() const
{
	return _status;
}


inline const std::string& HTTPResponse::getReason() const
{
	return _reason;
}


} } // namespace Poco::Net


#endif // Net_HTTPResponse_INCLUDED
