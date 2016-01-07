//
// HTTPRequest.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/HTTPRequest.h#3 $
//
// Library: Net
// Package: HTTP
// Module:  HTTPRequest
//
// Definition of the HTTPRequest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPRequest_INCLUDED
#define Net_HTTPRequest_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/HTTPMessage.h"


namespace Poco {
namespace Net {


class Net_API HTTPRequest: public HTTPMessage
	/// This class encapsulates an HTTP request
	/// message.
	///
	/// In addition to the properties common to
	/// all HTTP messages, a HTTP request has
	/// a method (e.g. GET, HEAD, POST, etc.) and
	/// a request URI.
{
public:
	HTTPRequest();
		/// Creates a GET / HTTP/1.0 HTTP request.
		
	HTTPRequest(const std::string& version);
		/// Creates a GET / HTTP/1.x request with
		/// the given version (HTTP/1.0 or HTTP/1.1).
		
	HTTPRequest(const std::string& method, const std::string& uri);
		/// Creates a HTTP/1.0 request with the given method and URI.

	HTTPRequest(const std::string& method, const std::string& uri, const std::string& version);
		/// Creates a HTTP request with the given method, URI and version.

	virtual ~HTTPRequest();
		/// Destroys the HTTPRequest.

	void setMethod(const std::string& method);
		/// Sets the method.

	const std::string& getMethod() const;
		/// Returns the method.

	void setURI(const std::string& uri);
		/// Sets the request URI.
		
	const std::string& getURI() const;
		/// Returns the request URI.
		
	void setHost(const std::string& host);
		/// Sets the value of the Host header field.
		
	void setHost(const std::string& host, Poco::UInt16 port);
		/// Sets the value of the Host header field.
		///
		/// If the given port number is a non-standard
		/// port number (other than 80 or 443), it is
		/// included in the Host header field.
		
	const std::string& getHost() const;
		/// Returns the value of the Host header field.
		///
		/// Throws a NotFoundException if the request
		/// does not have a Host header field.

	void setCookies(const NameValueCollection& cookies);
		/// Adds a Cookie header with the names and
		/// values from cookies.
		
	void getCookies(NameValueCollection& cookies) const;
		/// Fills cookies with the cookies extracted
		/// from the Cookie headers in the request.

	bool hasCredentials() const;
		/// Returns true iff the request contains authentication
		/// information in the form of an Authorization header.
		
	void getCredentials(std::string& scheme, std::string& authInfo) const;
		/// Returns the authentication scheme and additional authentication
		/// information contained in this request.
		///
		/// Throws a NotAuthenticatedException if no authentication information
		/// is contained in the request.
		
	void setCredentials(const std::string& scheme, const std::string& authInfo);
		/// Sets the authentication scheme and information for
		/// this request.

	bool hasProxyCredentials() const;
		/// Returns true iff the request contains proxy authentication
		/// information in the form of an Proxy-Authorization header.
		
	void getProxyCredentials(std::string& scheme, std::string& authInfo) const;
		/// Returns the proxy authentication scheme and additional proxy authentication
		/// information contained in this request.
		///
		/// Throws a NotAuthenticatedException if no proxy authentication information
		/// is contained in the request.
		
	void setProxyCredentials(const std::string& scheme, const std::string& authInfo);
		/// Sets the proxy authentication scheme and information for
		/// this request.

	void write(std::ostream& ostr) const;
		/// Writes the HTTP request to the given
		/// output stream.

	void read(std::istream& istr);
		/// Reads the HTTP request from the
		/// given input stream.
		
	static const std::string HTTP_GET;
	static const std::string HTTP_HEAD;
	static const std::string HTTP_PUT;
	static const std::string HTTP_POST;
	static const std::string HTTP_OPTIONS;
	static const std::string HTTP_DELETE;
	static const std::string HTTP_TRACE;
	static const std::string HTTP_CONNECT;
	
	static const std::string HOST;
	static const std::string COOKIE;
	static const std::string AUTHORIZATION;
	static const std::string PROXY_AUTHORIZATION;
	static const std::string UPGRADE;

protected:
	void getCredentials(const std::string& header, std::string& scheme, std::string& authInfo) const;
		/// Returns the authentication scheme and additional authentication
		/// information contained in the given header of request.
		///
		/// Throws a NotAuthenticatedException if no authentication information
		/// is contained in the request.
		
	void setCredentials(const std::string& header, const std::string& scheme, const std::string& authInfo);
		/// Writes the authentication scheme and information for
		/// this request to the given header.

private:
	enum Limits
	{
		MAX_METHOD_LENGTH  = 32,
		MAX_URI_LENGTH     = 16384,
		MAX_VERSION_LENGTH = 8
	};
	
	std::string _method;
	std::string _uri;
	
	HTTPRequest(const HTTPRequest&);
	HTTPRequest& operator = (const HTTPRequest&);
};


//
// inlines
//
inline const std::string& HTTPRequest::getMethod() const
{
	return _method;
}


inline const std::string& HTTPRequest::getURI() const
{
	return _uri;
}


} } // namespace Poco::Net


#endif // Net_HTTPRequest_INCLUDED
