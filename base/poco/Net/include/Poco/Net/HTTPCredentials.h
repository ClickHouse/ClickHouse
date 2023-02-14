//
// HTTPCredentials.h
//
// Library: Net
// Package: HTTP
// Module:	HTTPCredentials
//
// Definition of the HTTPCredentials class.
//
// Copyright (c) 2011, Anton V. Yabchinskiy (arn at bestmx dot ru).
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPCredentials_INCLUDED
#define Net_HTTPCredentials_INCLUDED


#include "Poco/Net/HTTPDigestCredentials.h"


namespace Poco {


class URI;


namespace Net {


class HTTPRequest;
class HTTPResponse;


class Net_API HTTPCredentials
	/// This is a utility class for working with HTTP
	/// authentication (Basic or Digest) in HTTPRequest objects.
	///
	/// Usage is as follows:
	/// First, create a HTTPCredentials object containing
	/// the username and password.
	///     Poco::Net::HTTPCredentials creds("user", "s3cr3t");
	///
	/// Second, send the HTTP request with Poco::Net::HTTPClientSession.
	///     Poco::Net::HTTPClientSession session("pocoproject.org");
	///     Poco::Net::HTTPRequest request(HTTPRequest::HTTP_GET, "/index.html", HTTPMessage::HTTP_1_1);
	///     session.sendRequest(request);
	///     Poco::Net::HTTPResponse;
	///     std::istream& istr = session.receiveResponse(response);
	///
	/// If the server responds with a 401 status, authenticate the
	/// request and resend it:
	///     if (response.getStatus() == Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED)
	///     {
	///         creds.authenticate(request, response);
	///         session.sendRequest(request);
	///         ...
	///     }
	///
	/// To perform multiple authenticated requests, call updateAuthInfo()
	/// instead of authenticate() on subsequent requests.
	///     creds.updateAuthInfo(request);
	///     session.sendRequest(request);
	///     ...
	///
	/// Note: Do not forget to read the entire response stream from the 401 response
	/// before sending the authenticated request, otherwise there may be
	/// problems if a persistent connection is used.
{
public:
	HTTPCredentials();
		/// Creates an empty HTTPCredentials object.

	HTTPCredentials(const std::string& username, const std::string& password);
		/// Creates an HTTPCredentials object with the given username and password.

	~HTTPCredentials();
		/// Destroys the HTTPCredentials.

	void fromUserInfo(const std::string& userInfo);
		/// Parses username:password string and sets username and password of
		/// the credentials object.
		/// Throws SyntaxException on invalid user information.

	void fromURI(const URI& uri);
		/// Extracts username and password from the given URI and sets username
		/// and password of the credentials object.
		/// Does nothing if URI has no user info part.

	void clear();
		/// Clears username, password and host.

	void setUsername(const std::string& username);
		/// Sets the username.

	const std::string& getUsername() const;
		/// Returns the username.

	void setPassword(const std::string& password);
		/// Sets the password.

	const std::string& getPassword() const;
		/// Returns the password.

	bool empty() const;
		/// Returns true if both username and password are empty, otherwise false.

	void authenticate(HTTPRequest& request, const HTTPResponse& response);
		/// Inspects WWW-Authenticate header of the response, initializes
		/// the internal state (in case of digest authentication) and
		/// adds required information to the given HTTPRequest.
		///
		/// Does nothing if there is no WWW-Authenticate header in the
		/// HTTPResponse.

	void updateAuthInfo(HTTPRequest& request);
		/// Updates internal state (in case of digest authentication) and
		/// replaces authentication information in the request accordingly.

	void proxyAuthenticate(HTTPRequest& request, const HTTPResponse& response);
		/// Inspects Proxy-Authenticate header of the response, initializes
		/// the internal state (in case of digest authentication) and
		/// adds required information to the given HTTPRequest.
		///
		/// Does nothing if there is no Proxy-Authenticate header in the
		/// HTTPResponse.

	void updateProxyAuthInfo(HTTPRequest& request);
		/// Updates internal state (in case of digest authentication) and
		/// replaces proxy authentication information in the request accordingly.

	static bool isBasicCredentials(const std::string& header);
		/// Returns true if authentication header is for Basic authentication.

	static bool isDigestCredentials(const std::string& header);
		/// Returns true if authentication header is for Digest authentication.

	static bool hasBasicCredentials(const HTTPRequest& request);
		/// Returns true if an Authorization header with Basic credentials is present in the request.

	static bool hasDigestCredentials(const HTTPRequest& request);
		/// Returns true if an Authorization header with Digest credentials is present in the request.

	static bool hasNTLMCredentials(const HTTPRequest& request);
		/// Returns true if an Authorization header with NTLM credentials is present in the request.

	static bool hasProxyBasicCredentials(const HTTPRequest& request);
		/// Returns true if a Proxy-Authorization header with Basic credentials is present in the request.

	static bool hasProxyDigestCredentials(const HTTPRequest& request);
		/// Returns true if a Proxy-Authorization header with Digest credentials is present in the request.

	static void extractCredentials(const std::string& userInfo, std::string& username, std::string& password);
		/// Extracts username and password from user:password information string.

	static void extractCredentials(const Poco::URI& uri, std::string& username, std::string& password);
		/// Extracts username and password from the given URI (e.g.: "http://user:pass@sample.com/secret").

private:
	HTTPCredentials(const HTTPCredentials&);
	HTTPCredentials& operator = (const HTTPCredentials&);

	HTTPDigestCredentials _digest;
};


//
// inlines
//
inline void HTTPCredentials::setUsername(const std::string& username)
{
	_digest.setUsername(username);
}


inline const std::string& HTTPCredentials::getUsername() const
{
	return _digest.getUsername();
}


inline void HTTPCredentials::setPassword(const std::string& password)
{
	_digest.setPassword(password);
}


inline const std::string& HTTPCredentials::getPassword() const
{
	return _digest.getPassword();
}


inline bool HTTPCredentials::empty() const
{
	return _digest.empty();
}


} } // namespace Poco::Net


#endif // Net_HTTPCredentials_INCLUDED
