//
// HTTPDigestCredentials.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/HTTPDigestCredentials.h#3 $
//
// Library: Net
// Package: HTTP
// Module:	HTTPDigestCredentials
//
// Definition of the HTTPDigestCredentials class.
//
// Copyright (c) 2011, Anton V. Yabchinskiy (arn at bestmx dot ru).
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPDigestCredentials_INCLUDED
#define Net_HTTPDigestCredentials_INCLUDED


#include "Poco/Net/HTTPAuthenticationParams.h"
#include "Poco/Mutex.h"
#include <map>


namespace Poco {
namespace Net {


class HTTPRequest;
class HTTPResponse;


class Net_API HTTPDigestCredentials
	/// This is a utility class for working with
	/// HTTP Digest Authentication in HTTPRequest
	/// objects.
	///
	/// Note: currently, no qop or qop=auth is
	/// supported only.
{
public:
	HTTPDigestCredentials();
		/// Creates an empty HTTPDigestCredentials object.

	HTTPDigestCredentials(const std::string& username, const std::string& password);
		/// Creates a HTTPDigestCredentials object with the given username and password.

	~HTTPDigestCredentials();
		/// Destroys the HTTPDigestCredentials.

	void reset();
		/// Resets the HTTPDigestCredentials object to a clean state.

	void setUsername(const std::string& username);
		/// Sets the username.

	const std::string& getUsername() const;
		/// Returns the username.

	void setPassword(const std::string& password);
		/// Sets the password.

	const std::string& getPassword() const;
		/// Returns the password.

	void authenticate(HTTPRequest& request, const HTTPResponse& response);
		/// Parses WWW-Authenticate header of the HTTPResponse, initializes
		/// internal state, and adds authentication information to the given HTTPRequest.

	void authenticate(HTTPRequest& request, const HTTPAuthenticationParams& responseAuthParams);
		/// Initializes internal state according to information from the
		/// HTTPAuthenticationParams of the response, and adds authentication
		/// information to the given HTTPRequest.
		///
		/// Throws InvalidArgumentException if HTTPAuthenticationParams is
		/// invalid or some required parameter is missing.
		/// Throws NotImplementedException in case of unsupported digest
		/// algorithm or quality of protection method.

	void updateAuthInfo(HTTPRequest& request);
		/// Updates internal state and adds authentication information to
		/// the given HTTPRequest.

	void proxyAuthenticate(HTTPRequest& request, const HTTPResponse& response);
		/// Parses Proxy-Authenticate header of the HTTPResponse, initializes
		/// internal state, and adds proxy authentication information to the given HTTPRequest.

	void proxyAuthenticate(HTTPRequest& request, const HTTPAuthenticationParams& responseAuthParams);
		/// Initializes internal state according to information from the
		/// HTTPAuthenticationParams of the response, and adds proxy authentication
		/// information to the given HTTPRequest.
		///
		/// Throws InvalidArgumentException if HTTPAuthenticationParams is
		/// invalid or some required parameter is missing.
		/// Throws NotImplementedException in case of unsupported digest
		/// algorithm or quality of protection method.

	void updateProxyAuthInfo(HTTPRequest& request);
		/// Updates internal state and adds proxy authentication information to
		/// the given HTTPRequest.

	bool verifyAuthInfo(const HTTPRequest& request) const;
		/// Verifies the digest authentication information in the given HTTPRequest
		/// by recomputing the response and comparing it with what's in the request.
		///
		/// Note: This method creates a HTTPAuthenticationParams object from the request
		/// and calls verifyAuthParams() with request and params.

	bool verifyAuthParams(const HTTPRequest& request, const HTTPAuthenticationParams& params) const;
		/// Verifies the digest authentication information in the given HTTPRequest
		/// and HTTPAuthenticationParams by recomputing the response and comparing 
		/// it with what's in the request.

	static std::string createNonce();
		/// Creates a random nonce string.

	static const std::string SCHEME;

private:
	HTTPDigestCredentials(const HTTPDigestCredentials&);
	HTTPDigestCredentials& operator = (const HTTPDigestCredentials&);

	void createAuthParams(const HTTPRequest& request, const HTTPAuthenticationParams& responseAuthParams);
	void updateAuthParams(const HTTPRequest& request);
	int updateNonceCounter(const std::string& nonce);
	
	static const std::string DEFAULT_ALGORITHM;
	static const std::string DEFAULT_QOP;
	static const std::string NONCE_PARAM;
	static const std::string REALM_PARAM;
	static const std::string QOP_PARAM;
	static const std::string ALGORITHM_PARAM;
	static const std::string USERNAME_PARAM;
	static const std::string OPAQUE_PARAM;
	static const std::string URI_PARAM;
	static const std::string RESPONSE_PARAM;
	static const std::string AUTH_PARAM;
	static const std::string CNONCE_PARAM;
	static const std::string NC_PARAM;

	typedef std::map<std::string, int> NonceCounterMap;

	std::string _username;
	std::string _password;
	HTTPAuthenticationParams _requestAuthParams;
	NonceCounterMap _nc;
	
	static int _nonceCounter;
	static Poco::FastMutex _nonceMutex;
};


//
// inlines
//
inline const std::string& HTTPDigestCredentials::getUsername() const
{
	return _username;
}


inline const std::string& HTTPDigestCredentials::getPassword() const
{
	return _password;
}


} } // namespace Poco::Net


#endif // Net_HTTPDigestCredentials_INCLUDED
