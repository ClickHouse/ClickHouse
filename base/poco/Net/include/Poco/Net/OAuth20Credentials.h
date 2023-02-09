//
// OAuth20Credentials.h
//
// Library: Net
// Package: OAuth
// Module:	OAuth20Credentials
//
// Definition of the OAuth20Credentials class.
//
// Copyright (c) 2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_OAuth20Credentials_INCLUDED
#define Net_OAuth20Credentials_INCLUDED


#include "Poco/Net/Net.h"


namespace Poco {
namespace Net {


class HTTPRequest;


class Net_API OAuth20Credentials
	/// This class implements OAuth 2.0 authentication for HTTP requests,
	/// via Bearer tokens in the Authorization header, 
	/// according to RFC 6749 and RFC 6750.
	///
	/// To add an Authorization header containing a bearer token
	/// to a HTTPRequest object, create an OAuth20Credentials object
	/// with the bearer token and call authenticate().
	///
	/// The bearer token can also be extracted from a HTTPRequest
	/// by creating the OAuth20Credentials object with a HTTPRequest
	/// object containing a "Bearer" Authorization header and
	/// calling getBearerToken().
	///
	/// The authorization header scheme can be changed from 
	/// "Bearer" to a custom value. For example, GitHub uses
	/// the "token" scheme.
{
public:
	OAuth20Credentials();
		/// Creates an empty OAuth20Credentials object.

	explicit OAuth20Credentials(const std::string& bearerToken);
		/// Creates an OAuth20Credentials object with the given bearer token.

	OAuth20Credentials(const std::string& bearerToken, const std::string& scheme);
		/// Creates an OAuth20Credentials object with the given bearer token
		/// and authorization scheme, which overrides the default scheme ("Bearer").
		///
		/// This is useful for services like GitHub, which use "token" as scheme.

	explicit OAuth20Credentials(const HTTPRequest& request);
		/// Creates an OAuth20Credentials object from a HTTPRequest object.
		///
		/// Extracts bearer token from the Authorization header, which
		/// must use the "Bearer" authorization scheme.
		///
		/// Throws a NotAuthenticatedException if the request does
		/// not contain a bearer token in the Authorization header.

	OAuth20Credentials(const HTTPRequest& request, const std::string& scheme);
		/// Creates an OAuth20Credentials object from a HTTPRequest object.
		///
		/// Extracts bearer token from the Authorization header, which must
		/// use the given authorization scheme.
		///
		/// Throws a NotAuthenticatedException if the request does
		/// not contain a bearer token in the Authorization header.

	~OAuth20Credentials();
		/// Destroys the HTTPCredentials.

	void setBearerToken(const std::string& bearerToken);
		/// Sets the bearer token.
		
	const std::string& getBearerToken() const;
		/// Returns the bearer token.

	void setScheme(const std::string& scheme);
		/// Sets the Authorization header scheme.
		
	const std::string& getScheme() const;
		/// Returns the Authorization header scheme.
		
	void authenticate(HTTPRequest& request);
		/// Adds an Authorization header containing the bearer token to
		/// the HTTPRequest.

	static const std::string SCHEME;

protected:
	void extractBearerToken(const HTTPRequest& request);
		/// Extracts the bearer token from the HTTPRequest.
		
private:
	OAuth20Credentials(const OAuth20Credentials&);
	OAuth20Credentials& operator = (const OAuth20Credentials&);
	
	std::string _bearerToken;
	std::string _scheme;
};


//
// inlines
//
inline const std::string& OAuth20Credentials::getBearerToken() const
{
	return _bearerToken;
}


inline const std::string& OAuth20Credentials::getScheme() const
{
	return _scheme;
}


} } // namespace Poco::Net


#endif // Net_OAuth20Credentials_INCLUDED
