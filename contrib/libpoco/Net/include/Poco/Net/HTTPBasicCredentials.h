//
// HTTPBasicCredentials.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/HTTPBasicCredentials.h#4 $
//
// Library: Net
// Package: HTTP
// Module:  HTTPBasicCredentials
//
// Definition of the HTTPBasicCredentials class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPBasicCredentials_INCLUDED
#define Net_HTTPBasicCredentials_INCLUDED


#include "Poco/Net/Net.h"


namespace Poco {
namespace Net {


class HTTPRequest;


class Net_API HTTPBasicCredentials
	/// This is a utility class for working with
	/// HTTP Basic Authentication in HTTPRequest
	/// objects.
{
public:
	HTTPBasicCredentials();
		/// Creates an empty HTTPBasicCredentials object.
		
	HTTPBasicCredentials(const std::string& username, const std::string& password);
		/// Creates a HTTPBasicCredentials object with the given username and password.

	explicit HTTPBasicCredentials(const HTTPRequest& request);
		/// Creates a HTTPBasicCredentials object with the authentication information
		/// from the given request.
		///
		/// Throws a NotAuthenticatedException if the request does
		/// not contain basic authentication information.

	explicit HTTPBasicCredentials(const std::string& authInfo);
		/// Creates a HTTPBasicCredentials object with the authentication information
		/// in the given string. The authentication information can be extracted
		/// from a HTTPRequest object by calling HTTPRequest::getCredentials().

	~HTTPBasicCredentials();
		/// Destroys the HTTPBasicCredentials.

	void setUsername(const std::string& username);
		/// Sets the username.
		
	const std::string& getUsername() const;
		/// Returns the username.
		
	void setPassword(const std::string& password);
		/// Sets the password.
		
	const std::string& getPassword() const;
		/// Returns the password.
		
	void authenticate(HTTPRequest& request) const;
		/// Adds authentication information to the given HTTPRequest.

	void proxyAuthenticate(HTTPRequest& request) const;
		/// Adds proxy authentication information to the given HTTPRequest.

	static const std::string SCHEME;

protected:
	void parseAuthInfo(const std::string& authInfo);
		/// Extracts username and password from Basic authentication info
		/// by base64-decoding authInfo and splitting the resulting
		/// string at the ':' delimiter.

private:
	HTTPBasicCredentials(const HTTPBasicCredentials&);
	HTTPBasicCredentials& operator = (const HTTPBasicCredentials&);
	
	std::string _username;
	std::string _password;
};


//
// inlines
//
inline const std::string& HTTPBasicCredentials::getUsername() const
{
	return _username;
}


inline const std::string& HTTPBasicCredentials::getPassword() const
{
	return _password;
}


} } // namespace Poco::Net


#endif // Net_HTTPBasicCredentials_INCLUDED
