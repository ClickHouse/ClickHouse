//
// HTTPAuthenticationParams.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/HTTPAuthenticationParams.h#2 $
//
// Library: Net
// Package: HTTP
// Module:  HTTPAuthenticationParams
//
// Definition of the HTTPAuthenticationParams class.
//
// Copyright (c) 2011, Anton V. Yabchinskiy (arn at bestmx dot ru).
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPAuthenticationParams_INCLUDED
#define Net_HTTPAuthenticationParams_INCLUDED


#include "Poco/Net/NameValueCollection.h"


namespace Poco {
namespace Net {


class HTTPRequest;
class HTTPResponse;


class Net_API HTTPAuthenticationParams: public NameValueCollection
	/// Collection of name-value pairs of HTTP authentication header (i.e.
	/// "realm", "qop", "nonce" in case of digest authentication header).
{
public:
	HTTPAuthenticationParams();
		/// Creates an empty authentication parameters collection.

	explicit HTTPAuthenticationParams(const std::string& authInfo);
		/// See fromAuthInfo() documentation.

	explicit HTTPAuthenticationParams(const HTTPRequest& request);
		/// See fromRequest() documentation.

	HTTPAuthenticationParams(const HTTPResponse& response, const std::string& header = WWW_AUTHENTICATE);
		/// See fromResponse() documentation.

	virtual ~HTTPAuthenticationParams();
		/// Destroys the HTTPAuthenticationParams.

	HTTPAuthenticationParams& operator = (const HTTPAuthenticationParams& authParams);
		/// Assigns the content of another HTTPAuthenticationParams.

	void fromAuthInfo(const std::string& authInfo);
		/// Creates an HTTPAuthenticationParams by parsing authentication
		/// information.

	void fromRequest(const HTTPRequest& request);
		/// Extracts authentication information from the request and creates
		/// HTTPAuthenticationParams by parsing it.
		///
		/// Throws a NotAuthenticatedException if no authentication
		/// information is contained in request.
		/// Throws a InvalidArgumentException if authentication scheme is
		/// unknown or invalid.

	void fromResponse(const HTTPResponse& response, const std::string& header = WWW_AUTHENTICATE);
		/// Extracts authentication information from the response and creates
		/// HTTPAuthenticationParams by parsing it.
		///
		/// Throws a NotAuthenticatedException if no authentication
		/// information is contained in response.
		/// Throws a InvalidArgumentException if authentication scheme is
		/// unknown or invalid.

	void setRealm(const std::string& realm);
		/// Sets the "realm" parameter to the provided string.

	const std::string& getRealm() const;
		/// Returns value of the "realm" parameter.
		///
		/// Throws NotFoundException is there is no "realm" set in the
		/// HTTPAuthenticationParams.

	std::string toString() const;
		/// Formats the HTTPAuthenticationParams for inclusion in HTTP
		/// request or response authentication header.

	static const std::string REALM;
	static const std::string WWW_AUTHENTICATE;
	static const std::string PROXY_AUTHENTICATE;

private:
	void parse(std::string::const_iterator first, std::string::const_iterator last);
};


} } // namespace Poco::Net


#endif // Net_HTTPAuthenticationParams_INCLUDED
