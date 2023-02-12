//
// OAuth10Credentials.h
//
// Library: Net
// Package: OAuth
// Module:	OAuth10Credentials
//
// Definition of the OAuth10Credentials class.
//
// Copyright (c) 2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_OAuth10Credentials_INCLUDED
#define Net_OAuth10Credentials_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/URI.h"


namespace Poco {
namespace Net {


class HTTPRequest;
class HTMLForm;


class Net_API OAuth10Credentials
	/// This class implements OAuth 1.0A authentication for HTTP requests,
	/// according to RFC 5849.
	/// 
	/// Only PLAINTEXT and HMAC-SHA1 signature methods are
	/// supported. The RSA-SHA1 signature method is not supported.
	///
	/// The OAuth10Credentials can be used to sign a client request, as
	/// well as to verify the signature of a request on the server.
	///
	/// To sign a client request, using a known consumer (client) key, consumer (client) secret,
	/// OAuth token and token secret:
	///
	///   1. Create an OAuth10Credentials object using all four credentials, either using
	///      the four argument constructor, or by using the default constructor and setting
	///      the credentials using the setter methods.
	///   2. Create a URI containing the full request URI.
	///   3. Create an appropriate HTTPRequest object.
	///   4. Optionally, create a HTMLForm object containing additional parameters to sign.
	///   5. Sign the request by calling authenticate(). This will add an OAuth
	///      Authorization header to the request.
	///   6. Send the request using a HTTPClientSession.
	///
	/// To request the OAuth request token from a server, using only the consumer (client) key
	/// and consumer (client) secret:
	///
	///   1. Create an OAuth10Credentials object using the two consumer credentials, either using
	///      the two argument constructor, or by using the default constructor and setting
	///      the credentials using the setter methods.
	///   2. Specify the callback URI using setCallback().
	///   3. Create a URI containing the full request URI to obtain the token.
	///   4. Create an appropriate HTTPRequest object.
	///   5. Sign the request by calling authenticate(). This will add an OAuth
	///      Authorization header to the request.
	///   6. Send the request using a HTTPClientSession.
	///   7. The response will contain the request token and request token secret.
	///      These can be extracted from the response body using a HTMLForm object.
	///
	/// Requesting the access token and secret (temporary credentials) from the server 
	/// is analogous to signing a client request using consumer key, consumer secret, 
	/// request token and request token secret. 
	/// The server response will contain the access token and access token secret,
	/// which can again be extracted from the response body using a HTMLForm object.
	///
	/// To verify a request on the server:
	///
	///   1. Create an OAuth10Credentials object using the constructor taking a
	///      HTTPRequest object. This will extract the consumer key and token (if
	///      provided).
	///   2. Provide the consumer secret and token secret (if required) matching the
	///      consumer key and token to the OAuth10Credentials object using the 
	///      setter methods.
	///   3. Create an URI object containing the full request URI.
	///   4. Call verify() to verify the signature.
	///   5. If verification was successful, and the request was a request for
	///      a request (temporary) token, call getCallback() to
	///      obtain the callback URI provided by the client.
{
public:
	enum SignatureMethod
		/// OAuth 1.0A Signature Method.
	{
		SIGN_PLAINTEXT, /// OAuth 1.0A PLAINTEXT signature method
		SIGN_HMAC_SHA1  /// OAuth 1.0A HMAC-SHA1 signature method
	};
	
	OAuth10Credentials();
		/// Creates an empty OAuth10Credentials object.

	OAuth10Credentials(const std::string& consumerKey, const std::string& consumerSecret);
		/// Creates an OAuth10Credentials object with the given consumer key and consumer secret.
		///
		/// The token and tokenSecret will be left empty.

	OAuth10Credentials(const std::string& consumerKey, const std::string& consumerSecret, const std::string& token, const std::string& tokenSecret);
		/// Creates an OAuth10Credentials object with the given consumer key and 
		/// consumer secret, as well as token and token secret.

	explicit OAuth10Credentials(const HTTPRequest& request);
		/// Creates an OAuth10Credentials object from a HTTPRequest object.
		///
		/// Extracts consumer key and token (if available) from the Authorization header.
		///
		/// Throws a NotAuthenticatedException if the request does
		/// not contain OAuth 1.0 credentials.

	~OAuth10Credentials();
		/// Destroys the OAuth10Credentials.

	void setConsumerKey(const std::string& consumerKey);
		/// Sets the consumer key.
		
	const std::string& getConsumerKey() const;
		/// Returns the consumer key.

	void setConsumerSecret(const std::string& consumerSecret);
		/// Sets the consumer secret.
		
	const std::string& getConsumerSecret() const;
		/// Returns the consumer secret.
		
	void setToken(const std::string& token);
		/// Sets the token.
		
	const std::string& getToken() const;
		/// Returns the token.

	void setTokenSecret(const std::string& tokenSecret);
		/// Sets the token.
		
	const std::string& getTokenSecret() const;
		/// Returns the token secret.
		
	void setRealm(const std::string& realm);
		/// Sets the optional realm to be included in the Authorization header.
		
	const std::string& getRealm() const;
		/// Returns the optional realm to be included in the Authorization header.
		
	void setCallback(const std::string& uri);
		/// Sets the callback URI.
		
	const std::string& getCallback() const;
		/// Returns the callback URI.
				
	void authenticate(HTTPRequest& request, const Poco::URI& uri, SignatureMethod method = SIGN_HMAC_SHA1);
		/// Adds an OAuth 1.0A Authentication header to the given request, using
		/// the given signature method.
		
	void authenticate(HTTPRequest& request, const Poco::URI& uri, const Poco::Net::HTMLForm& params, SignatureMethod method = SIGN_HMAC_SHA1);
		/// Adds an OAuth 1.0A Authentication header to the given request, using
		/// the given signature method.

	bool verify(const HTTPRequest& request, const Poco::URI& uri);
		/// Verifies the signature of the given request. 
		///
		/// The consumer key, consumer secret, token and token secret must have been set.
		///
		/// Returns true if the signature is valid, otherwise false.
		///
		/// Throws a NotAuthenticatedException if the request does not contain OAuth
		/// credentials, or in case of an unsupported OAuth version or signature method.
		
	bool verify(const HTTPRequest& request, const Poco::URI& uri, const Poco::Net::HTMLForm& params);
		/// Verifies the signature of the given request. 
		///
		/// The consumer key, consumer secret, token and token secret must have been set.
		///
		/// Returns true if the signature is valid, otherwise false.
		///
		/// Throws a NotAuthenticatedException if the request does not contain OAuth
		/// credentials, or in case of an unsupported OAuth version or signature method.

	void nonceAndTimestampForTesting(const std::string& nonce, const std::string& timestamp);
		/// Sets the nonce and timestamp to a wellknown value.
		///
		/// For use by testsuite only, to test the signature
		/// algorithm with wellknown inputs.
		///
		/// In normal operation, the nonce is a random value
		/// computed by createNonce() and the timestamp is taken
		/// from the system time.

	static const std::string SCHEME;

protected:
	void signPlaintext(Poco::Net::HTTPRequest& request) const;
		/// Signs the given HTTP request according to OAuth 1.0A PLAINTEXT signature method.

	void signHMACSHA1(Poco::Net::HTTPRequest& request, const std::string& uri, const Poco::Net::HTMLForm& params) const;
		/// Signs the given HTTP request according to OAuth 1.0A HMAC-SHA1 signature method.

	std::string createNonce() const;
		/// Creates a nonce, which is basically a Base64-encoded 32 character random
		/// string, with non-alphanumeric characters removed.

	std::string createSignature(const Poco::Net::HTTPRequest& request, const std::string& uri, const Poco::Net::HTMLForm& params, const std::string& nonce, const std::string& timestamp) const;
		/// Creates a OAuth signature for the given request and its parameters, according
		/// to <https://dev.twitter.com/docs/auth/creating-signature>.
		
	static std::string percentEncode(const std::string& str);
		/// Percent-encodes the given string according to Twitter API's rules,
		/// given in <https://dev.twitter.com/docs/auth/percent-encoding-parameters>.

private:
	OAuth10Credentials(const OAuth10Credentials&);
	OAuth10Credentials& operator = (const OAuth10Credentials&);
	
	std::string _consumerKey;
	std::string _consumerSecret;
	std::string _token;
	std::string _tokenSecret;
	std::string _callback;
	std::string _realm;
	std::string _nonce;
	std::string _timestamp;
};


//
// inlines
//
inline const std::string& OAuth10Credentials::getConsumerKey() const
{
	return _consumerKey;
}


inline const std::string& OAuth10Credentials::getConsumerSecret() const
{
	return _consumerSecret;
}


inline const std::string& OAuth10Credentials::getToken() const
{
	return _token;
}


inline const std::string& OAuth10Credentials::getTokenSecret() const
{
	return _tokenSecret;
}


inline const std::string& OAuth10Credentials::getRealm() const
{
	return _realm;
}


inline const std::string& OAuth10Credentials::getCallback() const
{
	return _callback;
}


} } // namespace Poco::Net


#endif // Net_OAuth10Credentials_INCLUDED
