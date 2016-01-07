//
// OAuth10Credentials.cpp
//
// $Id$
//
// Library: Net
// Package: OAuth
// Module:	OAuth10Credentials
//
// Copyright (c) 2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/OAuth10Credentials.h"
#include "Poco/Net/HTTPRequest.h"
#include "Poco/Net/HTMLForm.h"
#include "Poco/Net/NetException.h"
#include "Poco/Net/HTTPAuthenticationParams.h"
#include "Poco/SHA1Engine.h"
#include "Poco/HMACEngine.h"
#include "Poco/Base64Encoder.h"
#include "Poco/RandomStream.h"
#include "Poco/Timestamp.h"
#include "Poco/NumberParser.h"
#include "Poco/NumberFormatter.h"
#include "Poco/Format.h"
#include "Poco/String.h"
#include <map>
#include <sstream>


namespace Poco {
namespace Net {


const std::string OAuth10Credentials::SCHEME = "OAuth";


OAuth10Credentials::OAuth10Credentials()
{
}


OAuth10Credentials::OAuth10Credentials(const std::string& consumerKey, const std::string& consumerSecret):
	_consumerKey(consumerKey),
	_consumerSecret(consumerSecret)
{
}


OAuth10Credentials::OAuth10Credentials(const std::string& consumerKey, const std::string& consumerSecret, const std::string& token, const std::string& tokenSecret):
	_consumerKey(consumerKey),
	_consumerSecret(consumerSecret),
	_token(token),
	_tokenSecret(tokenSecret)
{
}


OAuth10Credentials::OAuth10Credentials(const Poco::Net::HTTPRequest& request)
{
	if (request.hasCredentials())
	{
		std::string authScheme;
		std::string authParams;
		request.getCredentials(authScheme, authParams);
		if (icompare(authScheme, SCHEME) == 0)
		{
			HTTPAuthenticationParams params(authParams);
			std::string consumerKey = params.get("oauth_consumer_key", "");
			URI::decode(consumerKey, _consumerKey);
			std::string token = params.get("oauth_token", "");
			URI::decode(token, _token);
			std::string callback = params.get("oauth_callback", "");
			URI::decode(callback, _callback);
		}
		else throw NotAuthenticatedException("No OAuth credentials in Authorization header", authScheme);
	}
	else throw NotAuthenticatedException("No Authorization header found");
}


OAuth10Credentials::~OAuth10Credentials()
{
}


void OAuth10Credentials::setConsumerKey(const std::string& consumerKey)
{
	_consumerKey = consumerKey;
}


void OAuth10Credentials::setConsumerSecret(const std::string& consumerSecret)
{
	_consumerSecret = consumerSecret;
}


void OAuth10Credentials::setToken(const std::string& token)
{
	_token = token;
}


void OAuth10Credentials::setTokenSecret(const std::string& tokenSecret)
{
	_tokenSecret = tokenSecret;
}


void OAuth10Credentials::setRealm(const std::string& realm)
{
	_realm = realm;
}


void OAuth10Credentials::setCallback(const std::string& callback)
{
	_callback = callback;
}


void OAuth10Credentials::authenticate(HTTPRequest& request, const Poco::URI& uri, SignatureMethod method)
{
	HTMLForm emptyParams;
	authenticate(request, uri, emptyParams, method);
}

	
void OAuth10Credentials::authenticate(HTTPRequest& request, const Poco::URI& uri, const Poco::Net::HTMLForm& params, SignatureMethod method)
{
	if (method == SIGN_PLAINTEXT)
	{
		signPlaintext(request);
	}
	else
	{
		URI uriWithoutQuery(uri);
		uriWithoutQuery.setQuery("");
		uriWithoutQuery.setFragment("");
		signHMACSHA1(request, uriWithoutQuery.toString(), params);
	}
}


bool OAuth10Credentials::verify(const HTTPRequest& request, const Poco::URI& uri)
{
	HTMLForm params;
	return verify(request, uri, params);
}


bool OAuth10Credentials::verify(const HTTPRequest& request, const Poco::URI& uri, const Poco::Net::HTMLForm& params)
{
	if (request.hasCredentials())
	{
		std::string authScheme;
		std::string authParams;
		request.getCredentials(authScheme, authParams);
		if (icompare(authScheme, SCHEME) == 0)
		{
			HTTPAuthenticationParams oauthParams(authParams);

			std::string version = oauthParams.get("oauth_version", "1.0");
			if (version != "1.0") throw NotAuthenticatedException("Unsupported OAuth version", version);
			
			_consumerKey.clear();
			std::string consumerKey = oauthParams.get("oauth_consumer_key", "");
			URI::decode(consumerKey, _consumerKey);
			
			_token.clear();
			std::string token = oauthParams.get("oauth_token", "");
			URI::decode(token, _token);
			
			_callback.clear();
			std::string callback = oauthParams.get("oauth_callback", "");
			URI::decode(callback, _callback);
			
			std::string nonceEnc = oauthParams.get("oauth_nonce", "");
			std::string nonce;
			URI::decode(nonceEnc, nonce);
			
			std::string timestamp = oauthParams.get("oauth_timestamp", "");
			
			std::string method = oauthParams.get("oauth_signature_method", "");
			
			std::string signatureEnc = oauthParams.get("oauth_signature", "");
			std::string signature;
			URI::decode(signatureEnc, signature);

			std::string refSignature;
			if (icompare(method, "PLAINTEXT") == 0)
			{
				refSignature = percentEncode(_consumerSecret);
				refSignature += '&';
				refSignature += percentEncode(_tokenSecret);
			}
			else if (icompare(method, "HMAC-SHA1") == 0)
			{
				URI uriWithoutQuery(uri);
				uriWithoutQuery.setQuery("");
				uriWithoutQuery.setFragment("");
				refSignature = createSignature(request, uriWithoutQuery.toString(), params, nonce, timestamp);
			}
			else throw NotAuthenticatedException("Unsupported OAuth signature method", method);
			
			return refSignature == signature;			
		}
		else throw NotAuthenticatedException("No OAuth credentials found in Authorization header");
	}
	else throw NotAuthenticatedException("No Authorization header found");
}


void OAuth10Credentials::nonceAndTimestampForTesting(const std::string& nonce, const std::string& timestamp)
{
	_nonce = nonce;
	_timestamp = timestamp;
}


void OAuth10Credentials::signPlaintext(Poco::Net::HTTPRequest& request) const
{
	std::string signature(percentEncode(_consumerSecret));
	signature += '&';
	signature += percentEncode(_tokenSecret);
	
	std::string authorization(SCHEME);
	if (!_realm.empty())
	{
		Poco::format(authorization, " realm=\"%s\",", _realm);
	}
	Poco::format(authorization, " oauth_consumer_key=\"%s\"", percentEncode(_consumerKey));
	Poco::format(authorization, ", oauth_signature=\"%s\"", percentEncode(signature));
	authorization += ", oauth_signature_method=\"PLAINTEXT\"";
	if (!_token.empty())
	{
		Poco::format(authorization, ", oauth_token=\"%s\"", percentEncode(_token));
	}
	if (!_callback.empty())
	{
		Poco::format(authorization, ", oauth_callback=\"%s\"", percentEncode(_callback));
	}
	authorization += ", oauth_version=\"1.0\"";

	request.set(HTTPRequest::AUTHORIZATION, authorization);
}


void OAuth10Credentials::signHMACSHA1(Poco::Net::HTTPRequest& request, const std::string& uri, const Poco::Net::HTMLForm& params) const
{
	std::string nonce(_nonce);
	if (nonce.empty())
	{
		nonce = createNonce();
	}
	std::string timestamp(_timestamp);
	if (timestamp.empty())
	{
		timestamp = Poco::NumberFormatter::format(Poco::Timestamp().epochTime());
	}
	std::string signature(createSignature(request, uri, params, nonce, timestamp));

	std::string authorization(SCHEME);
	if (!_realm.empty())
	{
		Poco::format(authorization, " realm=\"%s\",", _realm);
	}
	Poco::format(authorization, " oauth_consumer_key=\"%s\"", percentEncode(_consumerKey));
	Poco::format(authorization, ", oauth_nonce=\"%s\"", percentEncode(nonce));
	Poco::format(authorization, ", oauth_signature=\"%s\"", percentEncode(signature));
	authorization += ", oauth_signature_method=\"HMAC-SHA1\"";
	Poco::format(authorization, ", oauth_timestamp=\"%s\"", timestamp);
	if (!_token.empty())
	{
		Poco::format(authorization, ", oauth_token=\"%s\"", percentEncode(_token));
	}
	if (!_callback.empty())
	{
		Poco::format(authorization, ", oauth_callback=\"%s\"", percentEncode(_callback));
	}
	authorization += ", oauth_version=\"1.0\"";

	request.set(HTTPRequest::AUTHORIZATION, authorization);
}


std::string OAuth10Credentials::createNonce() const
{
	std::ostringstream base64Nonce;
	Poco::Base64Encoder base64Encoder(base64Nonce);
	Poco::RandomInputStream randomStream;
	for (int i = 0; i < 32; i++)
	{
		base64Encoder.put(randomStream.get());
	}
	base64Encoder.close();
	std::string nonce = base64Nonce.str();
	return Poco::translate(nonce, "+/=", "");
}


std::string OAuth10Credentials::createSignature(const Poco::Net::HTTPRequest& request, const std::string& uri, const Poco::Net::HTMLForm& params, const std::string& nonce, const std::string& timestamp) const
{
	std::map<std::string, std::string> paramsMap;
	paramsMap["oauth_version"]          = "1.0";
	paramsMap["oauth_consumer_key"]     = percentEncode(_consumerKey);
	paramsMap["oauth_nonce"]            = percentEncode(nonce);
	paramsMap["oauth_signature_method"] = "HMAC-SHA1";
	paramsMap["oauth_timestamp"]        = timestamp;
	if (!_token.empty())
	{
		paramsMap["oauth_token"] = percentEncode(_token);
	}
	if (!_callback.empty())
	{
		paramsMap["oauth_callback"] = percentEncode(_callback);
	}
	for (Poco::Net::HTMLForm::ConstIterator it = params.begin(); it != params.end(); ++it)
	{
		paramsMap[percentEncode(it->first)] = percentEncode(it->second);
	}
	
	std::string paramsString;
	for (std::map<std::string, std::string>::const_iterator it = paramsMap.begin(); it != paramsMap.end(); ++it)
	{
		if (it != paramsMap.begin()) paramsString += '&';
		paramsString += it->first;
		paramsString += "=";
		paramsString += it->second;
	}
	
	std::string signatureBase = request.getMethod();
	signatureBase += '&';
	signatureBase += percentEncode(uri);
	signatureBase += '&';
	signatureBase += percentEncode(paramsString);
	
	std::string signingKey;
	signingKey += percentEncode(_consumerSecret);
	signingKey += '&';
	signingKey += percentEncode(_tokenSecret);
	
	Poco::HMACEngine<Poco::SHA1Engine> hmacEngine(signingKey);
	hmacEngine.update(signatureBase);
	Poco::DigestEngine::Digest digest = hmacEngine.digest();
	std::ostringstream digestBase64;
	Poco::Base64Encoder base64Encoder(digestBase64);
	base64Encoder.write(reinterpret_cast<char*>(&digest[0]), digest.size());
	base64Encoder.close();
	return digestBase64.str();
}


std::string OAuth10Credentials::percentEncode(const std::string& str)
{
	std::string encoded;
	Poco::URI::encode(str, "!?#/'\",;:$&()[]*+=@", encoded);
	return encoded;
}


} } // namespace Poco::Net
