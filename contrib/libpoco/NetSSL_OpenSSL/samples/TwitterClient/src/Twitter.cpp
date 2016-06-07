//
// Twitter.cpp
//
// $Id: //poco/1.4/Net/samples/TwitterClient/src/Twitter.cpp#2 $
//
// A C++ implementation of a Twitter client based on the POCO Net library.
//
// Copyright (c) 2009-2013, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Twitter.h"
#include "Poco/Net/HTTPSClientSession.h"
#include "Poco/Net/HTTPRequest.h"
#include "Poco/Net/HTTPResponse.h"
#include "Poco/Net/OAuth10Credentials.h"
#include "Poco/Util/JSONConfiguration.h"
#include "Poco/URI.h"
#include "Poco/Format.h"
#include "Poco/StreamCopier.h"


const std::string Twitter::TWITTER_URI("https://api.twitter.com/1.1/statuses/");


Twitter::Twitter():
	_uri(TWITTER_URI)
{
}

	
Twitter::Twitter(const std::string& twitterURI):
	_uri(twitterURI)
{
}

	
Twitter::~Twitter()
{
}


void Twitter::login(const std::string& consumerKey, const std::string& consumerSecret, const std::string& token, const std::string& tokenSecret)
{
	_consumerKey    = consumerKey;
	_consumerSecret = consumerSecret;
	_token          = token;
	_tokenSecret    = tokenSecret;
}

	
Poco::Int64 Twitter::update(const std::string& status)
{
	Poco::Net::HTMLForm form;
	form.set("status", status);
	Poco::AutoPtr<Poco::Util::AbstractConfiguration> pResult = invoke(Poco::Net::HTTPRequest::HTTP_POST, "update", form);
	return pResult->getInt64("id", 0);
}


Poco::AutoPtr<Poco::Util::AbstractConfiguration> Twitter::invoke(const std::string& httpMethod, const std::string& twitterMethod, Poco::Net::HTMLForm& form)
{
	// Create the request URI.
	// We use the JSON version of the Twitter API.
	Poco::URI uri(_uri + twitterMethod + ".json");
	
	Poco::Net::HTTPSClientSession session(uri.getHost(), uri.getPort());
	Poco::Net::HTTPRequest req(httpMethod, uri.getPath(), Poco::Net::HTTPMessage::HTTP_1_1);
	
	// Sign request
	Poco::Net::OAuth10Credentials creds(_consumerKey, _consumerSecret, _token, _tokenSecret);
	creds.authenticate(req, uri, form);
	
	// Send the request.
	form.prepareSubmit(req);
	std::ostream& ostr = session.sendRequest(req);
	form.write(ostr);
	
	// Receive the response.
	Poco::Net::HTTPResponse res;
	std::istream& rs = session.receiveResponse(res);
	
	Poco::AutoPtr<Poco::Util::JSONConfiguration> pResult = new Poco::Util::JSONConfiguration(rs);

	// If everything went fine, return the JSON document.
	// Otherwise throw an exception.
	if (res.getStatus() == Poco::Net::HTTPResponse::HTTP_OK)
	{
		return pResult;
	}
	else
	{
		throw Poco::ApplicationException("Twitter Error", pResult->getString("errors[0].message", ""));
	}
}
