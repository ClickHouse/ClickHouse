//
// ApacheRequestHandlerFactory.cpp
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ApacheRequestHandlerFactory.h"
#include "ApacheConnector.h"
#include "Poco/Net/HTTPRequestHandler.h"
#include "Poco/StringTokenizer.h"
#include "Poco/Manifest.h"
#include "Poco/File.h"
#include <vector>


using Poco::StringTokenizer;
using Poco::FastMutex;


ApacheRequestHandlerFactory::ApacheRequestHandlerFactory()
{
}


ApacheRequestHandlerFactory::~ApacheRequestHandlerFactory()
{
}


Poco::Net::HTTPRequestHandler* ApacheRequestHandlerFactory::createRequestHandler(const Poco::Net::HTTPServerRequest& request)
{
	FastMutex::ScopedLock lock(_mutex);
	
	// only if the given uri is found in _uris we are
	// handling this request.
	RequestHandlerFactories::iterator it = _requestHandlers.begin();
	RequestHandlerFactories::iterator itEnd = _requestHandlers.end();
	std::string uri = request.getURI();

	// if any uri in our map is found at the beginning of the given
	// uri -> then we handle it!!
	for (; it != itEnd; it++)
	{
		if (uri.find(it->first) == 0 || it->first.find(uri) == 0)
		{
			return it->second->createRequestHandler(request);
		}
	}

	return 0;
}


void ApacheRequestHandlerFactory::handleURIs(const std::string& uris)
{
	FastMutex::ScopedLock lock(_mutex);

	StringTokenizer st(uris, " ", StringTokenizer::TOK_TRIM);
	StringTokenizer::Iterator it = st.begin();
	StringTokenizer::Iterator itEnd = st.end();
	std::string factoryName = (*it);
	it++;
	std::string dllName = (*it);
	it++;

	for (; it != itEnd; it++)
	{
		addRequestHandlerFactory(dllName, factoryName, *it);
	}
}


void ApacheRequestHandlerFactory::addRequestHandlerFactory(const std::string& dllPath, const std::string& factoryName, const std::string& uri)
{	
	try
	{
		_loader.loadLibrary(dllPath);
		Poco::Net::HTTPRequestHandlerFactory* pFactory = _loader.classFor(factoryName).create();
		_requestHandlers.insert(std::make_pair(uri, pFactory));
	}
	catch (Poco::Exception& exc)
	{
		ApacheConnector::log(__FILE__, __LINE__, ApacheConnector::PRIO_ERROR, 0, exc.displayText().c_str());
	}
}


bool ApacheRequestHandlerFactory::mustHandle(const std::string& uri)
{
	FastMutex::ScopedLock lock(_mutex);

	// only if the given uri is found in _uris we are
	// handling this request.
	RequestHandlerFactories::iterator it = _requestHandlers.begin();
	RequestHandlerFactories::iterator itEnd = _requestHandlers.end();

	// if any uri in our map is found at the beginning of the given
	// uri -> then we handle it!!
	for (; it != itEnd; it++)
	{
		// dealing with both cases:
		// handler is registered with: /download
		// uri: /download/xyz
		// uri: /download
		if (uri.find(it->first) == 0 || it->first.find(uri) == 0)
			return true;
	}

	return false;
}
