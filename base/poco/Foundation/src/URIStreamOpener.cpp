//
// URIStreamOpener.cpp
//
// Library: Foundation
// Package: URI
// Module:  URIStreamOpener
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/URIStreamOpener.h"
#include "Poco/URIStreamFactory.h"
#include "Poco/FileStreamFactory.h"
#include "Poco/URI.h"
#include "Poco/Path.h"
#include "Poco/SingletonHolder.h"
#include "Poco/Exception.h"


namespace Poco {


URIStreamOpener::URIStreamOpener()
{
	registerStreamFactory("file", new FileStreamFactory);
}


URIStreamOpener::~URIStreamOpener()
{
	for (FactoryMap::iterator it = _map.begin(); it != _map.end(); ++it)
		delete it->second;
}


std::istream* URIStreamOpener::open(const URI& uri) const
{
	FastMutex::ScopedLock lock(_mutex);

	std::string scheme;
	if (uri.isRelative())
		scheme = "file";
	else
		scheme = uri.getScheme();
	return openURI(scheme, uri);
}


std::istream* URIStreamOpener::open(const std::string& pathOrURI) const
{
	FastMutex::ScopedLock lock(_mutex);

	try
	{
		URI uri(pathOrURI);
		std::string scheme(uri.getScheme());
		FactoryMap::const_iterator it = _map.find(scheme);
		if (it != _map.end())
		{
			return openURI(scheme, uri);
		}
		else if (scheme.length() <= 1) // could be Windows path
		{
			Path path;
			if (path.tryParse(pathOrURI, Path::PATH_GUESS))
			{
				return openFile(path);
			}
		}
		throw UnknownURISchemeException(pathOrURI);
	}
	catch (URISyntaxException&)
	{
		Path path;
		if (path.tryParse(pathOrURI, Path::PATH_GUESS))
			return openFile(path);
		else 
			throw;
	}
}


std::istream* URIStreamOpener::open(const std::string& basePathOrURI, const std::string& pathOrURI) const
{
	FastMutex::ScopedLock lock(_mutex);

	try
	{
		URI uri(basePathOrURI);
		std::string scheme(uri.getScheme());
		FactoryMap::const_iterator it = _map.find(scheme);
		if (it != _map.end())
		{
			uri.resolve(pathOrURI);
			scheme = uri.getScheme();
			return openURI(scheme, uri);
		}
		else if (scheme.length() <= 1) // could be Windows path
		{
			Path base;
			Path path;
			if (base.tryParse(basePathOrURI, Path::PATH_GUESS) && path.tryParse(pathOrURI, Path::PATH_GUESS))
			{
				base.resolve(path);
				return openFile(base);
			}
		}
		throw UnknownURISchemeException(basePathOrURI);
	}
	catch (URISyntaxException&)
	{
		Path base;
		Path path;
		if (base.tryParse(basePathOrURI, Path::PATH_GUESS) && path.tryParse(pathOrURI, Path::PATH_GUESS))
		{
			base.resolve(path);
			return openFile(base);
		}
		else throw;
	}
}

	
void URIStreamOpener::registerStreamFactory(const std::string& scheme, URIStreamFactory* pFactory)
{
	poco_check_ptr (pFactory);

	FastMutex::ScopedLock lock(_mutex);
	if (_map.find(scheme) == _map.end())
	{
		_map[scheme] = pFactory;
	}
	else throw ExistsException("An URIStreamFactory for the given scheme has already been registered", scheme);
}


void URIStreamOpener::unregisterStreamFactory(const std::string& scheme)
{
	FastMutex::ScopedLock lock(_mutex);
	
	FactoryMap::iterator it = _map.find(scheme);
	if (it != _map.end())
	{
		URIStreamFactory* pFactory = it->second;
		_map.erase(it);
		delete pFactory;
	}
	else throw NotFoundException("No URIStreamFactory has been registered for the given scheme", scheme);
}


bool URIStreamOpener::supportsScheme(const std::string& scheme)
{
	FastMutex::ScopedLock lock(_mutex);
	return _map.find(scheme) != _map.end();
}


namespace
{
	static SingletonHolder<URIStreamOpener> sh;
}


URIStreamOpener& URIStreamOpener::defaultOpener()
{
	return *sh.get();
}


std::istream* URIStreamOpener::openFile(const Path& path) const
{
	FileStreamFactory factory;
	return factory.open(path);
}


std::istream* URIStreamOpener::openURI(const std::string& scheme, const URI& uri) const
{
	std::string actualScheme(scheme);
	URI actualURI(uri);
	int redirects = 0;
	
	while (redirects < MAX_REDIRECTS)
	{
		try
		{
			FactoryMap::const_iterator it = _map.find(actualScheme);
			if (it != _map.end())
				return it->second->open(actualURI);
			else if (redirects > 0)
				throw UnknownURISchemeException(actualURI.toString() + std::string("; redirected from ") + uri.toString());
			else
				throw UnknownURISchemeException(actualURI.toString());
		}
		catch (URIRedirection& redir)
		{
			actualURI = redir.uri();
			actualScheme = actualURI.getScheme();
			++redirects;
		}
	}
	throw TooManyURIRedirectsException(uri.toString());
}


} // namespace Poco
