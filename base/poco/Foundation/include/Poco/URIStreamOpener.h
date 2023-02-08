//
// URIStreamOpener.h
//
// Library: Foundation
// Package: URI
// Module:  URIStreamOpener
//
// Definition of the URIStreamOpener class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_URIStreamOpener_INCLUDED
#define Foundation_URIStreamOpener_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Mutex.h"
#include <istream>
#include <map>


namespace Poco {


class URI;
class URIStreamFactory;
class Path;


class Foundation_API URIStreamOpener
	/// The URIStreamOpener class is used to create and open input streams
	/// for resourced identified by Uniform Resource Identifiers.
	///
	/// For every URI scheme used, a URIStreamFactory must be registered.
	/// A FileStreamFactory is automatically registered for file URIs.
{
public:
	enum
	{
		MAX_REDIRECTS = 10
	};
	
	URIStreamOpener();
		/// Creates the URIStreamOpener and registers a FileStreamFactory
		/// for file URIs.

	~URIStreamOpener();
		/// Destroys the URIStreamOpener and deletes all registered
		/// URI stream factories.

	std::istream* open(const URI& uri) const;
		/// Tries to create and open an input stream for the resource specified
		/// by the given uniform resource identifier.
		///
		/// If no URIStreamFactory has been registered for the URI's
		/// scheme, a UnknownURIScheme exception is thrown.
		/// If the stream cannot be opened for any reason, an
		/// IOException is thrown.
		///
		/// The given URI must be a valid one. This excludes file system paths.
		///
		/// Whoever calls the method is responsible for deleting
		/// the returned stream.

	std::istream* open(const std::string& pathOrURI) const;
		/// Tries to create and open an input stream for the resource specified
		/// by the given path or uniform resource identifier.
		///
		/// If the stream cannot be opened for any reason, an
		/// Exception is thrown.
		///
		/// The method first tries to interpret the given pathOrURI as an URI.
		/// If this fails, the pathOrURI is treated as local filesystem path.
		/// If this also fails, an exception is thrown.
		///
		/// Whoever calls the method is responsible for deleting
		/// the returned stream.

	std::istream* open(const std::string& basePathOrURI, const std::string& pathOrURI) const;
		/// Tries to create and open an input stream for the resource specified
		/// by the given path or uniform resource identifier.
		///
		/// pathOrURI is resolved against basePathOrURI (see URI::resolve() and
		/// Path::resolve() for more information).
		///
		/// If the stream cannot be opened for any reason, an
		/// Exception is thrown.
		///
		/// Whoever calls the method is responsible for deleting
		/// the returned stream.
		
	void registerStreamFactory(const std::string& scheme, URIStreamFactory* pFactory);
		/// Registers a URIStreamFactory for the given scheme. If another factory
		/// has already been registered for the scheme, an ExistsException is thrown.
		///
		/// The URIStreamOpener takes ownership of the factory and deletes it when it is
		/// no longer needed (in other words, when the URIStreamOpener is deleted).

	void unregisterStreamFactory(const std::string& scheme);
		/// Unregisters and deletes the URIStreamFactory for the given scheme.
		///
		/// Throws a NotFoundException if no URIStreamFactory has been registered
		/// for the given scheme.
		
	bool supportsScheme(const std::string& scheme);
		/// Returns true iff a URIStreamFactory for the given scheme
		/// has been registered.

	static URIStreamOpener& defaultOpener();
		/// Returns a reference to the default URIStreamOpener.

protected:
	std::istream* openFile(const Path& path) const;
	std::istream* openURI(const std::string& scheme, const URI& uri) const;

private:
	URIStreamOpener(const URIStreamOpener&);
	URIStreamOpener& operator = (const URIStreamOpener&);

	typedef std::map<std::string, URIStreamFactory*> FactoryMap;
	
	FactoryMap        _map;
	mutable FastMutex _mutex;
};


} // namespace Poco


#endif // Foundation_URIStreamOpener_INCLUDED
