//
// URIStreamFactory.h
//
// $Id: //poco/1.4/Foundation/include/Poco/URIStreamFactory.h#1 $
//
// Library: Foundation
// Package: URI
// Module:  URIStreamFactory
//
// Definition of the URIStreamFactory class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_URIStreamFactory_INCLUDED
#define Foundation_URIStreamFactory_INCLUDED


#include "Poco/Foundation.h"
#include <istream>


namespace Poco {


class URI;


class Foundation_API URIStreamFactory
	/// This class defines the interface that all
	/// URI stream factories must implement.
	///
	/// Subclasses must implement the open() method.
{
public:
	URIStreamFactory();
		/// Creates the URIStreamFactory.

	virtual std::istream* open(const URI& uri) = 0;
		/// Tries to create and open an input stream for the
		/// resource specified by the given URI.
		///
		/// If the stream cannot be opened for whatever reason,
		/// an appropriate IOException must be thrown.
		///
		/// If opening the stream results in a redirect, a
		/// URIRedirection exception should be thrown.

protected:
	virtual ~URIStreamFactory();
		/// Destroys the URIStreamFactory.

private:
	URIStreamFactory(const URIStreamFactory&);
	URIStreamFactory& operator = (const URIStreamFactory&);
	
	friend class URIStreamOpener;
};


class Foundation_API URIRedirection
	/// An instance of URIRedirection is thrown by a URIStreamFactory::open()
	/// if opening the original URI resulted in a redirection response
	/// (such as a MOVED PERMANENTLY in HTTP).
{
public:
	URIRedirection(const std::string& uri);
	URIRedirection(const URIRedirection& redir);
	
	URIRedirection& operator = (const URIRedirection& redir);
	void swap(URIRedirection& redir);
	
	const std::string& uri() const;
		/// Returns the new URI.
	
private:
	URIRedirection();
	
	std::string _uri;
};


//
// inlines
//
inline const std::string& URIRedirection::uri() const
{
	return _uri;
}


} // namespace Poco


#endif // Foundation_URIStreamFactory_INCLUDED
