//
// FileStreamFactory.cpp
//
// $Id: //poco/1.4/Foundation/src/FileStreamFactory.cpp#1 $
//
// Library: Foundation
// Package: URI
// Module:  FileStreamFactory
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/FileStreamFactory.h"
#include "Poco/URI.h"
#include "Poco/Path.h"
#include "Poco/File.h"
#include "Poco/Exception.h"
#include "Poco/FileStream.h"


namespace Poco {


FileStreamFactory::FileStreamFactory()
{
}


FileStreamFactory::~FileStreamFactory()
{
}


std::istream* FileStreamFactory::open(const URI& uri)
{
	poco_assert (uri.isRelative() || uri.getScheme() == "file");

	std::string uriPath = uri.getPath();
	if (uriPath.substr(0, 2) == "./")
		uriPath.erase(0, 2);
	Path p(uriPath, Path::PATH_UNIX);
	p.setNode(uri.getHost());
	return open(p);
}


std::istream* FileStreamFactory::open(const Path& path)
{
	File file(path);
	if (!file.exists()) throw FileNotFoundException(path.toString());
	
	FileInputStream* istr = new FileInputStream(path.toString(), std::ios::binary);
	if (!istr->good())
	{
		delete istr;
		throw OpenFileException(path.toString());
	}	
	return istr;
}


} // namespace Poco
