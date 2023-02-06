//
// download.cpp
//
// This sample demonstrates the URIStreamOpener class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/URIStreamOpener.h"
#include "Poco/StreamCopier.h"
#include "Poco/Path.h"
#include "Poco/URI.h"
#include "Poco/Exception.h"
#include "Poco/Net/HTTPStreamFactory.h"
#include "Poco/Net/FTPStreamFactory.h"
#include <memory>
#include <iostream>


using Poco::URIStreamOpener;
using Poco::StreamCopier;
using Poco::Path;
using Poco::URI;
using Poco::Exception;
using Poco::Net::HTTPStreamFactory;
using Poco::Net::FTPStreamFactory;


int main(int argc, char** argv)
{
	HTTPStreamFactory::registerFactory();
	FTPStreamFactory::registerFactory();
	
	if (argc != 2)
	{
		Path p(argv[0]);
		std::cerr << "usage: " << p.getBaseName() << " <uri>" << std::endl;
		std::cerr << "       Download <uri> to standard output." << std::endl;
		std::cerr << "       Works with http, ftp and file URIs." << std::endl;
		return 1;
	}

	try
	{
		URI uri(argv[1]);
#ifndef POCO_ENABLE_CPP11
		std::auto_ptr<std::istream> pStr(URIStreamOpener::defaultOpener().open(uri));
#else
		std::unique_ptr<std::istream> pStr(URIStreamOpener::defaultOpener().open(uri));
#endif // POCO_ENABLE_CPP11
		StreamCopier::copyStream(*pStr.get(), std::cout);
	}
	catch (Exception& exc)
	{
		std::cerr << exc.displayText() << std::endl;
		return 1;
	}
		
	return 0;
}
