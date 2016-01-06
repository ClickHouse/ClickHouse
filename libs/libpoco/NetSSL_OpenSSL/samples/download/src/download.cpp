//
// download.cpp
//
// $Id: //poco/1.4/NetSSL_OpenSSL/samples/download/src/download.cpp#1 $
//
// This sample demonstrates the URIStreamOpener class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/URIStreamOpener.h"
#include "Poco/StreamCopier.h"
#include "Poco/Path.h"
#include "Poco/URI.h"
#include "Poco/SharedPtr.h"
#include "Poco/Exception.h"
#include "Poco/Net/HTTPStreamFactory.h"
#include "Poco/Net/HTTPSStreamFactory.h"
#include "Poco/Net/FTPStreamFactory.h"
#include "Poco/Net/SSLManager.h"
#include "Poco/Net/KeyConsoleHandler.h"
#include "Poco/Net/ConsoleCertificateHandler.h"
#include <memory>
#include <iostream>


using Poco::URIStreamOpener;
using Poco::StreamCopier;
using Poco::Path;
using Poco::URI;
using Poco::SharedPtr;
using Poco::Exception;
using Poco::Net::HTTPStreamFactory;
using Poco::Net::HTTPSStreamFactory;
using Poco::Net::FTPStreamFactory;
using Poco::Net::SSLManager;
using Poco::Net::Context;
using Poco::Net::KeyConsoleHandler;
using Poco::Net::PrivateKeyPassphraseHandler;
using Poco::Net::InvalidCertificateHandler;
using Poco::Net::ConsoleCertificateHandler;


class SSLInitializer
{
public:
	SSLInitializer()
	{
		Poco::Net::initializeSSL();
	}
	
	~SSLInitializer()
	{
		Poco::Net::uninitializeSSL();
	}
};


int main(int argc, char** argv)
{
	SSLInitializer sslInitializer;
	HTTPStreamFactory::registerFactory();
	HTTPSStreamFactory::registerFactory();
	FTPStreamFactory::registerFactory();
	
	if (argc != 2)
	{
		Path p(argv[0]);
		std::cerr << "usage: " << p.getBaseName() << " <uri>" << std::endl;
		std::cerr << "       Download <uri> to standard output." << std::endl;
		std::cerr << "       Works with http, https, ftp and file URIs." << std::endl;
		return 1;
	}

	// Note: we must create the passphrase handler prior Context 
	SharedPtr<InvalidCertificateHandler> ptrCert = new ConsoleCertificateHandler(false); // ask the user via console
	Context::Ptr ptrContext = new Context(Context::CLIENT_USE, "", "", "rootcert.pem", Context::VERIFY_RELAXED, 9, false, "ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH");
	SSLManager::instance().initializeClient(0, ptrCert, ptrContext);

	try
	{
		URI uri(argv[1]);
		std::auto_ptr<std::istream> pStr(URIStreamOpener::defaultOpener().open(uri));
		StreamCopier::copyStream(*pStr.get(), std::cout);
	}
	catch (Exception& exc)
	{
		std::cerr << exc.displayText() << std::endl;
		return 1;
	}
		
	return 0;
}
