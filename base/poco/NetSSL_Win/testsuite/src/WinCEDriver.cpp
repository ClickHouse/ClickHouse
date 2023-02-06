//
// WinCEDriver.cpp
//
// Console-based test driver for Windows CE.
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CppUnit/TestRunner.h"
#include "NetSSLTestSuite.h"
#include "Poco/Util/Application.h"
#include "Poco/Net/HTTPStreamFactory.h"
#include "Poco/Net/HTTPSStreamFactory.h"
#include <cstdlib>
#include <iostream>


class NetSSLApp: public Poco::Util::Application
{
public:
	NetSSLApp()
	{
		Poco::Net::initializeSSL();
		Poco::Net::HTTPStreamFactory::registerFactory();
		Poco::Net::HTTPSStreamFactory::registerFactory();
	}

	~NetSSLApp()
	{
		Poco::Net::uninitializeSSL();
	}

	int main(const std::vector<std::string>& args)
	{
		CppUnit::TestRunner runner;
		runner.addTest("NetSSLTestSuite", NetSSLTestSuite::suite());
		return runner.run(_targs) ? 0 : 1;
	}
	
	void setup(const std::vector<std::string>& args)
	{
		char* argv[] =
		{
			const_cast<char*>(args[0].c_str())
		};
		
		init(1, argv);
		for (std::size_t i = 0; i < args.size(); ++i)
			_targs.push_back(args[i]);
	}

protected:
	void initialize(Poco::Util::Application& self)
	{
		loadConfiguration(); // load default configuration files, if present
		Poco::Util::Application::initialize(self);
	}
	
private:
	std::vector<std::string> _targs;
};


int _tmain(int argc, wchar_t* argv[])
{
	std::vector<std::string> args;
	for (int i = 0; i < argc; ++i)
	{
		char buffer[1024];
		std::wcstombs(buffer, argv[i], sizeof(buffer));
		args.push_back(std::string(buffer));
	}

	NetSSLApp app;
	try
	{
		app.setup(args);
		return app.run();
	}
	catch (Poco::Exception& exc)
	{
		std::cout << exc.displayText() << std::endl;
		return 1;
	}
}
