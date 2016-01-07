//
// WinDriver.cpp
//
// $Id: //poco/1.4/NetSSL_OpenSSL/testsuite/src/WinDriver.cpp#1 $
//
// Windows test driver for Poco OpenSSL.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "WinTestRunner/WinTestRunner.h"
#include "NetSSLTestSuite.h"
#include "Poco/Util/Application.h"
#include "Poco/Net/HTTPStreamFactory.h"
#include "Poco/Net/HTTPSStreamFactory.h"


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
		CppUnit::WinTestRunner runner;
		runner.addTest(NetSSLTestSuite::suite());
		runner.run();
		return 0;
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


class TestDriver: public CppUnit::WinTestRunnerApp
{
	void TestMain()
	{
		NetSSLApp app;
		std::string argv("TestSuite");
		const char* pArgv = argv.c_str();
		try
		{
			app.init(1, (char**)&pArgv);
			app.run();
		}
		catch (Poco::Exception& exc)
		{
			app.logger().log(exc);
		}
	}
};


static TestDriver theDriver;
