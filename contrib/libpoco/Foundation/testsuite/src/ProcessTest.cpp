//
// ProcessTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/ProcessTest.cpp#2 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ProcessTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Process.h"
#include "Poco/Pipe.h"
#include "Poco/PipeStream.h"


using Poco::Process;
using Poco::ProcessHandle;
using Poco::Pipe;
using Poco::PipeInputStream;
using Poco::PipeOutputStream;


ProcessTest::ProcessTest(const std::string& name): CppUnit::TestCase(name)
{
}


ProcessTest::~ProcessTest()
{
}


void ProcessTest::testLaunch()
{
	std::string name("TestApp");
	std::string cmd;
#if defined(_DEBUG)
	name += "d";
#endif

#if defined(POCO_OS_FAMILY_UNIX)
	cmd = "./";
	cmd += name;
#elif defined(_WIN32_WCE)
	cmd = "\\";
	cmd += name;
	cmd += ".EXE";
#else
	cmd = name;
#endif

	std::vector<std::string> args;
	args.push_back("arg1");
	args.push_back("arg2");
	args.push_back("arg3");
	ProcessHandle ph = Process::launch(cmd, args);
	int rc = ph.wait();
	assert (rc == 3);
}


void ProcessTest::testLaunchRedirectIn()
{
#if !defined(_WIN32_WCE)
	std::string name("TestApp");
	std::string cmd;
#if defined(_DEBUG)
	name += "d";
#endif

#if defined(POCO_OS_FAMILY_UNIX)
	cmd = "./";
	cmd += name;
#else
	cmd = name;
#endif

	std::vector<std::string> args;
	args.push_back("-count");
	Pipe inPipe;
	ProcessHandle ph = Process::launch(cmd, args, &inPipe, 0, 0);
	PipeOutputStream ostr(inPipe);
	ostr << std::string(100, 'x');
	ostr.close();
	int rc = ph.wait();
	assert (rc == 100);
#endif // !defined(_WIN32_WCE)
}


void ProcessTest::testLaunchRedirectOut()
{
#if !defined(_WIN32_WCE)
	std::string name("TestApp");
	std::string cmd;
#if defined(_DEBUG)
	name += "d";
#endif

#if defined(POCO_OS_FAMILY_UNIX)
	cmd = "./";
	cmd += name;
#else
	cmd = name;
#endif

	std::vector<std::string> args;
	args.push_back("-hello");
	Pipe outPipe;
	ProcessHandle ph = Process::launch(cmd, args, 0, &outPipe, 0);
	PipeInputStream istr(outPipe);
	std::string s;
	int c = istr.get();
	while (c != -1) { s += (char) c; c = istr.get(); }
	assert (s == "Hello, world!");
	int rc = ph.wait();
	assert (rc == 1);
#endif // !defined(_WIN32_WCE)
}


void ProcessTest::testLaunchEnv()
{
#if !defined(_WIN32_WCE)
	std::string name("TestApp");
	std::string cmd;
#if defined(_DEBUG)
	name += "d";
#endif

#if defined(POCO_OS_FAMILY_UNIX)
	cmd = "./";
	cmd += name;
#else
	cmd = name;
#endif

	std::vector<std::string> args;
	args.push_back("-env");
	Pipe outPipe;
	Process::Env env;
	env["TESTENV"] = "test";
	ProcessHandle ph = Process::launch(cmd, args, 0, &outPipe, 0, env);
	PipeInputStream istr(outPipe);
	std::string s;
	int c = istr.get();
	while (c != -1) { s += (char) c; c = istr.get(); }
	assert (s == "test");
	int rc = ph.wait();
	assert (rc == 0);
#endif // !defined(_WIN32_WCE)
}


void ProcessTest::testIsRunning()
{
#if !defined(_WIN32_WCE)
	std::string name("TestApp");
	std::string cmd;
#if defined(_DEBUG)
	name += "d";
#endif

#if defined(POCO_OS_FAMILY_UNIX)
	cmd = "./";
	cmd += name;
#else
	cmd = name;
#endif

	std::vector<std::string> args;
	args.push_back("-count");
	Pipe inPipe;
	ProcessHandle ph = Process::launch(cmd, args, &inPipe, 0, 0);
	Process::PID id = ph.id();
	assert (Process::isRunning(ph));
	assert (Process::isRunning(id));
	PipeOutputStream ostr(inPipe);
	ostr << std::string(100, 'x');
	ostr.close();
	int rc = ph.wait();
	assert (!Process::isRunning(ph));
	assert (!Process::isRunning(id));
#endif // !defined(_WIN32_WCE)
}


void ProcessTest::setUp()
{
}


void ProcessTest::tearDown()
{
}


CppUnit::Test* ProcessTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ProcessTest");

	CppUnit_addTest(pSuite, ProcessTest, testLaunch);
	CppUnit_addTest(pSuite, ProcessTest, testLaunchRedirectIn);
	CppUnit_addTest(pSuite, ProcessTest, testLaunchRedirectOut);
	CppUnit_addTest(pSuite, ProcessTest, testLaunchEnv);
	CppUnit_addTest(pSuite, ProcessTest, testIsRunning);

	return pSuite;
}
