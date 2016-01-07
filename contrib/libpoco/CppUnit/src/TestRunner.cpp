//
// TestRunner.cpp
//
// $Id: //poco/1.4/CppUnit/src/TestRunner.cpp#1 $
//


#include "CppUnit/TestRunner.h"
#include "CppUnit/Test.h"
#include "CppUnit/TestSuite.h"
#include "CppUnit/TextTestResult.h"
#include <iostream>


namespace CppUnit {


TestRunner::TestRunner():
	_ostr(std::cout)
{
}


TestRunner::TestRunner(std::ostream& ostr):
	_ostr(ostr)
{
}


TestRunner::~TestRunner()
{
	for (Mappings::iterator it = _mappings.begin(); it != _mappings.end(); ++it)
		delete it->second;
}


void TestRunner::printBanner()
{
    _ostr 
		<< "Usage: driver [-all] [-print] [-wait] [name] ..." << std::endl
		<< "       where name is the name of a test case class" << std::endl;
}


bool TestRunner::run(const std::vector<std::string>& args)
{
	std::string testCase;
	int numberOfTests = 0;
	bool success = true;
	bool all     = false;
	bool wait    = false;
	bool printed = false;

	for (int i = 1; i < args.size(); i++) 
	{
		const std::string& arg = args[i];
		if (arg == "-wait") 
		{
			wait = true;
			continue;
		}
		else if (arg == "-all")
		{
			all = true;
			continue;
		}
		else if (arg == "-print")
		{
			for (Mappings::iterator it = _mappings.begin(); it != _mappings.end(); ++it) 
			{
				print(it->first, it->second, 0);
			}
			printed = true;
			continue;
		}

		if (!all)
		{
			testCase = arg;

			if (testCase == "")
			{
				printBanner();
				return false;
			}

			Test* testToRun = 0;
			for (Mappings::iterator it = _mappings.begin(); !testToRun && it != _mappings.end(); ++it) 
			{
				testToRun = find(testCase, it->second, it->first);
			}
			if (testToRun)
			{
				if (!run(testToRun)) success = false;
			}
			numberOfTests++;

			if (!testToRun) 
			{
				_ostr << "Test " << testCase << " not found." << std::endl;
				return false;
			}
		}
	}

	if (all)
	{
		for (Mappings::iterator it = _mappings.begin(); it != _mappings.end(); ++it) 
		{
			if (!run(it->second)) success = false;
			numberOfTests++;
		}
	}
	
	if (numberOfTests == 0 && !printed) 
	{
		printBanner();
		return false;
	}

	if (wait) 
	{
		_ostr << "<RETURN> to continue" << std::endl;
		std::cin.get();
	}

	return success;
}


bool TestRunner::run(Test* test)
{
	TextTestResult result(_ostr);

	test->run(&result);
	_ostr << result << std::endl;

	return result.wasSuccessful();
}


void TestRunner::addTest(const std::string& name, Test* test)
{
	_mappings.push_back(Mapping(name, test));
}


void TestRunner::print(const std::string& name, Test* pTest, int indent)
{
	for (int i = 0; i < indent; ++i)
		_ostr << "    ";
	_ostr << name << std::endl;
	TestSuite* pSuite = dynamic_cast<TestSuite*>(pTest);
	if (pSuite)
	{
		const std::vector<Test*>& tests = pSuite->tests();
		for (std::vector<Test*>::const_iterator it = tests.begin(); it != tests.end(); ++it)
		{
			print((*it)->toString(), *it, indent + 1);
		}
	}
}


Test* TestRunner::find(const std::string& name, Test* pTest, const std::string& testName)
{
	if (testName.find(name) != std::string::npos)
	{
		return pTest;
	}
	else
	{
		TestSuite* pSuite = dynamic_cast<TestSuite*>(pTest);
		if (pSuite)
		{
			const std::vector<Test*>& tests = pSuite->tests();
			for (std::vector<Test*>::const_iterator it = tests.begin(); it != tests.end(); ++it)
			{
				Test* result = find(name, *it, (*it)->toString());
				if (result) return result;
			}
		}
		return 0;
	}
}


} // namespace CppUnit
